import requests
from bs4 import BeautifulSoup
import psycopg2

url_users = 'http://forum.lvivport.com/members/'
url_themes = 'http://forum.lvivport.com/threads/'
HEADERS = {
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.122 Safari/537.36 ',
    'accept': '*/*'
}


con = psycopg2.connect(
    database='parser',
    user='postgres',
    password='',
    host='127.0.0.1',
    port='5432'
)
cur = con.cursor()
cur.execute(
    "CREATE TABLE IF NOT EXISTS users (id_user SERIAL PRIMARY KEY,username VARCHAR(255),user_title VARCHAR(255),happy_birth VARCHAR(255),city VARCHAR(255),occupation VARCHAR(255),last_active VARCHAR(255),pozitive_bal VARCHAR(255),neutral_bal VARCHAR(255),negative_bal VARCHAR(255))")
cur.execute(
    "CREATE TABLE IF NOT EXISTS messages (id SERIAL PRIMARY KEY,theme VARCHAR(255),message TEXT NOT NULL,username VARCHAR(255),date VARCHAR(255),user_id INT)")
con.commit()


def get_html(url):
    r = requests.get(url, headers=HEADERS)
    return r


def user():
    for item in range(1, 101):
        url = url_users+str(item)
        user_html = get_html(url)
        if user_html.status_code == 200:
            sql = "SELECT id_user FROM users WHERE id_user=%s"
            val = (str(item),)
            cur.execute(sql, val)
            chek = cur.fetchone()

            if chek == None:
                soup = BeautifulSoup(user_html.text, 'html5lib')
                link_user = soup.find('a', {'class': 'username'}).get('href')
                user_id = link_user.split('.')
                user_id = user_id[1].replace('/', '')

                username = soup.find('a', {'class': 'username'})
                if username:
                    username = soup.find(
                        'a', {'class': 'username'}).get_text(strip=True)
                else:
                    username = 'new_member'

                user_title = soup.find('span', {'class': 'userTitle'})
                if user_title:
                    user_title = soup.find(
                        'span', {'class': 'userTitle'}).get_text(strip=True)
                else:
                    user_title = ' I LOVE LVIV '

                happy_birth = soup.find('span', {'class': 'dob'})
                if happy_birth:
                    happy_birth = soup.find(
                        'span', {'class': 'dob'}).get_text(strip=True)
                else:
                    happy_birth = ' close info '

                city = soup.find('a', {'itemprop': 'address'})
                if city:
                    city = soup.find(
                        'a', {'itemprop': 'address'}).get_text(strip=True)
                else:
                    city = 'Україна'

                occupation = soup.find('dd', {'itemprop': 'role'})
                if occupation:
                    occupation = soup.find(
                        'dd', {'itemprop': 'role'}).get_text(strip=True)
                else:
                    occupation = 'Freedom'

                last_active = soup.find(
                    'div', {'class': 'ProfileNav_activites'})
                if last_active:
                    last_active = soup.find(
                        'div', {'class': 'ProfileNav_activites'}).get_text(strip=True)
                else:
                    last_active = 'last seen recently'
                pozitive_bal = soup.find(
                    'dd', {'class': 'dark_postrating_positive'})
                if pozitive_bal:
                    pozitive_bal = soup.find(
                        'dd', {'class': 'dark_postrating_positive'}).get_text(strip=True)
                else:
                    pozitive_bal = 'member has not pozitive score'
                neutral_bal = soup.find(
                    'dd', {'class': 'dark_postrating_neutral'})
                if neutral_bal:
                    neutral_bal = soup.find(
                        'dd', {'class': 'dark_postrating_neutral'}).get_text(strip=True)
                else:
                    neutral_bal = 'member has not neutral score'
                negative_bal = soup.find(
                    'dd', {'class': 'dark_postrating_negative'})
                if negative_bal:
                    negative_bal = soup.find(
                        'dd', {'class': 'dark_postrating_negative'}).get_text(strip=True)
                else:
                    negative_bal = 'member has not negative score'
                s = "INSERT INTO users (id_user,username,user_title,happy_birth,city,occupation,last_active,pozitive_bal,neutral_bal,negative_bal) VALUES (%s,%s, %s, %s, %s,%s, %s, %s, %s, %s)"
                v = (user_id, username, user_title, happy_birth, city, occupation,
                     last_active, pozitive_bal, neutral_bal, negative_bal)
                cur.execute(s, v)
                con.commit()
                print(chek, "VS", item, '-', "ЗАЛЕТАЕТ В БД")
            else:
                print(chek[0], "VS", item, '-', "УЖЕ В БД")
        else:
            continue


def message():
    for item in range(1, 101):
        url = url_themes+str(item)
        message_html = get_html(url)
        if message_html.status_code == 200:
            a = BeautifulSoup(message_html.text, 'html5lib')
            d = a.find('p', {'class': 'muted'}).get_text(strip=True)
            theme = d.split(',')
            b = a.find_all('div', {'class': 'messageInfo'})
            for c in b:
                message = c.find(
                    'blockquote', class_='messageText').get_text(strip=True)
                username = c.find(
                    'a', {'class': 'username'}).get_text(strip=True)
                date = c.find('a', {'class': 'datePermalink'}
                              ).get_text(strip=True)
                # Парсим ссилку на користувача, удаляэмо все, окрім цифр - це буде його ІД
                link_user = c.find('a', {'class': 'username'}).get('href')
                user_id = link_user.split('.')
                user_id = user_id[1].replace('/', '')

                sql = "INSERT INTO messages (theme,message,username,date,user_id) VALUES (%s,%s, %s, %s, %s)"
                val = (theme[0], message, username, date, user_id)
                cur.execute(sql, val)
                con.commit()
        else:
            continue


user()
message()
