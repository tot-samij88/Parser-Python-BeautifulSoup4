import requests
from bs4 import BeautifulSoup
import psycopg2


URL = 'http://forum.lvivport.com/'
HEADERS = {
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.122 Safari/537.36 ',
    'accept': '*/*'
}  # Для того щоб сайт не думав що це бот

# Підключення до БД
con = psycopg2.connect(
    database='parser',
    user='postgres',
    password='',
    host='127.0.0.1',
    port='5432'
)

cur = con.cursor()
# Створення двох таблиць
cur.execute(
    "CREATE TABLE IF NOT EXISTS messages (theme VARCHAR(255),message TEXT NOT NULL,username VARCHAR(255),link_username VARCHAR(255),date VARCHAR(255))")

cur.execute(
    "CREATE TABLE IF NOT EXISTS users (username VARCHAR(255),user_title VARCHAR(255),happy_birth VARCHAR(255),city VARCHAR(255),occupation VARCHAR(255),last_active VARCHAR(255),pozitive_bal VARCHAR(255),neutral_bal VARCHAR(255),negative_bal VARCHAR(255))")


con.commit()


# Функція, якою повертаємо html сторінку
def get_html(url, params=None):
    r = requests.get(url, headers=HEADERS, params=params)
    return r


catalog_full_links = []  # Суто для ссилок з каталогу в теми
themes_name = []  # Назви ТЕМ
themes_full_links = []  # Суто для ссилок з ТЕМ в Повідомлення
short_links_on_user = []  # Ссилки на юзерів


# Функція, яка дістає html сторінку заданої в URL. Перевіряє статус,якщо 200 то пускає далі.
# Дальше робота з BS4, в першому циклі дістаємо ссилки, а в другому формуємо повну ссилку і передаємо в глобальний список
def get_catalog():
    catalog_html = get_html(URL)
    if catalog_html.status_code == 200:
        soup = BeautifulSoup(catalog_html.text, 'html5lib')
        catalog_names = soup.find_all('h3', {'class': 'nodeTitle'})
        catalog_short_links = []
        for item in catalog_names:
            catalog_short_links.append(item.find('a').get('href'))
        # print(len(catalog_links)) # ОТРИМАВ СПИСОК ССИЛОК
        for i in range(0, len(catalog_short_links)):
            full_link_to_themes = URL+catalog_short_links[i]
            catalog_full_links.append(full_link_to_themes)

    else:
        print('ERROR')
# Для збільшення ссилок, тобіш щоб воно ходило
#  по пагінатору вибору тем. Потрібно просто
# створити ще одну функцію, яка буде це робити
# (часточка "page-") +(last_page = c.previous_sibling.previous_sibling.text) --->
# цифра останньої сторінки пагінатора. Але тоді мега-довго буде парсить і закидати в БД


#  Схоже що і у попередній функції. Тільки тут є ще один цикл, якій формує назви Тем в глобальний список.
def get_themes():
    for item in range(1, len(catalog_full_links)):
        link_theme = catalog_full_links[item]
        html_theme = get_html(link_theme)
        soup = BeautifulSoup(html_theme.text, 'html5lib')
        theme_name = soup.find_all('h3', {'class': 'title'})
        theme_short_links = []
        for n in theme_name:
            themes_name.append(n.find('a').get_text())
        for item in theme_name:
            theme_short_links.append(item.find('a').get('href'))
        for i in range(0, len(theme_short_links)):
            full_link_to_message = URL+theme_short_links[i]
            themes_full_links.append(full_link_to_message)


# Дана функція, парсить повідомлення, які знайденні по ссилці із Тем.
# Та в циклі закидає найдену інформацію в БД
def get_message():
    for item in range(0, len(themes_full_links)):
        message_link = themes_full_links[item]
        message_name = themes_name[item]
        message_html = get_html(message_link)
        a = BeautifulSoup(message_html.text, 'html5lib')
        b = a.find_all('div', {'class': 'messageInfo'})
        for c in b:
            theme = message_name
            message = c.find(
                'blockquote', class_='messageText').get_text(strip=True)
            username = c.find('a', {'class': 'username'}).get_text(strip=True)
            link_username = c.find('a', {'class': 'username'}).get('href')
            short_links_on_user.append(link_username)
            date = c.find('a', {'class': 'datePermalink'}).get_text(strip=True)
            sql = "INSERT INTO messages (theme,message,username,link_username,date) VALUES (%s,%s, %s, %s, %s)"
            val = (theme, message, username, link_username, date)
            cur.execute(sql, val)
            con.commit()


# А ця функція, також схожа з попередньою, тільки тут є перевірка на елементи пошуку. Якщо є - супер,немає - дефолт значення
# Та в циклі закидає найдену інформацію в БД
def get_user():
    for item in range(0, len(short_links_on_user)):
        full_link_user = URL + str(short_links_on_user[item])
        user_html = get_html(full_link_user)
        soup = BeautifulSoup(user_html.text, 'html5lib')

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
            city = soup.find('a', {'itemprop': 'address'}).get_text(strip=True)
        else:
            city = 'Україна'

        occupation = soup.find('dd', {'itemprop': 'role'})
        if occupation:
            occupation = soup.find(
                'dd', {'itemprop': 'role'}).get_text(strip=True)
        else:
            occupation = 'Freedom'

        last_active = soup.find('div', {'class': 'ProfileNav_activites'})
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
        s = "INSERT INTO users (username,user_title,happy_birth,city,occupation,last_active,pozitive_bal,neutral_bal,negative_bal) VALUES (%s,%s, %s, %s, %s,%s, %s, %s, %s)"
        v = (username, user_title, happy_birth, city, occupation,
             last_active, pozitive_bal, neutral_bal, negative_bal)
        cur.execute(s, v)
        con.commit()


#  Виклик усіх функцій
get_catalog()
get_themes()
get_message()
get_user()
