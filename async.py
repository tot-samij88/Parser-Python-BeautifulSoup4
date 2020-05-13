import requests
from bs4 import BeautifulSoup
import psycopg2
import asyncio
from time import time
import aiohttp


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

url = "http://forum.lvivport.com/members/"
url1 = 'http://forum.lvivport.com/threads/'
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.122 Safari/537.36 '}


async def get_html(url):
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=HEADERS) as resp:
            return await resp.text()


async def user():
    for item in range(1, 11):
        response = await get_html(url + str(item))

        sql = "SELECT id_user FROM users WHERE id_user=%s"
        val = (str(item),)
        cur.execute(sql, val)
        chek = cur.fetchone()

        if chek == None:
            soup = BeautifulSoup(response, 'html5lib')
            link_user = soup.find('a', {'class': 'username'}).get('href')
            user_id = link_user.split('.')
            user_id = user_id[1].replace('/', '')
            try:
                username = soup.find(
                    'a', {'class': 'username'}).get_text(strip=True)
            except:
                username = 'new_member'

            try:
                user_title = soup.find(
                    'span', {'class': 'userTitle'}).get_text(strip=True)
            except:
                user_title = ' I LOVE LVIV '

            try:
                happy_birth = soup.find(
                    'span', {'class': 'dob'}).get_text(strip=True)
            except:
                happy_birth = ' close info '

            try:
                city = soup.find(
                    'a', {'itemprop': 'address'}).get_text(strip=True)
            except:
                city = 'Україна'

            try:
                occupation = soup.find(
                    'dd', {'itemprop': 'role'}).get_text(strip=True)
            except:
                occupation = 'Freedom'

            try:
                last_active = soup.find(
                    'div', {'class': 'ProfileNav_activites'}).get_text(strip=True)
            except:
                last_active = 'last seen recently'
            try:
                pozitive_bal = soup.find(
                    'dd', {'class': 'dark_postrating_positive'}).get_text(strip=True)
            except:
                pozitive_bal = 'member has not pozitive score'
            try:
                neutral_bal = soup.find(
                    'dd', {'class': 'dark_postrating_neutral'}).get_text(strip=True)
            except:
                neutral_bal = 'member has not neutral score'
            try:
                negative_bal = soup.find(
                    'dd', {'class': 'dark_postrating_negative'}).get_text(strip=True)
            except:
                negative_bal = 'member has not negative score'
            s = "INSERT INTO users (id_user,username,user_title,happy_birth,city,occupation,last_active,pozitive_bal,neutral_bal,negative_bal) VALUES (%s,%s, %s, %s, %s,%s, %s, %s, %s, %s)"
            v = (user_id, username, user_title, happy_birth, city, occupation,
                 last_active, pozitive_bal, neutral_bal, negative_bal)
            cur.execute(s, v)
            con.commit()
            print(chek, "VS", item, '-', "ЗАЛЕТАЕТ В БД")
        else:
            print(chek[0], "VS", item, '-', "УЖЕ В БД")

    await asyncio.sleep(1)


async def message():
    for i in range(33678, 33689):
        res = await get_html(url1 + str(i))
        a = BeautifulSoup(res, 'html5lib')
        try:
            d = a.find('p', {'class': 'muted'}).get_text(strip=True)
            d = d.split(',')
            d = d[0]
        except:
            continue
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
            val = (d, message, username, date, user_id)
            cur.execute(sql, val)
            con.commit()

    await asyncio.sleep(1)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(get_html(url))
    loop.run_until_complete(get_html(url))
    try:
        start = time()
        coroutines = [
            loop.create_task(user()),
            loop.create_task(message())
        ]
        loop.run_until_complete(asyncio.wait(coroutines))
    finally:
        loop.close()
        print(f"Время выполнения: {time() - start}")
