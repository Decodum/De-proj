# tass.py

import psycopg2
import requests
import xml.etree.ElementTree as ET

def create_table_tass():
    conn = psycopg2.connect(
        host='postgres',
        port='5432',
        user='airflow',
        password='airflow',
        dbname='airflow'
    )
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS tass (
            id SERIAL PRIMARY KEY,
            title VARCHAR(255),
            description TEXT,
            pub_date VARCHAR(255)
        )
    """)
    conn.commit()
    cursor.close()
    conn.close()
    print("Таблица 'tass' создана успешно.")

def fetch_rss_tass_initial():
    create_table_tass()  

    rss_url = "https://tass.ru/rss/v2.xml"
    response = requests.get(rss_url)
    if response.status_code == 200:
        root = ET.fromstring(response.content)
        rss_data = []
        for item in root.findall('.//item'):
            title = item.find('title').text if item.find('title') is not None else ''
            description = item.find('description').text if item.find('description') is not None else ''
            pub_date = item.find('pubDate').text if item.find('pubDate') is not None else ''
            rss_data.append({'title': title, 'description': description, 'pub_date': pub_date})

        conn = psycopg2.connect(
            host='postgres',
            port='5432',
            user='airflow',
            password='airflow',
            dbname='airflow'
        )
        cursor = conn.cursor()

        for item in rss_data:
            cursor.execute("""
                INSERT INTO tass (title, description, pub_date)
                VALUES (%s, %s, %s)
            """, (item['title'], item['description'], item['pub_date']))

        conn.commit()
        cursor.close()
        conn.close()
        print("Данные из RSS-ленты Тасс успешно добавлены в таблицу в базе данных.")
    else:
        print("Не удалось получить данные из RSS-ленты Тасс.")

def fetch_rss_tass_incremental():
    conn = psycopg2.connect(
        host='postgres',
        port='5432',
        user='airflow',
        password='airflow',
        dbname='airflow'
    )
    cursor = conn.cursor()

    # Получить дату последней загрузки из вашей базы данных
    cursor.execute("SELECT MAX(pub_date) FROM tass")
    last_loaded_date = cursor.fetchone()[0]

    rss_url = "https://tass.ru/rss/v2.xml"
    response = requests.get(rss_url)
    if response.status_code == 200:
        root = ET.fromstring(response.content)
        rss_data = []
        for item in root.findall('.//item'):
            title = item.find('title').text if item.find('title') is not None else ''
            description = item.find('description').text if item.find('description') is not None else ''
            pub_date = item.find('pubDate').text if item.find('pubDate') is not None else ''

            # Проверка на новые данные по дате
            if pub_date > last_loaded_date:
                rss_data.append({'title': title, 'description': description, 'pub_date': pub_date})

        # Добавить только новые данные в базу данных
        for item in rss_data:
            cursor.execute("""
                INSERT INTO tass (title, description, pub_date)
                VALUES (%s, %s, %s)
            """, (item['title'], item['description'], item['pub_date']))

        conn.commit()
        cursor.close()
        conn.close()
        print("Инкрементальная загрузка данных из RSS-ленты Тасс завершена.")
    else:
        print("Не удалось получить данные из RSS-ленты Тасс.")
