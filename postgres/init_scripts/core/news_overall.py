import psycopg2
from datetime import datetime

# Подключение к базе данных
conn = psycopg2.connect(
    dbname='airflow',
    user='airflow',
    password='airflow',
    host='localhost',
    port='5432'
)
cur = conn.cursor()

# Создание таблицы news_overall
create_table_query = '''
CREATE TABLE IF NOT EXISTS news_overall (
    news_id SERIAL PRIMARY KEY,
    title VARCHAR(255),
    description TEXT,
    pub_date TIMESTAMP,
    source VARCHAR(50)
);
'''
cur.execute(create_table_query)
conn.commit()

# Вставка данных из таблиц vedomosti, tass и lenta в таблицу news_overall
tables = ['vedomosti', 'tass', 'lenta']

for table in tables:
    source = table  # Источник данных
    insert_query = f'''
    INSERT INTO news_overall (title, description, pub_date, source)
    SELECT title, description, TIMESTAMP %s, '{source}' AS source
    FROM {table};
    '''
    cur.execute(insert_query, (datetime.now(),))
    conn.commit()

# Закрываем соединение с базой данных
cur.close()
conn.close()
