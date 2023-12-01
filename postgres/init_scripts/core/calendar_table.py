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

# Создание таблицы calendar с новым столбцом calendar_id
create_table_query = '''
CREATE TABLE IF NOT EXISTS calendar (
    calendar_id SERIAL PRIMARY KEY,
    pub_date DATE,
    news_id INTEGER,
    day_of_week INTEGER,
    week_day VARCHAR(10)
);
'''
cur.execute(create_table_query)
conn.commit()

# Заполнение таблицы calendar данными из news_overall
select_query = '''
SELECT news_id, pub_date
FROM news_overall;
'''
cur.execute(select_query)
news_dates = cur.fetchall()

for data in news_dates:
    news_id = data[0]
    pub_date = data[1]
    formatted_date = datetime.strptime(str(pub_date), '%Y-%m-%d').date()
    day_of_week = formatted_date.isoweekday()
    weekday = formatted_date.strftime('%a').lower()

    insert_query = '''
    INSERT INTO calendar (pub_date, news_id, day_of_week, week_day)
    VALUES (%s, %s, %s, %s);
    '''
    cur.execute(insert_query, (formatted_date, news_id, day_of_week, weekday))
    conn.commit()

# Закрываем соединение с базой данных
cur.close()
conn.close()
