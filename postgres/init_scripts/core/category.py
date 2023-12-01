import psycopg2

# Подключение к базе данных
conn = psycopg2.connect(
    dbname='airflow',
    user='airflow',
    password='airflow',
    host='localhost',
    port='5432'
)
cur = conn.cursor()

# Создание таблицы category, если её нет
create_category_table_query = '''
CREATE TABLE IF NOT EXISTS category (
    news_id SERIAL PRIMARY KEY,
    category_id INT,
    category_name VARCHAR(255)
);
'''
cur.execute(create_category_table_query)
conn.commit()

# Функция для определения категории новости
def get_category(news_title):
    categories = {
        "Политика/Международные отношения": ["Песков", "Министр", "Международные отношения", "Путин"],
        "Экономика/Финансы": ["IT-разработки", "финансирования", "финансы", "экономика"],
        "Погода/Климат": ["погода", "прогноз", "дождь", "снег"],
        "Культура/Кино/Искусство": ["российские фильмы", "кино", "культура", "выставка"],
        "Технологии/Наука": ["новые технологии", "наука", "инновации"],
        "События в регионах": ["Мариуполе", "Краснодарском крае", "Тольятти"]
    }

    for category, keywords in categories.items():
        for keyword in keywords:
            if keyword.lower() in news_title.lower():
                return category
    return None

# Получение новостей из таблицы news_overall
cur.execute("SELECT news_id, title FROM news_overall")
news_items = cur.fetchall()

# Вставка данных в таблицу category
for news_id, title in news_items:
    category_name = get_category(title)
    if category_name:
        # Поиск соответствующего category_id для category_name
        category_ids = {
            "Политика/Международные отношения": 1,
            "Экономика/Финансы": 2,
            "Погода/Климат": 3,
            "Культура/Кино/Искусство": 4,
            "Технологии/Наука": 5,
            "События в регионах": 6
        }
        category_id = category_ids.get(category_name)

        # Вставка записи в таблицу category
        if category_id:
            cur.execute(
                "INSERT INTO category (news_id, category_id, category_name) VALUES (%s, %s, %s)",
                (news_id, category_id, category_name)
            )
            conn.commit()

# Закрытие соединения с базой данных
cur.close()
conn.close()
