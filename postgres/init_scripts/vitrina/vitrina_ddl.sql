CREATE TABLE IF NOT EXISTS vitrina (
    client_id SERIAL PRIMARY KEY,
    category_id BIGINT,
    category_name VARCHAR(255),
    news_amount BIGINT,
    news_amount_lenta BIGINT,
    news_amount_vedomosti BIGINT,
    news_amount_tass BIGINT,
    news_amount_last_day BIGINT,
    news_amount_amg_day BIGINT,
    day_of_max_news VARCHAR(20)
    
);
