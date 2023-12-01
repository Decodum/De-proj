INSERT INTO vitrina (
    category_id,
    category_name,
    news_amount,
    news_amount_lenta,
    news_amount_vedomosti,
    news_amount_tass,
    news_amount_last_day,
    news_amount_amg_day,
    day_of_max_news
)
SELECT
    c.category_id,
    c.category_name,
    COUNT(news.news_id) AS news_amount,
    COUNT(CASE WHEN news.source = 'lenta' THEN 1 END) AS news_amount_lenta,
    COUNT(CASE WHEN news.source = 'vedomosti' THEN 1 END) AS news_amount_vedomosti,
    COUNT(CASE WHEN news.source = 'tass' THEN 1 END) AS news_amount_tass,
    COUNT(CASE WHEN cal.pub_date >= CURRENT_DATE - INTERVAL '1 DAY' THEN 1 END) AS news_amount_last_day,
    COUNT(CASE WHEN cal.pub_date >= CURRENT_DATE - INTERVAL '1 DAY' THEN 1 END) AS news_amount_amg_day,
    to_char(MAX(cal.pub_date), 'Day') AS day_of_max_news
FROM
    category c
LEFT JOIN
    news_overall news ON c.news_id = news.news_id
LEFT JOIN
    calendar cal ON news.news_id = cal.news_id
GROUP BY
    c.category_id, c.category_name;
