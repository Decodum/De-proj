-- Создание типов
CREATE TYPE book_status AS ENUM ('t', 'r');
CREATE TYPE gender AS ENUM ('m', 'f');

-- Создание таблиц
CREATE TABLE IF NOT EXISTS readers (
    reading_ticket BIGINT PRIMARY KEY,
    full_name VARCHAR(255),
    address VARCHAR(255),
    phone_number BIGINT,
    registr_time TIMESTAMP,
    birthday DATE
);

CREATE TABLE IF NOT EXISTS authors (
    author_id BIGINT PRIMARY KEY,
    author_name VARCHAR(255),
    gender gender
);

CREATE TABLE IF NOT EXISTS books (
    book_id SERIAL PRIMARY KEY,
    book_name VARCHAR(255),
    author_id BIGINT REFERENCES authors(author_id),
    created_year DATE,
    amount_pages BIGINT,
    amount BIGINT,
    price BIGINT,
    publisher_id BIGINT
);

CREATE TABLE IF NOT EXISTS borrow_table (
    id SERIAL PRIMARY KEY,
    status book_status,
    date_start TIMESTAMP,
    date_issue TIMESTAMP,
    reading_ticket BIGINT REFERENCES readers(reading_ticket),
    book_id BIGINT REFERENCES books(book_id),
    FOREIGN KEY (reading_ticket) REFERENCES readers(reading_ticket) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS publisher (
    publisher_id BIGINT PRIMARY KEY,
    city VARCHAR(255),
    publisher_name VARCHAR(255),
    book_id BIGINT REFERENCES books(book_id)
);

CREATE TABLE IF NOT EXISTS authors_books (
    ab_key SERIAL PRIMARY KEY,
    book_id BIGINT REFERENCES books(book_id),
    author_id BIGINT REFERENCES authors(author_id)
);