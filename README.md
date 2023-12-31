[![Typing SVG](https://readme-typing-svg.herokuapp.com?color=%2336BCF7&lines=FINAL+PROJECT+DATA+ENGINEER)](https://git.io/typing-svg)
# Проект: Анализ публикуемых новостей

## Описание проекта
Этот проект представляет собой ETL-процесс формирования витрин данных для анализа публикаций новостей из различных источников. Проект был создан с использованием Apache Airflow для планирования и автоматизации ETL-процесса и PostgreSQL для хранения данных.

## Структура проекта
Проект построен на основе следующей структуры данных:
- **Сырой слой данных:** Хранит исходные данные, полученные из различных новостных источников, таких как Lenta.ru, Vedomosti и Tass.
- **Промежуточный слой:** Включает в себя трансформированные данные с учётом стандартизации категорий и предобработки информации.
- **Слой витрин:** Формирует витрину данных для анализа новостей.

## Стек технологий и достижения
- **Python:** Использовался для написания скриптов загрузки, предобработки и трансформации данных.
- **PostgreSQL:** База данных для хранения сырых, промежуточных данных и витрины. Была произведена оптимизация запросов для улучшения производительности.
- **Apache Airflow (DAG):** Настройка DAG'а для автоматизации и планирования ETL-процесса.
- **Диаграмма хранилища данных:** Построена диаграмма, отражающая структуру данных и взаимосвязи между слоями.
- **Оптимизация процесса загрузки данных:** Применены методы оптимизации загрузки для сокращения времени выполнения ETL-процесса.

## Ход работы
1. **Подготовка среды и загрузка данных:** Написание скриптов загрузки новостей из различных RSS-источников.
2. **Трансформация данных:** Предобработка и стандартизация данных перед загрузкой в БД.
3. **Создание витрины данных:** Формирование витрины данных согласно требованиям проекта.
4. **Планирование и автоматизация процесса:** Настройка DAG'а в Apache Airflow для выполнения ETL-процесса.

## Заключение
Этот проект подчёркивает мои навыки работы с базами данных, написания скриптов на Python, использования Apache Airflow для планирования ETL-процессов и оптимизации загрузки данных. Данный проект отражает моё стремление к решению технических задач и применению передовых технологий в области Data Engineering.
