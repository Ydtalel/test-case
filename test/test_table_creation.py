import csv
import pytest
import psycopg2
import os
from dotenv import load_dotenv, find_dotenv

# Загрузка переменных окружения
load_dotenv(find_dotenv())

# Параметры подключения к базе данных
db_params = {
    'host': '127.0.0.1',
    'port': '5432',
    'database': 'library_db',
    'user': 'postgres',
    'password': os.environ.get("db_password")
}


@pytest.fixture(scope="module")
def db_connection():
    # Установка соединения с базой данных
    conn = psycopg2.connect(**db_params)
    yield conn
    # Закрытие соединения с базой данных
    conn.close()


@pytest.fixture(scope="module")
def db_cursor(db_connection):
    # Создание курсора базы данных
    cur = db_connection.cursor()
    yield cur
    # Закрытие курсора базы данных
    cur.close()


def test_tables_existence(db_cursor):
    """
    Проверка существования таблиц
    """
    cur = db_cursor
    # Проверка существования таблиц
    tables = ['Книги', 'Читатели', 'Выдачи_книг']
    for table in tables:
        query = f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '{table}')"
        cur.execute(query)
        exists = cur.fetchone()[0]
        assert exists, f"Таблица {table} не существует в базе данных"


def get_csv_row_count(file_path):
    with open(file_path, 'r', encoding='cp1251') as file:
        reader = csv.reader(file)
        row_count = sum(1 for row in reader)
    return row_count


def test_record_counts(db_cursor):
    """
    Проверка количества записей в таблицах базы данных
    """
    cur = db_cursor
    # Сопоставление имен таблиц и путей к соответствующим CSV-файлам
    table_csv_mapping = {
        'Книги': '../csv_files/books.csv',
        'Читатели': '../csv_files/readers.csv',
        'Выдачи_книг': '../csv_files/checkouts.csv'
    }

    for table, csv_file in table_csv_mapping.items():
        # Получение количества записей в таблице из базы данных
        query = f"SELECT COUNT(*) FROM {table}"
        cur.execute(query)
        db_record_count = cur.fetchone()[0]

        # Получение количества строк в CSV-файле
        csv_record_count = get_csv_row_count(csv_file)

        # Утверждение, что количество записей соответствует. +1 т.к. в БД не учитывается заголовок
        assert db_record_count + 1 == csv_record_count, f"Несоответствие количества записей в таблице {table}: " \
                                                        f"ожидалось {csv_record_count}, получено {db_record_count}"

# запуск теста в терминале командой pytest test_table_creation.py
