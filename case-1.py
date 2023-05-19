import psycopg2
from dotenv import load_dotenv, find_dotenv

import csv
import os
from datetime import datetime, timedelta

load_dotenv(find_dotenv())

db_params = {
    'host': '127.0.0.1',
    'port': '5432',
    'database': 'library_db',
    'user': 'postgres',
    'password': os.environ.get("db_password")
}


# Создание таблиц
def create_tables():
    commands = (
        """
        CREATE TABLE IF NOT EXISTS Книги (
            ID_книги SERIAL PRIMARY KEY,
            Название TEXT,
            Автор TEXT,
            Издательство TEXT,
            Год_издания INT,
            Город_издания TEXT,
            Количество_страниц INT,
            ID_экземпляра INT
        )
        """,
        """
           CREATE TABLE IF NOT EXISTS Читатели (
               №_читательского_билета INT PRIMARY KEY,
               Фамилия TEXT,
               Имя TEXT,
               Отчество TEXT,
               Дата_рождения DATE,
               Пол TEXT,
               Адрес TEXT,
               Телефон TEXT
           )
       """,

        """
        CREATE TABLE IF NOT EXISTS Выдачи_книг (
            ID_экземпляра INT,
            Дата_выдачи DATE,
            Дата_возврата DATE,
            №_читательского_билета INT,
            FOREIGN KEY (№_читательского_билета) REFERENCES Читатели (№_читательского_билета)
        )
        """
    )

    conn = None
    try:
        conn = psycopg2.connect(**db_params)
        cur = conn.cursor()
        for command in commands:
            cur.execute(command)
        cur.close()
        conn.commit()
        print("Таблицы успешно созданы")
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


# Запись книг в базу данных
def insert_books(books):
    conn = None
    try:
        conn = psycopg2.connect(**db_params)
        cur = conn.cursor()
        for book in books:
            cur.execute("""
                INSERT INTO Книги (Название, Автор, Издательство, Год_издания, Город_издания, Количество_страниц, ID_экземпляра)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                        (
                            book['Название'],
                            book['Автор'],
                            book['Издательство'],
                            book['Год_издания'],
                            book['Город_издания'],
                            book['Количество_страниц'],
                            book['ID_экземпляра']
                        )
                        )
        cur.close()
        conn.commit()
        print("Книги успешно записаны в базу данных")
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


# Запись выдач книг в базу данных
def insert_checkouts(checkouts):
    conn = None
    try:
        conn = psycopg2.connect(**db_params)
        cur = conn.cursor()
        for checkout in checkouts:
            cur.execute("""
                INSERT INTO Выдачи_книг (ID_экземпляра, Дата_выдачи, Дата_возврата, №_читательского_билета)
                VALUES (%s, %s, %s, %s)
                """,
                        (
                            checkout['ID_экземпляра'],
                            checkout['Дата_выдачи'],
                            checkout['Дата_возврата'],
                            checkout['№_читательского_билета']
                        )
                        )
        cur.close()
        conn.commit()
        print("Выдачи книг успешно записаны в базу данных")
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


# Запись читателей в базу данных
def insert_readers(readers):
    conn = None
    try:
        conn = psycopg2.connect(**db_params)
        cur = conn.cursor()
        for reader in readers:
            cur.execute("""
                INSERT INTO Читатели (№_читательского_билета, Фамилия, Имя, Отчество, Дата_рождения, Пол, Адрес, Телефон)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                        (
                            reader['№_читательского_билета'],
                            reader['Фамилия'],
                            reader['Имя'],
                            reader['Отчество'],
                            reader['Дата_рождения'],
                            reader['Пол'],
                            reader['Адрес'],
                            reader['Телефон']
                        )
                        )
        cur.close()
        conn.commit()
        print("Читатели успешно записаны в базу данных")
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


# Чтение читателей из CSV файла и запись в базу данных
def read_readers_from_csv(file_path):
    readers = []
    with open(file_path, 'r', encoding='cp1251') as file:
        reader = csv.DictReader(file, delimiter=';')
        for row in reader:
            reader_data = {
                '№_читательского_билета': int(row['№ читательского билета']),
                'Фамилия': row['Фамилия'],
                'Имя': row['Имя'],
                'Отчество': row['Отчество'],
                'Дата_рождения': datetime.strptime(row['Дата рождения'], '%Y-%m-%d') if row['Дата рождения'] else None,
                'Пол': row['Пол'],
                'Адрес': row['Адрес'] if row['Адрес'] else None,
                'Телефон': row['Телефон'] if row['Телефон'] else None
            }
            readers.append(reader_data)

    # Запись читателей в базу данных
    insert_readers(readers)


# Чтение выдач книг из CSV файла и запись в базу данных
def read_checkouts_from_csv(file_path):
    checkouts = []
    with open(file_path, 'r', encoding='cp1251') as file:
        reader = csv.DictReader(file, delimiter=';')
        for row in reader:
            if '-' in row['Дата выдачи']:
                checkout_date = datetime.strptime(row['Дата выдачи'], '%Y-%m-%d').strftime('%d-%m-%Y')
            else:
                checkout_date = datetime.strptime(row['Дата выдачи'], '%d.%m.%Y').strftime('%d-%m-%Y')
            if '-' in row['Дата возврата']:
                return_date = datetime.strptime(row['Дата возврата'], '%Y-%m-%d').strftime('%d-%m-%Y')
            else:
                return_date = datetime.strptime(row['Дата возврата'], '%d.%m.%Y').strftime('%d-%m-%Y')

            checkout = {
                'ID_экземпляра': int(row['ID экземпляра']),
                'Дата_выдачи': checkout_date,
                'Дата_возврата': return_date,
                '№_читательского_билета': int(row['№ читательского билета'])
            }
            checkouts.append(checkout)

    # Запись выдач книг в базу данных
    insert_checkouts(checkouts)


# Чтение книг из CSV файла и запись в базу данных
def read_books_from_csv(file_path):
    books = []
    with open(file_path, 'r', encoding='cp1251') as file:
        reader = csv.DictReader(file, delimiter=';')
        for row in reader:
            book = {
                'Название': row['Название'],
                'Автор': row['Автор'],
                'Издательство': row['Издательство'],
                'Год_издания': calculate_publishing_year(row['Дата поступления в библиотеку']),
                'Город_издания': row['Город издания'],
                'Количество_страниц': int(row['Количество страниц']),
                'ID_экземпляра': int(row['ID экземпляра']) if row['ID экземпляра'].isdigit() else None
            }
            books.append(book)

    # Запись книг в базу данных
    insert_books(books)


# Вычисление года издания на основе даты поступления в библиотеку
def calculate_publishing_year(date_string):
    date_format = '%d.%m.%Y'
    date_object = datetime.strptime(date_string, date_format)
    publishing_year = date_object.year - 5
    return publishing_year


create_tables()
read_books_from_csv('csv_files/books.csv')
read_readers_from_csv('csv_files/readers.csv')
read_checkouts_from_csv('csv_files/checkouts.csv')
