import psycopg2
from dotenv import load_dotenv, find_dotenv

import os

load_dotenv(find_dotenv())

db_params = {
    'host': '127.0.0.1',
    'port': '5432',
    'database': 'library_db',
    'user': 'postgres',
    'password': os.environ.get("db_password")
}

# Подключение к базе данных
conn = psycopg2.connect(**db_params)

# Запрос: Найти города, в которых в указанном году было издано максимальное количество книг
query1 = """
SELECT "Город_издания"
FROM Книги
WHERE "Год_издания" = 2016
GROUP BY "Город_издания"
HAVING COUNT(*) = (
    SELECT COUNT(*) as count
    FROM Книги
    WHERE "Год_издания" = 2016
    GROUP BY "Город_издания"
    ORDER BY count DESC
    LIMIT 1
)
ORDER BY "Город_издания";
"""

# Запрос: Вывести количество экземпляров книг "Война и мир" Л.Н.Толстого, которые находятся в библиотеке
query2 = """
SELECT COUNT("id_экземпляра") as count
FROM Книги
WHERE Название = 'Война и мир' AND Автор = 'Л.Н.Толстой';
-- можно изменить на WHERE Название LIKE '%Война и мир%' AND Автор LIKE '%Толстой%' для неполного совпадения на случай 
-- если автор указан как 'Толстой Л.Н.' или название указано 'Война и мир.'
"""

# Запрос: Найти читателей, которые за последний месяц брали больше всего книг в библиотеке и сортировать по возрасту
query3 = """
SELECT Читатели.Фамилия, Читатели.Имя, Читатели.Отчество, Читатели."Дата_рождения", "Выдачи_книг"."Дата_выдачи", 
COUNT(*) as count
FROM Читатели
JOIN "Выдачи_книг" ON Читатели."№_читательского_билета" = "Выдачи_книг"."№_читательского_билета"
WHERE "Выдачи_книг"."Дата_выдачи" >= CURRENT_DATE - INTERVAL '1 month'
GROUP BY Читатели."№_читательского_билета", Читатели.Фамилия, Читатели.Имя, Читатели.Отчество, Читатели."Дата_рождения",
 "Выдачи_книг"."Дата_выдачи"
ORDER BY DATE_PART('year', CURRENT_DATE) - DATE_PART('year', Читатели."Дата_рождения") ASC;
"""

# Выполнение запросов
cur = conn.cursor()

# Запрос 1
cur.execute(query1)
result1 = cur.fetchall()
print(f"всего городов: {len(result1)}")
print("Города, в которых в указанном году было издано максимальное количество книг:\n")
for row in result1:
    print("Город:", row[0])

# Запрос 2
cur.execute(query2)
result2 = cur.fetchall()
print("\nКоличество экземпляров книг 'Война и мир' Л.Н.Толстого, которые находятся в библиотеке:")
for row in result2:
    print("Количество экземпляров:", row[0])

# Запрос 3
cur.execute(query3)
result3 = cur.fetchall()
print("\nЧитатели, которые за последний месяц брали больше всего книг в библиотеке (сортировка по возрасту):")
for row in result3:
    print("Фамилия:", row[0])
    print("Имя:", row[1])
    print("Отчество:", row[2])
    print("Дата рождения:", row[3])
    print("Дата выдачи:", row[4])
    print("Количество книг:", row[5])
    print()

# Закрытие соединения с базой данных
cur.close()
conn.close()
