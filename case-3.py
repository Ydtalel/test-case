import csv


def read_operations(operations_file):
    # Чтение операций из CSV файла
    operations = []
    with open(operations_file, 'r') as file:
        reader = csv.reader(file)
        next(reader)  # Пропускаем заголовок
        for row in reader:
            operation_id = int(row[0])
            year = int(row[1])
            month = int(row[2])
            day = int(row[3])
            department_id = int(row[4])
            income = int(row[5])
            operations.append((operation_id, year, month, day, department_id, income))
    return operations


def read_departments(departments_file):
    # Чтение подразделений из CSV файла
    departments = {}
    with open(departments_file, 'r') as file:
        reader = csv.reader(file)
        next(reader)  # Пропускаем заголовок
        for row in reader:
            department_id = int(row[0])
            start_year = int(row[1])
            end_year = int(row[2])
            department_name = row[3]
            departments[department_id] = (start_year, end_year, department_name)
    return departments


def calculate_department_profit(operations, departments):
    # Вычисление прибыли подразделений
    department_profit = {}

    for operation in operations:
        operation_id = operation[0]
        year = operation[1]
        month = operation[2]
        day = operation[3]
        department_id = operation[4]
        income = operation[5]
        if month > 12 or day > 31:
            continue

        if department_id in departments:
            start_year = departments[department_id][0]
            end_year = departments[department_id][1]
            department_name = departments[department_id][2]
            if start_year <= year <= end_year:
                if year not in department_profit:
                    department_profit[year] = {}
                if month not in department_profit[year]:
                    department_profit[year][month] = {}
                if department_name not in department_profit[year][month]:
                    department_profit[year][month][department_name] = 0
                department_profit[year][month][department_name] += income

    return department_profit


def save_results(result, output_file):
    # Сохранение результатов в CSV файл
    with open(output_file, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['year', 'month', 'department_name', 'profit'])
        for year in result.keys():
            for month in result[year].keys():
                for department_name in result[year][month].keys():
                    profit = result[year][month][department_name]
                    writer.writerow([year, month, department_name, profit])


operations_file = "csv_files/operations.csv"
departments_file = "csv_files/departments.csv"
output_file = "csv_files/result.csv"

# Чтение операций и подразделений
operations = read_operations(operations_file)
departments = read_departments(departments_file)

# Вычисление прибыли подразделений
result = calculate_department_profit(operations, departments)

# Сохранение результатов в файл
save_results(result, output_file)

# Вывод результатов в консоль
for year in result.keys():
    for month, departments in result[year].items():
        for department_name, profit in departments.items():
            print(f"{year}, {month}, {department_name}, {profit}")