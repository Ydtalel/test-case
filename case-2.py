class MaxPositiveLength:
    def __init__(self, file_name):
        """
        Конструктор класса MaxPositiveLength.

        :param file_name: Название файла с данными.
        """
        self.file_name = file_name

    def calculate(self):
        """
        Метод calculate выполняет вычисление максимальной длины последовательности положительных чисел в файле.

        :return: Максимальная длина последовательности положительных чисел.
        """
        with open(self.file_name, 'r') as file:
            next(file)  # Пропускаем первую строку
            numbers = [int(line) for line in file]

        max_length = 0
        current_length = 0

        for num in numbers:
            if num > 0:
                current_length += 1
                if current_length > max_length:
                    max_length = current_length
            else:
                current_length = 0

        return max_length


file_name = "csv_files/numbers.csv"  # Название файла с данными

calculator = MaxPositiveLength(file_name)
result = calculator.calculate()

print("max_length")
print(result)
