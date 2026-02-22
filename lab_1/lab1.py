import csv  # работа с CSV-файлами - чтение и запись таблиц
import random  # генерация случайных чисел
import math  # математические функции
import multiprocessing  # параллельная обработка


# создаёт csv файлы со случайными данными
def generate_csv_files():
    categories = ['A', 'B', 'C', 'D']

    for num in range(1, 6):
        filename = f'file_{num}.csv'

        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)

            writer.writerow(['Категория', 'Значение'])

            for i in range(1, 21):
                category = random.choice(categories)
                value = random.uniform(0, 100)
                writer.writerow([category, value])

        print(f"Файл {filename} создан")


# вычисляет медиану списка чисел
def cal_median(data):
    if not data:
        return 0

    sorted_d = sorted(data)
    n = len(sorted_d)

    if n % 2 == 1:
        return sorted_d[n // 2]
    else:
        mid = n // 2
        return (sorted_d[mid - 1] + sorted_d[mid]) / 2


# вычисляет стандартное отклонение выборки
def deviat(data):
    if len(data) <= 1:
        return 0

    mean = sum(data) / len(data)

    # сумму квадратов отклонений от среднего
    # делим на (n-1) для несмещённой оценки выборки
    deviant = sum((x - mean) ** 2 for x in data) / (len(data) - 1)

    return math.sqrt(deviant)


# обрабатывает csv файл: группирует по категориям считает статистику
def onefile(filename):
    category_d = {}

    with open(filename, 'r', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)

        for row in reader:
            category = row['Категория']
            value = float(row['Значение'])

            if category not in category_d:
                category_d[category] = []

            category_d[category].append(value)

    results = {}

    for category, values in category_d.items():
        median = cal_median(values)
        std_dev = deviat(values)
        results[category] = {'median': median, 'std_dev': std_dev}

    return results


# параллельно обрабатывает все файлы
def parallel():
    print("\nПараллельная обработка файлов")

    files_to_process = [f'file_{num}.csv' for num in range(1, 6)]

    num_cores = multiprocessing.cpu_count()

    # пул процессов для параллельной работы
    with multiprocessing.Pool(processes=num_cores) as pool:
        # pool.map автоматически распределяет задачи по ядрам
        all_results = pool.map(onefile, files_to_process)

    all_medians = {}

    print("\nРезультаты обработки каждого файла")

    for i, file_results in enumerate(all_results, 1):
        print(f"\nФайл file_{i}.csv:")

        for category in sorted(file_results.keys()):
            median = file_results[category]['median']
            std_dev = file_results[category]['std_dev']

            print(f"  {category}: медиана = {median:.2f}, отклонение = {std_dev:.2f}")

            if category not in all_medians:
                all_medians[category] = []
            all_medians[category].append(median)

    print("\nМедиана медиан и стандартное отклонение медиан")

    final_results = {}

    for category in sorted(all_medians.keys()):
        medians_list = all_medians[category]

        median_of_medians = cal_median(medians_list)
        std_dev_of_medians = deviat(medians_list)

        final_results[category] = {
            'median_of_medians': median_of_medians,
            'std_dev_of_medians': std_dev_of_medians
        }

        print(f"{category}: медиана медиан = {median_of_medians:.2f}, "
              f"стандартное отклонение медиан = {std_dev_of_medians:.2f}")

    return final_results


if __name__ == "__main__":
    generate_csv_files()
    final_results = parallel()