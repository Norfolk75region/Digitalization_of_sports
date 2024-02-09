from r5.helpers.general import Logger
import openpyxl
import os
import csv
import glob

log = Logger()

def main(file_path):
    # Загрузка книги Excel
    workbook = openpyxl.load_workbook(file_path)

    # Получение первого листа
    first_sheet = workbook.active

    # Открытие файла CSV для записи
    csv_file_path = os.path.splitext(file_path)[0]+'.csv'
    with open(csv_file_path, 'w', newline='') as csv_file:
        writer = csv.writer(csv_file,delimiter=';')

        # Запись содержимого листа Excel в файл CSV
        for row in first_sheet.iter_rows():
            writer.writerow([cell.value for cell in row])

full_dict_path = '/app/cache'+os.getenv('AF_LOAD_PATH')

for file_name in glob.glob(os.path.join(full_dict_path, '*.xlsx')):
    print(full_dict_path, os.path.basename(file_name))
    main(file_name)

