import sqlite3
import os
import pandas as pd

# Подключаемся к базе данных
conn = sqlite3.connect('data/transactions.db')

# Создаем курсор, который позволяет нам выполнять SQL-запросы
cursor = conn.cursor()

# Читаем SQL-запрос из файла
with open('scripts/uniq_users_purchases.sql', 'r') as file:
    sql_query = file.read()

# Выполняем SELECT-запрос и сохраняем результат в DataFrame
df = pd.read_sql_query(sql_query, conn)

# Указываем путь к директории и имени выходного файла
output_directory = 'output'
output_file_path = os.path.join(output_directory, 'output.csv')

# Создаем директорию, если она не существует
if not os.path.exists(output_directory):
    os.makedirs(output_directory)

# Сохраняем DataFrame в CSV-файл
df.to_csv(output_file_path, index=False)
print(f"Here  {output_file_path}")

# Закрываем соединение
conn.close()