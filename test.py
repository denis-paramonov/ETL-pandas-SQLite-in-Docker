import sqlite3

# Подключаемся к базе данных
conn = sqlite3.connect('data/transactions.db')

# Создаем курсор, который позволяет нам выполнять SQL-запросы
cursor = conn.cursor()

# Выполняем SELECT-запрос
cursor.execute("select * from transactions")

# Извлекаем все полученные строки
rows = cursor.fetchall()

# Печатаем каждую строку
for row in rows:
    print(row)

# Закрываем соединение
conn.close()