import pandas as pd
from sqlalchemy import create_engine, Table, Column, Integer, String, Float, Boolean, Date, MetaData, Index, inspect
import logging
import sys

# Настройка логирования
logging.basicConfig(
    filename='../logs/etl.log',
    level=logging.INFO,
    format='%(asctime)s:%(levelname)s:%(message)s'
)

def extract(file_path):
    try:
        logging.info(f"Начало извлечения данных из {file_path}")
        df = pd.read_excel(file_path)
        logging.info("Извлечение данных успешно завершено")
        return df
    except Exception as e:
        logging.error(f"Ошибка на этапе извлечения данных: {e}")
        sys.exit(1)

def transform(df):
    try:
        logging.info("Начало трансформации данных")

        # Удаление лишних пробелов в названиях столбцов
        df.columns = df.columns.str.strip()
        logging.info("Удалены пробелы в названиях столбцов")

        # Выбор строк только с подходящим типом user_id
        df = df[df['user_id'].apply(lambda x: isinstance(x, int))]
        logging.info("Выбраны строки только с типом данных int у user_id")
        
        # Преобразование типов данных
        df['transaction_date'] = pd.to_datetime(df['transaction_date'])
        df['user_id'] = df['user_id'].astype('int64')
        logging.info("Преобразование типов данных завершено")

        # Объединение строк с одинаковым transaction_id и user_id
        df = df.groupby(['transaction_id', 'user_id'], as_index=False).agg({
                        'transaction_amount': 'sum',
                        'transaction_date': 'first',
                        'transaction_type': 'first'   
                    })
        logging.info("Данные сгруппированы по 'transaction_id', 'user_id'")
        
        # Регулярные выражение для приведения к общему формату
        df['transaction_type'] = df['transaction_type'].replace({
                            r'^subscription\s*$': 'subscription',  
                            r'^subscriptio$': 'subscription'      
                        }, regex=True)
        logging.info("Столбец transaction_type приведен к общему типу")
        
        # Выбор только транзакций с неотрицательной суммой транзакции
        df = df[df['transaction_amount']>=0]
        logging.info("Выбраны строчки только с неотрицательной суммой транзакции")
        
        # Создание новых полей
        df['year'] = df['transaction_date'].dt.year
        df['month'] = df['transaction_date'].dt.month
        df['is_refund'] = df['transaction_type'].apply(lambda x: True if x.lower() == 'refund' else False)
        logging.info("Созданы новые поля: year, month, is_refund")

        logging.info("Трансформация данных успешно завершена")
        return df
    except Exception as e:
        logging.error(f"Ошибка на этапе трансформации данных: {e}")
        sys.exit(1)

def load(df, db_path='/data/transactions.db'):
    try:
        logging.info("Начало загрузки данных в базу данных SQLite")
        
        # Создание объекта подключения к базе данных
        engine = create_engine(f'sqlite:///{db_path}')
        logging.info("Создан объект подключения к базе данных SQLite")

        # Определение метаданных
        metadata = MetaData()

        # Определение таблицы
        transactions = Table('transactions', metadata,
                             Column('transaction_id', Integer),
                             Column('user_id', Integer, nullable=False),
                             Column('transaction_amount', Float, nullable=False),
                             Column('transaction_date', Date, nullable=False),
                             Column('transaction_type', String, nullable=False),
                             Column('year', Integer),
                             Column('month', Integer),
                             Column('is_refund', Boolean),
                             Index('idx_transaction_id', 'transaction_id'),
                             Index('idx_user_id', 'user_id')
                             )

        # Создание таблицы и индексов
        metadata.create_all(engine)  # Создает таблицы

        # Подключение к базе данных
        conn = engine.connect()
        logging.info("Подключение к базе данных успешно установлено")

        # Загрузка данных
        df.to_sql('transactions', con=engine, if_exists='append', index=False)
        logging.info("Данные успешно загружены в базу данных")

        conn.close()
    except Exception as e:
        logging.error(f"Ошибка на этапе загрузки данных: {e}")
        sys.exit(1)


def query(db_path='/data/transactions.db'):
    try:
        logging.info("Начало аналитического запроса")

        import os
        import pandas as pd

        engine = create_engine(f'sqlite:///{db_path}')
        conn = engine.connect()
        logging.info("Подключение к базе данных успешно установлено")

        with open('scripts/uniq_users_purchases.sql', 'r') as file:
            sql_query = file.read()

        df = pd.read_sql_query(sql_query, conn)
        logging.info("Запрос успешно выполнен")

        output_directory = 'output'
        output_file_path = os.path.join(output_directory, 'uniq_users_purchases.csv')
    
        os.makedirs(output_directory, exist_ok=True)
        logging.info(f"Директория {output_directory} успешно создана")

        # Сохраняем DataFrame в CSV-файл
        df.to_csv(output_file_path, index=False)
        logging.info(f"Результаты запроса успешно сохранены в {output_file_path}")

        conn.close()

    except Exception as e:
        logging.error(f"Ошибка на этапе выполнения аналитического: {e}")
        sys.exit(1)

def main():
    excel_file = '../data/transactions.xlsx'
    
    df = extract(excel_file)
    
    df_transformed = transform(df)
    
    load(df_transformed)

    query()

if __name__ == "__main__":
    main()
