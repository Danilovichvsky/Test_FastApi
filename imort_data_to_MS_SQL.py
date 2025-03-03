import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime

# Строка подключения к базе данных
DATABASE_URL = "mssql+pyodbc://DESKTOP-5TC17S0\\SQLEXPRESS/creditbank?driver=ODBC+Driver+18+for+SQL+Server&trusted_connection=yes&Encrypt=no"

# Создаем движок подключения
engine = create_engine(DATABASE_URL)

def load_excel_to_db(excel_file, table_name):
    """
    Метод, який бере ексель файл нового формату .xlsx і додає в БД

    :param excel_file:
    :param table_name:
    :return: data in MS SQL
    """
    try:
        # Чтение данных из Excel
        df = pd.read_excel(excel_file, engine='openpyxl')

        # Выводим данные для проверки
        print(f"Данные из Excel: {df.head()}")

        # Преобразуем столбцы с датами в формат datetime
        for col in df.columns:
            if df[col].dtype == 'object':  # Проверяем, что столбец — строковый
                try:
                    # Преобразуем строки в формат datetime, если это дата
                    df[col] = pd.to_datetime(df[col], format='%d.%m.%Y', errors='ignore')

                    # Извлекаем только дату (без времени)
                    if df[col].dtype == 'datetime64[ns]':
                        df[col] = df[col].dt.date
                except Exception as e:
                    pass  # Если не удается преобразовать, пропускаем

        # Запись данных в базу данных
        df.to_sql(table_name, con=engine, if_exists='replace', index=False)
        print(f"Данные успешно загружены в таблицу '{table_name}'")

    except Exception as e:
        print(f"Ошибка при загрузке данных: {e}")

# Пример использования
load_excel_to_db('dataset/credits.xlsx', 'credits')
