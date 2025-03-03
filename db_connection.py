import asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

# URL подключения
DATABASE_URL = "mssql+aioodbc://DESKTOP-5TC17S0\\SQLEXPRESS/creditbank?driver=ODBC+Driver+18+for+SQL+Server&trusted_connection=yes&Encrypt=no"

# Создание асинхронного движка
async_engine = create_async_engine(DATABASE_URL, echo=True, future=True)

# Создание асинхронной сессии
async_session = sessionmaker(
    async_engine,
    expire_on_commit=False,  # Чтобы объекты не удалялись после commit()
    class_=AsyncSession,
)

# Функция для получения асинхронной сессии
def get_async_session() -> AsyncSession:
    return async_session()  # Тепер ми просто повертаємо сесію без yield
