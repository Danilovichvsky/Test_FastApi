import uvicorn
from fastapi import FastAPI, Depends, HTTPException, UploadFile, File
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from db_connection import get_async_session
from datetime import datetime
import pandas as pd

# Ініціалізація FastAPI додатку
app = FastAPI()


class CreditInfo(BaseModel):
    issuance_date: str
    is_closed: bool
    return_date: str = None
    actual_return_date: str = None
    body: float
    percent: float
    total_payments: float = None
    due_date: str = None
    overdue_days: int = None
    total_body_payments: float = None
    total_interest_payments: float = None


@app.get("/user_credits/{user_id}", response_model=list[CreditInfo],
         tags=["Інформація про кредит"])
async def get_user_credits_info(user_id: int):
    async with get_async_session() as session:  # Використовуємо асинхронний контекст
        result = await session.execute(
            text("SELECT * FROM credits WHERE user_id = :user_id"), {"user_id": user_id}
        )
        credits = result.fetchall()

        if not credits:
            raise HTTPException(status_code=404, detail="User not found")

        credit_info = []
        for credit in credits:
            credit_data = {
                "issuance_date": credit.issuance_date.isoformat(),
                "is_closed": credit.actual_return_date is not None
            }

            # Отримуємо інформацію про платежі
            payments = await session.execute(
                text("SELECT * FROM payments WHERE credit_id = :credit_id"), {"credit_id": credit.id}
            )
            payments = payments.fetchall()

            if credit_data["is_closed"]:
                # Закритий кредит
                total_payments = sum(payment.sum for payment in payments)
                credit_data.update({
                    "return_date": credit.return_date.isoformat(),
                    "actual_return_date": credit.actual_return_date.isoformat(),
                    "body": credit.body,
                    "percent": credit.percent,
                    "total_payments": total_payments
                })
            else:
                # Відкритий кредит
                overdue_days = (
                        datetime.now().date() - credit.return_date).days if datetime.now().date() > credit.return_date else 0
                total_body_payments = sum(payment.sum for payment in payments if payment.sum > 0)  # Платежі по тілу
                total_interest_payments = sum(
                    payment.sum for payment in payments if payment.sum < 0)  # Платежі по відсоткам

                credit_data.update({
                    "due_date": credit.return_date.isoformat(),
                    "overdue_days": overdue_days,
                    "body": credit.body,
                    "percent": credit.percent,
                    "total_body_payments": total_body_payments,
                    "total_interest_payments": total_interest_payments
                })

            credit_info.append(credit_data)

        return credit_info


@app.post("/plans_insert",tags=["Додавання плану"])
async def insert_plans(file: UploadFile = File(...), db: AsyncSession = Depends(get_async_session)):
    # Зчитуємо файл Excel
    try:
        df = pd.read_excel(file.file)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Не вдалося прочитати файл Excel: {str(e)}")

    # Перевіряємо наявність необхідних колонок у файлі
    required_columns = ['period', 'category_id', 'sum']
    if not all(col in df.columns for col in required_columns):
        raise HTTPException(status_code=400, detail="Файл має містити стовпці: 'period', 'category_id', 'sum'")

    # Перевірка на правильність формату місяця (period)
    df['period'] = pd.to_datetime(df['period'], errors='coerce', format='%d.%m.%Y')
    if df['period'].isnull().any():
        raise HTTPException(status_code=400,
                            detail="Невірний формат місяця. Має бути перше число місяця у форматі dd.mm.yyyy.")

    # Перевірка на порожні значення у стовпці sum
    if df['sum'].isnull().any():
        raise HTTPException(status_code=400, detail="Стовпець 'sum' не може містити порожніх значень.")

    # Перевірка на наявність плану в базі даних
    for index, row in df.iterrows():
        period = row['period'].strftime('%Y-%m-%d')  # Перетворюємо на рік-місяць-день
        category_id = row['category_id']

        # Перевіряємо, чи існує вже план для цього місяця та категорії
        result = await db.execute(
            text("""
                SELECT * FROM plans p
                WHERE p.period = :period AND p.category_id = :category_id
            """), {"period": period, "category_id": category_id}
        )
        existing_plan = result.fetchone()

        if existing_plan:
            raise HTTPException(status_code=400,
                                detail=f"План для місяця {period} та категорії з ID {category_id} вже існує.")

    # Якщо всі перевірки пройдено, додаємо нові плани
    for index, row in df.iterrows():
        period = row['period'].strftime('%Y-%m-%d')  # Перетворюємо на рік-місяць-день
        category_id = row['category_id']
        amount = row['sum']

        # Вставка нового плану в базу даних
        await db.execute(
            text("""
                INSERT INTO plans (period, sum, category_id)
                VALUES (:period, :sum, :category_id)
            """), {"period": period, "sum": amount, "category_id": category_id}
        )

    # Підтверджуємо транзакцію
    await db.commit()

    return {"message": "Плани успішно завантажено та внесено в базу даних."}


if __name__ == '__main__':
    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)
