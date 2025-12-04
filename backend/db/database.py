from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import QueuePool
from db.models import Base
import os

DATABASE_URL = "postgresql://postgres:your_password@postgres:5432/satellite_db"

# Настройка движка
engine = create_engine(
    DATABASE_URL,
    poolclass=QueuePool,
    pool_size=10,
    max_overflow=20,
    pool_pre_ping=True,
    echo=False  # Для отладки можно включить True
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def init_db():
    """Создание всех таблиц в базе данных"""
    Base.metadata.create_all(bind=engine)
    print("✓ Таблицы созданы/проверены")

def clear_database():
    """Очистка всех данных из таблиц"""
    session = SessionLocal()
    try:
        # Получаем все таблицы
        for table in reversed(Base.metadata.sorted_tables):
            # Используем text() для явного указания SQL выражения
            session.execute(text(f"TRUNCATE TABLE {table.name} RESTART IDENTITY CASCADE"))
        session.commit()
        print("✓ База данных очищена")
    except Exception as e:
        session.rollback()
        print(f"✗ Ошибка при очистке базы данных: {e}")
        raise e
    finally:
        session.close()
