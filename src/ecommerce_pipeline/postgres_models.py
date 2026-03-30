"""
SQLAlchemy ORM models.

Define your database tables here using the SQLAlchemy 2.0 declarative API.
Every class you define here that inherits from Base will become a table
when `Base.metadata.create_all(engine)` is called at startup.

Useful imports are already provided below. Add more as needed.

Documentation:
    https://docs.sqlalchemy.org/en/20/orm/declarative_tables.html
"""

from sqlalchemy import CheckConstraint, DateTime, ForeignKey, Integer, Numeric, String, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from sqlalchemy.sql import func

from datetime import datetime
from decimal import Decimal
from typing import List, Optional

class Base(DeclarativeBase):
    pass

class Customer(Base):
    __tablename__ = "customers"

    customer_id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    email: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())

    # קשר להזמנות
    orders: Mapped[List["Order"]] = relationship(back_populates="customer")

class Order(Base):
    __tablename__ = "orders"

    order_id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    customer_id: Mapped[int] = mapped_column(ForeignKey("customers.customer_id"), nullable=False)
    # total_amount חייב להיות כאן בשביל שאילתות סכימה מהירות ב-SQL
    total_amount: Mapped[Decimal] = mapped_column(Numeric(10, 2), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
    customer: Mapped["Customer"] = relationship(back_populates="orders")