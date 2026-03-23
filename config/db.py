"""
config/db.py
────────────
Database connection layer (SQLAlchemy 2.x).

ALL SQLAlchemy imports are confined to this single file.
The analytics scripts import only pandas/numpy; they call helpers
here only when a real DB session is required (i.e. at runtime, not
at import time).  This keeps every pure analytics function testable
without SQLAlchemy being installed.

ORM Models mirror the .NET backend schema:
  Products   →  Product
  Orders     →  Order
  OrderItems →  OrderItem
"""

import os
from datetime import datetime

from dotenv import load_dotenv

# ── SQLAlchemy ──────────────────────────────────────────────────────────────
from sqlalchemy import (
    Column, DateTime, Float, ForeignKey,
    Integer, String, create_engine,
)
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import declarative_base, relationship, sessionmaker

load_dotenv()

Base = declarative_base()


# ── ORM Models ───────────────────────────────────────────────────────────────

class Product(Base):
    __tablename__ = "Products"

    id             = Column(Integer, primary_key=True)
    name           = Column(String(200), nullable=False)
    sku            = Column(String(100), unique=True, nullable=False)
    stock_quantity = Column(Integer, default=0)
    price          = Column(Float, nullable=False)
    created_at     = Column(DateTime, default=datetime.utcnow)

    order_items = relationship("OrderItem", back_populates="product")

    def __repr__(self):
        return f"<Product id={self.id} sku='{self.sku}' stock={self.stock_quantity}>"


class Order(Base):
    __tablename__ = "Orders"

    id           = Column(Integer, primary_key=True)
    order_date   = Column(DateTime, default=datetime.utcnow, nullable=False)
    total_amount = Column(Float, nullable=False)
    status       = Column(String(50), default="Pending")
    customer_name = Column(String(200))

    items = relationship("OrderItem", back_populates="order")

    def __repr__(self):
        return f"<Order id={self.id} date={self.order_date} total={self.total_amount}>"


class OrderItem(Base):
    __tablename__ = "OrderItems"

    id         = Column(Integer, primary_key=True)
    order_id   = Column(Integer, ForeignKey("Orders.id"), nullable=False)
    product_id = Column(Integer, ForeignKey("Products.id"), nullable=False)
    quantity   = Column(Integer, nullable=False)
    unit_price = Column(Float, nullable=False)

    order   = relationship("Order",   back_populates="items")
    product = relationship("Product", back_populates="order_items")

    def __repr__(self):
        return f"<OrderItem order={self.order_id} product={self.product_id} qty={self.quantity}>"


# ── Engine / Session factory ──────────────────────────────────────────────────

def _build_conn_str() -> str:
    """Assemble MSSQL connection string from env vars — never hardcoded."""
    server   = os.getenv("DB_SERVER",   "localhost")
    port     = os.getenv("DB_PORT",     "1433")
    db       = os.getenv("DB_NAME",     "RetailDB")
    user     = os.getenv("DB_USER",     "sa")
    password = os.getenv("DB_PASSWORD", "")
    return (
        f"mssql+pyodbc://{user}:{password}@{server}:{port}/{db}"
        "?driver=ODBC+Driver+17+for+SQL+Server"
    )


def create_db_engine(connection_string: str | None = None, echo: bool = False):
    """
    Build and verify a SQLAlchemy engine.

    Args:
        connection_string: Override the default env-var-based string.
        echo:              Log all SQL statements (useful for debugging).

    Raises:
        RuntimeError: If the database cannot be reached.
    """
    conn_str = connection_string or _build_conn_str()
    try:
        engine = create_engine(conn_str, echo=echo, pool_pre_ping=True)
        with engine.connect():          # quick connectivity check
            pass
        return engine
    except SQLAlchemyError as exc:
        raise RuntimeError(f"Database connection failed: {exc}") from exc


def get_session_factory(engine):
    """Return a Session class bound to *engine*."""
    return sessionmaker(bind=engine, autoflush=False, autocommit=False)
