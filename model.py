"""
Generate pivot with price categories

Pivot: creating cols based on row values
"""

from sqlalchemy import BigInteger, Column, ForeignKey, Integer, Text, DateTime, func
from sqlalchemy.orm import relationship
from db import Base


class Author(Base):
    """
    """
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    books = relationship('Book')
    name = Column(Text, nullable=False)


class Book(Base):
    """
    """
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    author = relationship('Author')
    author_id = Column(BigInteger, ForeignKey('author.id'), nullable=True)
    genre = relationship('Genre')
    genre_id = Column(BigInteger, ForeignKey('genre.id'), nullable=True)
    price = Column(Integer, nullable=True)
    sales = relationship('BookSale', uselist=True)
    title = Column(Text, nullable=False)


class BookSale(Base):
    """

    """
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    book_id = Column(BigInteger, ForeignKey('book.id'), nullable=False)
    book = relationship('Book')
    transaction = relationship('Transaction')
    transaction_id = Column(BigInteger, ForeignKey('transaction.id'), nullable=False)


class Genre(Base):
    """
    """
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    books = relationship('Book')
    name = Column(Text, nullable=False)


class Transaction(Base):
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    book_sales = relationship('BookSale')
    create_date = Column(DateTime, default=func.now(), nullable=False)
