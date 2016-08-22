"""
Factory classes and functions for creating test data.
"""

import itertools
import random
from datetime import datetime

import factory
from factory import Sequence
from factory.alchemy import SQLAlchemyModelFactory

import model as m
from main import session
from test.test_base import TestBase


def sub_factory(factory_name):
    return factory.SubFactory('test.factories.' + factory_name)


class BaseFactory(SQLAlchemyModelFactory, TestBase):
    class Meta:
        abstract = True
        sqlalchemy_session = session

    id = Sequence(lambda n: n)


class Author(BaseFactory):
    class Meta:
        model = m.Author

    name = Sequence(lambda n: 'Author {}'.format(n))


class Book(BaseFactory):
    class Meta:
        model = m.Book

    author = sub_factory('Author')
    genre = sub_factory('Genre')
    price = Sequence(lambda n: n * 50)
    title = Sequence(lambda n: 'Title {}'.format(n))


class BookSale(BaseFactory):
    class Meta:
        model = m.BookSale

    book = sub_factory('Book')
    transaction = sub_factory('Transaction')


class Genre(BaseFactory):
    class Meta:
        model = m.Genre

    name = Sequence(lambda n: 'Genre {}'.format(n))


class Transaction(BaseFactory):
    class Meta:
        model = m.Transaction


# noinspection PyArgumentList
def create_dummy_sales(start_date, end_date):
    genres = [
        Genre(name=name) for name in [
            "Action and Adventure",
            "Art",
            "Biography",
            "Children's",
            "Comics",
            "Cookery",
            "Reference",
            "Drama",
            "History",
            "Travel",
        ]
        ]
    [
        Book(
            title='Book {i} of {genre}'.format(i=i, genre=genre.name),
            genre=genre,
            price=((genres.index(genre) + 1) + i) * 50
        ) for i, genre in itertools.product(range(1, 12 + 1), genres)
        ]
    session.flush()
    [
        Transaction(
            book_sales=[
                BookSale(
                    book=genres[int(random.betavariate(2, 2) * len(genres))].books[
                        int(random.betavariate(2, 2) * 12)])
                for _ in range(1, (int(random.triangular(0, 1, 0) * 6) + 1) + 1)],
            create_date=datetime.fromtimestamp(random.randint(start_date.timestamp(), end_date.timestamp()))
        )
        for _ in range(1, 100 + 1)
        ]
    session.commit()
