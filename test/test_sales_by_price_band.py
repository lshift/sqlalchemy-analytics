"""
Unit and integration tests
"""

import random
from datetime import datetime

import test.factories as f
from db import session
import model as m
from sales_by_price_band import get_increment_te, get_price_bands_te, SalesByPriceBandAndGenre
from test.test_base import TestBase, TestTable
from sqlalchemy import and_


class TableExpressionsTestCase(TestBase):
    def setUp(self, create_all=None):
        super().setUp(create_all=False)  # Don't need the model tables as inputs/outputs are table expressions

    def test_increment_is_correct_value(self):
        for fixture in [
            {'max_price_in': {'max_price': 1}, 'result': [{'increment': 50}]},
            {'max_price_in': {'max_price': 50}, 'result': [{'increment': 50}]},
            {'max_price_in': {'max_price': 99}, 'result': [{'increment': 50}]},
            {'max_price_in': {'max_price': 499}, 'result': [{'increment': 100}]},
            {'max_price_in': {'max_price': 500}, 'result': [{'increment': 100}]},
            {'max_price_in': {'max_price': 501}, 'result': [{'increment': 150}]},
            {'max_price_in': {'max_price': 1001}, 'result': [{'increment': 250}]},
            {'max_price_in': {'max_price': 10001}, 'result': [{'increment': 2050}]},
        ]:
            results = self.exec(get_increment_te(
                TestTable('max_price', [fixture['max_price_in']]),
                50,
                5))
            self.assertEqual(results, fixture['result'])

    def test_price_bands_are_correct(self):
        for fixture in [
            {'max_price_in': {'max_price': 1},
             'increment_in': {'increment': 50},
             'max_num_bands_in': 1,
             'result': [
                 {'num': 1, 'from_price': 1, 'to_price': 50}
             ]},
            {'max_price_in': {'max_price': 1},
             'increment_in': {'increment': 50},
             'max_num_bands_in': 99,
             'result': [
                 {'num': 1, 'from_price': 1, 'to_price': 50}
             ]},
            {'max_price_in': {'max_price': 50},
             'increment_in': {'increment': 50},
             'max_num_bands_in': 99,
             'result': [
                 {'num': 1, 'from_price': 1, 'to_price': 50}
             ]},
            {'max_price_in': {'max_price': 99},
             'increment_in': {'increment': 50},
             'max_num_bands_in': 99,
             'result': [
                 {'num': 1, 'from_price': 1, 'to_price': 50},
                 {'num': 2, 'from_price': 51, 'to_price': 100}
             ]},
            {'max_price_in': {'max_price': 499},
             'increment_in': {'increment': 100},
             'max_num_bands_in': 99,
             'result': [
                 {'num': 1, 'from_price': 1, 'to_price': 100},
                 {'num': 2, 'from_price': 101, 'to_price': 200},
                 {'num': 3, 'from_price': 201, 'to_price': 300},
                 {'num': 4, 'from_price': 301, 'to_price': 400},
                 {'num': 5, 'from_price': 401, 'to_price': 500},
             ]},
            {'max_price_in': {'max_price': 500},
             'increment_in': {'increment': 100},
             'max_num_bands_in': 99,
             'result': [
                 {'num': 1, 'from_price': 1, 'to_price': 100},
                 {'num': 2, 'from_price': 101, 'to_price': 200},
                 {'num': 3, 'from_price': 201, 'to_price': 300},
                 {'num': 4, 'from_price': 301, 'to_price': 400},
                 {'num': 5, 'from_price': 401, 'to_price': 500},
             ]},
            {'max_price_in': {'max_price': 501},
             'increment_in': {'increment': 150},
             'max_num_bands_in': 99,
             'result': [
                 {'num': 1, 'from_price': 1, 'to_price': 150},
                 {'num': 2, 'from_price': 151, 'to_price': 300},
                 {'num': 3, 'from_price': 301, 'to_price': 450},
                 {'num': 4, 'from_price': 451, 'to_price': 600},
             ]},
        ]:
            results = self.exec(get_price_bands_te(
                TestTable('max_price', [fixture['max_price_in']]),
                TestTable('incr', [fixture['increment_in']]),
                fixture['max_num_bands_in']
            ))
            self.assertEqual(results, fixture['result'])


class IntegrationTestCase(TestBase):
    def setUp(self, create_all=None):
        super().setUp(create_all=True)  # Need the model tables here

    def test_simple_orm_query(self):
        random.seed(1)  # Pseudo-random to make tests repeatable
        start_date = datetime(2016, 1, 1)
        end_date = datetime(2016, 12, 31)
        f.create_dummy_sales(start_date, end_date)

        january_art_book_sales_query = session.query(m.Genre) \
            .join(m.Book) \
            .join(m.BookSale) \
            .join(m.Transaction) \
            .filter(m.Genre.name == 'Art') \
            .filter(and_(m.Transaction.create_date >= start_date, m.Transaction.create_date < datetime(2016, 2, 1)))

        january_art_book_sales = january_art_book_sales_query.all()

        books = january_art_book_sales[0].books

        self.assertTrue(all(book.genre.name == 'Art' for book in books))
        self.assertTrue(all((sale.transaction.create_date.month == 1
                            for sale in book.sales)
                            for book in books))

    def test_sales_by_price_band_returns_bands_and_genres(self):
            random.seed(1)  # Pseudo-random to make tests repeatable
            start_date = datetime(2016, 1, 1)
            end_date = datetime(2016, 12, 31)
            f.create_dummy_sales(start_date, end_date)

            sales = session.query(SalesByPriceBandAndGenre).params(start_date=start_date, end_date=end_date) \
                .order_by(SalesByPriceBandAndGenre.price_band, SalesByPriceBandAndGenre.genre_id) \
                .all()

            self.assertEquals(sales[0].price_band, 1)
            self.assertEquals(sales[0].from_price, 1)
            self.assertIsNotNone(next((sale for sale in sales if sale.price_band == 2), None))

            self.assertIsNotNone(next((sale for sale in sales if sale.genre.name == 'Art'), None))
            self.assertIsNotNone(next((sale for sale in sales if sale.genre.name == 'Drama'), None))
