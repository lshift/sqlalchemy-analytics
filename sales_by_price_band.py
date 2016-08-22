from sqlalchemy import func, bindparam
from sqlalchemy.orm import relationship
from sqlalchemy.sql import select
from sqlalchemy.sql.expression import between

import model as m
from main import session


def get_sales_for_period_te(booksales_te, books_te, genres_te, transactions_te):
    return select([
        booksales_te.c.book_id,
        books_te.c.price,
        books_te.c.genre_id,
        transactions_te.c.id.label('transaction_id'),
        transactions_te.c.create_date]) \
        .select_from(
        booksales_te
            .join(books_te)
            .join(genres_te)
            .join(transactions_te)
    ) \
        .where(between(transactions_te.c.create_date, bindparam('start_date'), bindparam('end_date')))


def get_max_price_te(sales_for_period_te):
    return select([
        func.max(sales_for_period_te.c.price).label('max_price')
    ])


def get_increment_te(max_price_te, rounding_unit, max_num_bands):
    return select([
        func.greatest(
            (max_price_te.c.max_price + (rounding_unit * max_num_bands - 1)) / rounding_unit / max_num_bands
            * rounding_unit,
            rounding_unit).label('incr')
    ])


def get_price_bands_te(max_price_te, increment_te, max_num_bands):
    series_te = session.query(func.generate_series(
        1,
        func.least((max_price_te.c.max_price + increment_te.c.incr - 1)
                   / increment_te.c.incr, max_num_bands)).label('num')).subquery('series')

    return select([
        series_te.c.num,
        ((series_te.c.num - 1) * increment_te.c.incr + 1).label('from_price'),
        (series_te.c.num * increment_te.c.incr).label('to_price')
    ])


def get_sales_in_bands_te(sales_for_period_te, price_bands_te):
    return select([
        sales_for_period_te,
        price_bands_te.c.num.label('price_band'),
        price_bands_te.c.from_price,
        price_bands_te.c.to_price,
    ]) \
        .select_from(
        sales_for_period_te
            .join(price_bands_te,
                  between(sales_for_period_te.c.price, price_bands_te.c.from_price, price_bands_te.c.to_price)))


def get_total_sales_by_band_and_genre_te(sales_in_bands_te):
    return select([
        sales_in_bands_te.c.price_band,
        sales_in_bands_te.c.from_price,
        sales_in_bands_te.c.to_price,
        sales_in_bands_te.c.genre_id,
        func.sum(sales_in_bands_te.c.price).label('total_value'),
        func.count().label('total_volume'),
    ]) \
        .group_by(
            sales_in_bands_te.c.price_band,
            sales_in_bands_te.c.from_price,
            sales_in_bands_te.c.to_price,
            sales_in_bands_te.c.genre_id,
    )


class SalesByPriceBandAndGenre(m.Base):

    def mapped_te():
        sales_for_period_cte = get_sales_for_period_te(
            m.BookSale.__table__,
            m.Book.__table__,
            m.Genre.__table__,
            m.Transaction.__table__).cte('sales')

        max_price_cte = get_max_price_te(
            sales_for_period_cte) \
            .cte('max_price')

        increment_cte = get_increment_te(
            max_price_cte,
            rounding_unit=50,
            max_num_bands=5) \
            .cte('incr')

        price_bands_te = get_price_bands_te(
            max_price_cte,
            increment_cte,
            max_num_bands=5) \
            .cte('price_bands')

        sales_in_bands_cte = get_sales_in_bands_te(
            sales_for_period_cte,
            price_bands_te).cte('sales_in_bands')

        return get_total_sales_by_band_and_genre_te(
            sales_in_bands_cte)

    __table__ = mapped_te().alias('total_sales')

    # Create a primary key for anything needing object identity, e.g. Marshmallow
    __mapper_args__ = {
        'primary_key': [__table__.c.price_band, __table__.c.genre_id]
    }

    genre = relationship(m.Genre, primaryjoin="SalesByPriceBandAndGenre.genre_id == Genre.id", viewonly=True)


def get_sales_by_price_band_for_period(start_date, end_date):
    return session.query(SalesByPriceBandAndGenre).params(locals().copy()) \
        .order_by(SalesByPriceBandAndGenre.price_band, SalesByPriceBandAndGenre.genre_id)\
        .all()
