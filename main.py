from datetime import datetime
from db import engine, session, Base

if __name__ == '__main__':
    from sales_by_price_band import SalesByPriceBandAndGenre
    import test.factories as f
    engine.execute("DROP SCHEMA IF EXISTS public CASCADE")
    engine.execute("CREATE SCHEMA public")
    Base.metadata.create_all(bind=engine)
    start_date = datetime(2016, 1, 1)
    end_date = datetime(2016, 12, 31)
    f.create_dummy_sales(start_date, end_date)
    sales = session.query(SalesByPriceBandAndGenre).params(start_date=start_date, end_date=end_date) \
        .order_by(SalesByPriceBandAndGenre.price_band, SalesByPriceBandAndGenre.genre_id) \
        .all()
    print(flush=True)
    last_band = None
    for s in sales:
        if s.price_band != last_band:
            last_band = s.price_band
            print('\n\nBand {price_band} - books from {from_price}p to {to_price}p'.format(
                price_band=s.price_band,
                from_price=s.from_price,
                to_price=s.to_price,
            ))
        print('\n{genre}:\n  Value: {total_value}p\n  Volume: {total_volume}'.format(
            genre=s.genre.name,
            total_value=s.total_value,
            total_volume=s.total_volume,
        ))
    print(flush=True)
