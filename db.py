from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base, declared_attr


class DefaultTablenameMixin(object):
    @declared_attr
    def __tablename__(cls):
        return cls.__name__.lower()

engine = create_engine('postgresql://postgres:password@localhost:5432/modular_analytics')
session = sessionmaker(bind=engine)()
Base = declarative_base(bind=engine, cls=DefaultTablenameMixin)

