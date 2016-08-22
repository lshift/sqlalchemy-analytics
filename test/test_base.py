import unittest

from decimal import Decimal
from datetime import datetime
from sqlalchemy import Column, DateTime, Numeric, Float, Integer, Text, Boolean
from sqlalchemy import Table

from main import engine, session, Base


class TestTable(Table):
    """
    Variant of a SQLAlchemy table that can be constructed directly from a list of dicts representing test data.
    Use these to isolate the testing of core expressions by mocking the results of another expression that the
    one under test uses.
    """
    ptype_to_dbtype_map = {
        bool: Boolean,
        int: Integer,
        str: Text,
        float: Float,
        Decimal: Numeric,
        datetime: DateTime,
    }

    def __new__(cls, name, row_values, **kw):
        """
        Create a SQLAlchemy table with rows from a list of dicts. The table schema is inferred
        from the Python types of the dict values.
        :param name: Table name
        :param row_values: List of dicts representing rows
        :param kw:
        :return: SQLAlchemy table
        """
        if name not in Base.metadata.tables:
            columns = [Column(name, TestTable.ptype_to_dbtype_map[type(value)])
                       for name, value in row_values[0].items()]
            table = super().__new__(cls, name, Base.metadata, *tuple(columns))
            Base.metadata.create_all(tables=[table], checkfirst=False)
        else:
            # Same table is being reused in a test
            table = Base.metadata.tables[name]
            engine.execute(table.delete())
        engine.execute(table.insert().values(row_values))
        return table


class TestBase(unittest.TestCase):
    """
    Base class for unit and integration tests.
    """
    def setUp(self, create_all=True):
        """
        (Re)create the test database.
        :param create_all: If true create the tables defined in the model.
        """
        engine.execute("DROP SCHEMA IF EXISTS public CASCADE")
        engine.execute("CREATE SCHEMA public")
        engine.echo = True
        if create_all:
            Base.metadata.create_all(bind=engine)
        self.session = session

    def doCleanups(self):
        if hasattr(self, 'session') and session:
            self.session.close_all()

    def exec(self, exp):
        """
        Execute SQL expression
        :param exp:
        :return: list of dicts representing result rows
        """
        return [dict(result) for result in self.session.bind.engine.execute(exp).fetchall()]
