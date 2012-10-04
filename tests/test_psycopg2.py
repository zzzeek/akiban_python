import unittest
from nose import SkipTest
from . import fixtures, fails
import akiban
import datetime

try:
    import psycopg2
    import psycopg2.extensions
except ImportError:
    raise SkipTest("psycopg2 is not installed")

class Psycopg2Test(unittest.TestCase):

    @classmethod
    def setup_class(cls):
        from akiban.psycopg2 import Connection

        cls.connection = psycopg2.connect(port=15432, host="localhost",
                             connection_factory=Connection)
        fixtures._table_fixture(cls.connection)
        fixtures._data_fixture(cls.connection)

    @classmethod
    def teardown_class(cls):
        fixtures._drop_tables(cls.connection)
        cls.connection.close()

    def test_basic_description(self):
        cursor = self.connection.cursor()
        cursor.execute(
                "select customer_id, name from customers where customer_id=5")
        self.assertEquals(
            cursor.description,
            [
                ('customer_id', psycopg2.extensions.INTEGER, None, None,
                        None, None, None),
                ('name', psycopg2.STRING, None, None, None, None, None)
            ]
        )

    def test_typecodes_hashable(self):
        cursor = self.connection.cursor()
        cursor.execute(
                "select customer_id, name from customers where customer_id=5")
        self.assertEquals(
            set([cursor.description[1][1]]),
            set([1043])
        )

    def test_nested_description_toplevel(self):
        cursor = self.connection.cursor()
        cursor.execute(
                "select customer_id, name, "
                "(select order_id from orders where orders.customer_id=5) "
                "as orders from customers where customer_id=5")
        self.assertEquals(
            cursor.description,
            [
                ('customer_id', psycopg2.extensions.INTEGER, None, None,
                        None, None, None),
                ('name', psycopg2.STRING, None, None, None, None, None),
                ('orders', akiban.NESTED_CURSOR, None, None, None, None, None),
            ]
        )

    def _test_nested_fetch(self, getrows, padding=[]):
        cursor = self.connection.cursor()
        cursor.execute(
                "select "
                "(select order_id, order_info, order_date from orders "
                    "where customer_id=customers.customer_id) from customers "
                " where customer_id in (5, 6)"
                "order by customer_id")
        row = cursor.fetchone()
        nested_cursor = row[0]

        self.assertEquals(getrows(nested_cursor),
         [
            (113, 'some order info', datetime.datetime(2012, 9, 5, 17, 24, 12)),
            (114, 'some order info', datetime.datetime(2012, 9, 5, 17, 24, 12)),
            (115, 'some order info', datetime.datetime(2012, 9, 5, 17, 24, 12)),
        ] + padding
        )
        row = cursor.fetchone()
        nested_cursor = row[0]

        self.assertEquals(getrows(nested_cursor),
         [
            (116, 'some order info',
                    datetime.datetime(2012, 9, 5, 17, 24, 12)),
            (117, 'some order info',
                    datetime.datetime(2012, 9, 5, 17, 24, 12)),
            (118, 'some order info',
                    datetime.datetime(2012, 9, 5, 17, 24, 12)),
          ] + padding
        )

    def test_fetchone_nested(self):
        self._test_nested_fetch(
                lambda cursor: [cursor.fetchone()
                                for i in xrange(5)], [None, None])

    def test_fetchmany_nested(self):
        self._test_nested_fetch(lambda cursor: cursor.fetchmany(5))

    def test_fetchall_nested(self):
        self._test_nested_fetch(lambda cursor: cursor.fetchall())

    def test_none_cursor(self):
        cursor = self.connection.cursor()
        cursor.execute("insert into items VALUES (%s, %s, %s, %s)",
            (1012, 101, 9.99, 1)
            )
        self.assertEquals(cursor.description, None)
        self.assertEquals(cursor.akiban_description, None)

    def test_akiban_nested_description(self):
        cursor = self.connection.cursor()
        cursor.execute(
                "select customer_id, "
                "(select order_id, (select item_id from items where "
                    "order_id=orders.order_id) as i1 from orders "
                    "where customer_id=customers.customer_id) as o1 "
                "from customers "
                "where customer_id in (1, 2, 3)"
                "order by customer_id")
        self.assertEquals(
            cursor.akiban_description,
            [
                (u'customer_id',
                    psycopg2.extensions.INTEGER,
                    None, None, None, None, None, None),
                (u'o1',
                    akiban.NESTED_CURSOR,
                    None, None, None, None, None,
                    [
                        (u'order_id', psycopg2.extensions.INTEGER,
                        None, None, None, None, None, None),
                        (u'i1', akiban.NESTED_CURSOR,
                            None, None, None, None, None,
                            [(u'item_id', psycopg2.extensions.INTEGER,
                                None, None, None, None, None, None)
                            ]
                        )
                    ]
                )
            ]
        )
    def test_multiple_nest(self):
        cursor = self.connection.cursor()
        cursor.execute(
                "select customer_id, "
                "(select order_id, (select item_id from items where "
                    "order_id=orders.order_id) from orders "
                    "where customer_id=customers.customer_id) from customers "
                " where customer_id in (1, 2, 3)"
                "order by customer_id")

        def expand_row(row):
            return [
                expand_rows(col) for col in row
            ]
        def expand_rows(result):
            if hasattr(result, 'fetchall'):
                return [expand_row(elem) for elem in result.fetchall()]
            else:
                return result
        expanded = expand_rows(cursor)
        self.assertEquals(expanded,
            [
                [1,
                    [
                        [101, [[1001], [1002]]],
                        [102, [[1003]]],
                        [103, [[1004]]]
                    ]
                ],
                [2,
                    [
                        [104, [[1005]]],
                        [105, [[1006]]],
                        [106, [[1007]]]
                    ]
                ],
                [3,
                    [
                        [107, [[1008], [1009]]],
                        [108, [[1010]]],
                        [109, [[1011]]]
                    ]
                ]
            ]
        )

    def test_typecast_date_outer(self):
        cursor = self.connection.cursor()
        cursor.execute(
                "select birthdate from customers where "
                "customer_id=5")
        self.assertEquals(cursor.fetchone()[0], datetime.date(1982, 7, 16))

    def test_typecast_date_inner(self):
        cursor = self.connection.cursor()
        cursor.execute(
                'select (select order_date from orders where '
                    'customer_id=5) as orders from customers')
        self.assertEquals(cursor.fetchone()[0].fetchone()[0],
                    datetime.datetime(2012, 9, 5, 17, 24, 12))

    @fails("akiban doesn't support BOOL typecodes yet?")
    def test_typecast_boolean_outer(self):
        cursor = self.connection.cursor()
        cursor.execute(
                "select some_bool from customers where "
                "customer_id=5")
        self.assertEquals(cursor.fetchone()[0], True)

    def test_fetchone_plain(self):
        cursor = self.connection.cursor()
        cursor.execute(
                "select customer_id, name from customers where "
                "customer_id in (5, 6, 7) order by customer_id")
        self.assertEquals([
                cursor.fetchone() for i in xrange(5)
            ],
            [(5, 'Peter Beaman'), (6, u'Thomas Jones-Low'), (7, u'Mike McMahon'),
                None, None]
        )

    def test_fetchall_plain(self):
        cursor = self.connection.cursor()
        cursor.execute(
                "select customer_id, name from customers where "
                "customer_id in (5, 6, 7) order by customer_id")
        self.assertEquals(cursor.fetchall(),
            [(5, 'Peter Beaman'), (6, u'Thomas Jones-Low'), (7, u'Mike McMahon')]
        )

    def test_fetchmany_plain(self):
        cursor = self.connection.cursor()
        cursor.execute(
                "select customer_id, name from customers where "
                "customer_id in (5, 6, 7) order by customer_id")
        self.assertEquals(cursor.fetchmany(2),
            [(5, 'Peter Beaman'), (6, u'Thomas Jones-Low')]
        )
        self.assertEquals(cursor.fetchmany(2),
            [(7, u'Mike McMahon')]
            )

        self.assertEquals(cursor.fetchmany(2), [])

    def test_non_nested_disables_json(self):
        cursor = self.connection.cursor(nested=False)
        self.assertRaises(
            psycopg2.ProgrammingError,
            cursor.execute,
            "select customer_id, (select order_id from orders) from customers"
        )

    def test_non_nested_returns_rows(self):
        cursor = self.connection.cursor(nested=False)
        cursor.execute("select customer_id, name from customers "
                            "where customer_id in (5, 6, 7)")
        self.assertEquals(
            cursor.fetchall(),
            [(5, 'Peter Beaman'), (6, 'Thomas Jones-Low'), (7, 'Mike McMahon')]
        )
