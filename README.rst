Akiban for Python provides a DBAPI compatibility layer for
`Akiban Server <http://www.akiban.com/>`_.

Akiban Server is a new database engine that is similar in many ways to
well known engines like Postgresql and MySQL.   However, it introduces
some new twists on SQL, including the ability to render "nested" result
sets using plain SQL.

Akiban Server uses a database protocol that is compatible with
Postgresql.   Any `DBAPI <http://www.python.org/dev/peps/pep-0249/>`_
written for Postgresql can also work with Akiban
Server directly.  What Akiban for Python provides is a wrapper around
these DBAPIs so that Akiban's "nested" result system can be used
transparently, meaning any result row can contain columns which themselves
contain "sub-cursors".

So far, Akiban for Python implements one extension module for
the `psycopg2 <http://pypi.python.org/pypi/psycopg2/>`_ DBAPI for Postgresql.
Psycopg2 is the most widely used DBAPI for Postgresql, is extremely
performant and stable and supports Python 3.

Usage of Akiban for Python is extremely simple.   When using psycopg2,
the plugin is enabled as a **connection factory** for psycopg2::

  >>> from akiban.psycopg2 import Connection
  >>> import psycopg2

  >>> connection = psycopg2.connect(host="localhost", port=15432,
  ...                  connection_factory=Connection)

The connection above is in every way an ordinary psycopg2 connection object.
It's special behavior becomes apparent when using Akiban's **nested result set**
capability::

  >>> cursor = connection.cursor()
  >>> cursor.execute("""
  ...       select customers.customer_id, customers.name,
  ...            (select orders.order_id, orders.order_info,
  ...                 (select items.item_id, items.price, items.quantity
  ...                 from items
  ...                 where items.order_id = orders.order_id and
  ...                 orders.customer_id = customers.customer_id) as items
  ...             from orders
  ...             where orders.customer_id = customers.customer_id) as orders
  ...       from customers
  ...     """)

Above, we've selected from a table ``customers``, including a nested
result set for ``orders``.  Within that of ``orders``, we have another
nested result against ``items``. Inspecting ``cursor.description``, we
see the three outermost columns represented, all normally except for
``orders`` which has a special typecode "5001", which will compare as
``True`` to the ``akiban.api.NESTED_CURSOR`` DBAPI constant::

  >>> cursor.description
  [(u'customer_id', 23, None, None, None, None, None), (u'name', 1043, None, None, None, None, None), (u'orders', 5001, None, None, None, None, None)]

If we fetch the first row, it looks mostly normal except for one column that contains a "nested cursor"::

  >>> row = cursor.fetchone()
  >>> row
  (1, 'David McFarlane', <akiban.api.NestedCursor object at 0x10068e050>)

looking at the ``orders`` column, we can see that the value is itself a cursor, with its own ``.description``::

  >>> subcursor = row[2]
  >>> subcursor.description
  [(u'order_id', 23, None, None, None, None, None), (u'order_info', 1043, None, None, None, None, None), (u'items', 5001, None, None, None, None, None)]

Fetching a row from this cursor, we see it has its own nested data::

  >>> subrow = subcursor.fetchone()
  >>> subrow
  (101, 'apple related', <akiban.api.NestedCursor object at 0x10068e0d0>)

and continuing the process, we can see ``items`` column of this row contains another nested cursor::

  >>> subsubcursor = subrow[2]
  >>> subsubcursor.description
  [(u'item_id', 23, None, None, None, None, None), (u'price', 1700, None, None, None, None, None), (u'quantity', 23, None, None, None, None, None)]

We can also access all levels of ".description" in one step from the
lead result, using the extension ".akiban_description".  This is
basically the same structure as that of ``cursor.description``, except
it produces 8-tuples, instead of 7-tuples.  The eighth member of the
tuple contains the sub-description, if any::

  >>> cursor.akiban_description
  [(u'customer_id', 23, None, None, None, None, None, None), (u'name', 1043, None, None, None, None, None, None), (u'orders', 5001, None, None, None, None, None, [(u'order_id', 23, None, None, None, None, None, None), (u'order_info', 1043, None, None, None, None, None, None), (u'items', 5001, None, None, None, None, None, [(u'item_id', 23, None, None, None, None, None, None), (u'price', 1700, None, None, None, None, None, None), (u'quantity', 23, None, None, None, None, None, None)])])]

All those descriptions are nice, but how do we just get all those rows
back?   We need to recursively descend through the nested cursors.
The code below illustrates one way to do this::

  from akiban import NESTED_CURSOR

  def printrows(cursor, indent=""):
      for row in cursor.fetchall():
          nested = []
          out = ""
          for field, col in zip(cursor.description, row):
              if field[1] == NESTED_CURSOR:
                  nested.append((field[0], col, indent))
              else:
                  out += " " + str(col)
          print indent + out
          for key, values, indent in nested:
              printrows(values, "%s    %s: " % (indent, key))


