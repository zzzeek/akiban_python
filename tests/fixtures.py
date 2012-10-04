def _table_fixture(connection):
    connection.autocommit = True
    cursor = connection.cursor()

    cursor.execute("""DROP TABLE IF EXISTS items""")
    cursor.execute("""DROP TABLE IF EXISTS orders""")
    cursor.execute("""DROP TABLE IF EXISTS customers""")

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS customers
      (
         customer_id   INT NOT NULL PRIMARY KEY,
         rand_id       INT,
         name          VARCHAR(20),
         customer_info VARCHAR(100),
         birthdate     DATE,
         some_bool     BOOLEAN
      )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS orders
      (
         order_id    INT NOT NULL PRIMARY KEY,
         customer_id INT NOT NULL,
         order_info  VARCHAR(200),
         order_date  DATETIME NOT NULL,
         some_bool   BOOLEAN,
         GROUPING FOREIGN KEY(customer_id) REFERENCES customers
      )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS items
      (
         item_id  INT NOT NULL PRIMARY KEY,
         order_id INT NOT NULL,
         price    DECIMAL(10, 2) NOT NULL,
         quantity INT,
         GROUPING FOREIGN KEY(order_id) REFERENCES orders
      )
    """)
    cursor.close()
    connection.autocommit = False

def _data_fixture(connection):
    cursor = connection.cursor()
    cursor.executemany(
        "INSERT INTO customers VALUES (%s, floor(1 + rand() * 100), %s, %s, %s, %s)",
        [
        (1, 'David McFarlane', 'Co-Founder and CEO', '1982-07-16', True),
        (2, 'Ori Herrnstadt', 'Co-Founder and CTO', '1982-07-16', True),
        (3, 'Tim Wegner', 'VP of Engineering', '1982-07-16', True),
        (4, 'Jack Orenstein', 'Software Engineer', '1982-07-16', False),
        (5, 'Peter Beaman', 'Software Engineer', '1982-07-16', False),
        (6, 'Thomas Jones-Low', 'Software Engineer', '1982-07-16', True),
        (7, 'Mike McMahon', 'Software Engineer', '1982-07-16', False),
        (8, 'Padraig O''Sullivan', 'Software Engineer', '1983-12-09', True),
        (9, 'Yuval Shavit', 'Software Engineer', '1983-07-05', False),
        (10, 'Nathan Williams', 'Software Engineer', '1984-05-01', True),
        (11, 'Chris Ernenwein', 'Software Testing Engineer', '1982-07-16', False),
        ]
    )


    cursor.executemany(
        "INSERT INTO orders VALUES(%s, %s, %s, %s)",
        [
        (101, 1, 'apple related', '2012-09-05 17:24:12'),
        (102, 1, 'apple related', '2012-09-05 17:24:12'),
        (103, 1, 'apple related', '2012-09-05 17:24:12'),
        (104, 2, 'kite', '2012-09-05 17:24:12'),
        (105, 2, 'surfboard', '2012-09-05 17:24:12'),
        (106, 2, 'some order info', '2012-09-05 17:24:12'),
        (107, 3, 'some order info', '2012-09-05 17:24:12'),
        (108, 3, 'some order info', '2012-09-05 17:24:12'),
        (109, 3, 'some order info', '2012-09-05 17:24:12'),
        (110, 4, 'some order info', '2012-09-05 17:24:12'),
        (111, 4, 'some order info', '2012-09-05 17:24:12'),
        (112, 4, 'some order info', '2012-09-05 17:24:12'),
        (113, 5, 'some order info', '2012-09-05 17:24:12'),
        (114, 5, 'some order info', '2012-09-05 17:24:12'),
        (115, 5, 'some order info', '2012-09-05 17:24:12'),
        (116, 6, 'some order info', '2012-09-05 17:24:12'),
        (117, 6, 'some order info', '2012-09-05 17:24:12'),
        (118, 6, 'some order info', '2012-09-05 17:24:12'),
        (119, 7, 'some order info', '2012-09-05 17:24:12'),
        (120, 7, 'some order info', '2012-09-05 17:24:12'),
        (121, 7, 'some order info', '2012-09-05 17:24:12'),
        (122, 8, 'some order info', '2012-09-05 17:24:12'),
        (123, 8, 'some order info', '2012-09-05 17:24:12'),
        (124, 8, 'some order info', '2012-09-05 17:24:12'),
        (125, 9, 'some order info', '2012-09-05 17:24:12'),
        (126, 9, 'some order info', '2012-09-05 17:24:12'),
        (127, 9, 'some order info', '2012-09-05 17:24:12'),
        (128, 10, 'some order info', '2012-09-05 17:24:12'),
        (129, 10, 'some order info', '2012-09-05 17:24:12'),
        (130, 10, 'some order info', '2012-09-05 17:24:12'),
        (131, 11, 'some order info', '2012-09-05 17:24:12'),
        (132, 11, 'some order info', '2012-09-05 17:24:12'),
        (133, 11, 'some order info', '2012-09-05 17:24:12'),
        ])

    cursor.executemany(
        "INSERT INTO items VALUES (%s, %s, %s, %s)",
        [
            (1001, 101, 9.99, 1),
            (1002, 101, 19.99, 2),
            (1003, 102, 9.99, 1),
            (1004, 103, 9.99, 1),
            (1005, 104, 9.99, 5),
            (1006, 105, 9.99, 1),
            (1007, 106, 9.99, 1),
            (1008, 107, 999.99, 1),
            (1009, 107, 9.99, 1),
            (1010, 108, 9.99, 1),
            (1011, 109, 9.99, 1),
        ]
    )
    cursor.close()

def _drop_tables(connection):
    tables = ['items', 'orders', 'customers']
    connection.rollback()
    connection.autocommit = True
    for tname in tables:
        cursor = connection.cursor()
        cursor.execute("DROP TABLE %s" % tname)
    connection.autocommit = False

