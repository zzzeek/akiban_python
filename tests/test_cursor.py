import unittest

NUMBER = object()
STRING = object()

class NestedCursorTest(unittest.TestCase):
    def _fixture(self):
        from akiban.api import NestedCursor
        cursor = NestedCursor(
            None,
            12,
            [
                {"name":"id", "type":NUMBER},
                {"name":"value", "type": STRING}
            ],
            lambda fields: [
                    (field["name"], field["type"], None, None, None, None, None)
                    for field in fields
                ]

        )
        cursor._rows.extend(
            [
                (i, "row%d" % i) for i in xrange(20)
            ]
        )
        return cursor

    def test_fetchone(self):
        cursor = self._fixture()
        self.assertEquals(
            [cursor.fetchone() for i in xrange(22)],
            [
                (i, "row%d" % i) for i in xrange(20)
            ] + [None, None]
        )

    def test_fetchall(self):
        cursor = self._fixture()
        self.assertEquals(
            cursor.fetchall(),
            [
                (i, "row%d" % i) for i in xrange(20)
            ]
        )

    def test_fetchmany(self):
        cursor = self._fixture()
        self.assertEquals(
            cursor.fetchmany(5),
            [
                (i, "row%d" % i) for i in xrange(5)
            ]
        )
        self.assertEquals(
            cursor.fetchmany(),
            [
                (i, "row%d" % i) for i in xrange(5, 17)
            ]
        )
        self.assertEquals(
            cursor.fetchmany(10),
            [
                (i, "row%d" % i) for i in xrange(17, 20)
            ]
        )


    def test_description(self):
        cursor = self._fixture()
        self.assertEquals(
            cursor.description,
            [
                ("id", NUMBER, None, None, None, None, None),
                ("value", STRING, None, None, None, None, None),
            ]
        )