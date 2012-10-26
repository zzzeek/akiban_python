import collections

class NestedCursorType(object):
    def __eq__(self, other):
        return other == 5001 or isinstance(other, NestedCursorType)

    def __hash__(self):
        return hash(5001)

    def __repr__(self):
        return "<akiban.api.NESTED_CURSOR>"

NESTED_CURSOR = NestedCursorType()


class NestedCursor(object):

    def __init__(self, ctx, arraysize, fields, description_factory):
        self.ctx = ctx
        self._fields = fields
        self._description_factory = description_factory
        self._rows = collections.deque()
        self.arraysize = arraysize

    @property
    def description(self):
        return self._description_factory(self._fields)

    def fetchone(self):
        if self._rows:
            return self._rows.popleft()
        else:
            return None

    def fetchall(self):
        r = list(self._rows)
        self._rows.clear()
        return r

    def fetchmany(self, size=None):
        if size is None:
            size = self.arraysize
        l = list(self._rows)
        r, self._rows = l[0:size], collections.deque(l[size:])
        return r

    def close(self):
        pass