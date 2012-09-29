import collections

class NestedCursor(object):
    def __eq__(self, other):
        return other == 5001 or isinstance(other, NestedCursor)
NESTED_CURSOR = NestedCursor()


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
