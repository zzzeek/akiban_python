from __future__ import absolute_import

import psycopg2
import psycopg2.extensions
from .impl import _filter_row, _NESTED_OID, AkibanResultContext
from .api import NESTED_CURSOR


class Cursor(psycopg2.extensions.cursor):

    def execute(self, *arg, **kw):
        ret = self._super().execute(*arg, **kw)
        self._setup_description()
        return ret

    def executemany(self, *arg, **kw):
        ret = self._super().executemany(*arg, **kw)
        self._setup_description()
        return ret

    def _super(self):
        return super(Cursor, self)

    def _setup_description(self):
        if super(Cursor, self).description:
            self._akiban_ctx = Psycopg2ResultContext(
                                self, self._super().fetchone()
                            )
        else:
            self._akiban_ctx = None

    def fetchone(self):
        return _filter_row(
                    self._super().fetchone(),
                    self._akiban_ctx
                )

    def fetchall(self):
        return [
            _filter_row(row, self._akiban_ctx)
            for row in self._super().fetchall()
        ]

    def fetchmany(self, size=None):
        return [
            _filter_row(row, self._akiban_ctx)
            for row in self._super().fetchmany(size)
        ]

    @property
    def akiban_description(self):
        if self._akiban_ctx:
            return self._akiban_ctx.akiban_description
        else:
            return None

    @property
    def description(self):
        # TODO: I'm going on a "convenient" behavior here,
        # that the ".description" attribute on psycopg2.cursor
        # acts like a method that we override below.
        # Would need to confirm that the Python
        # C API and/or psycopg2 supports this pattern.
        if self._akiban_ctx:
            return self._akiban_ctx.description
        else:
            return None


_psycopg2_adapter_cache = {
}

class Psycopg2ResultContext(AkibanResultContext):

    def gen_description(self, fields):
        return [
            (rec['name'], rec['type_oid'],
                    None, None, None, None, None)
            for rec in fields
        ]

    @property
    def description(self):
        return self.gen_description(self.fields)

    @property
    def akiban_description(self):
        return self.gen_akiban_description(self.fields)

    def gen_akiban_description(self, fields):
        return [
            (rec['name'], rec['type_oid'],
                    None, None, None, None, None,
                    self.gen_akiban_description(rec['akiban.fields'])
                    if 'akiban.fields' in rec else None
            )
            for rec in fields
        ]

    def typecast(self, value, oid):
        try:
            # return a cached "adpater" for this oid.
            # for a particular oid that's been seen before,
            # this is the only codepath.
            adapter = _psycopg2_adapter_cache[oid]
        except KeyError:
            # no "adapter".  figure it out.   we don't want to be
            # calling isinstance() on every row so we cache whether or
            # not psycopg2 returns this particular oid as a string
            # or not, assuming it will be consistent per oid.
            if isinstance(value, basestring):
                adapter = _psycopg2_adapter_cache[oid] = \
                        psycopg2.extensions.string_types[oid]
            else:
                adapter = _psycopg2_adapter_cache[oid] = None

            # hardwire STRING types to not use adapters, since we're
            # getting string data back already.  Akiban seems to be
            # sending fully unicode data back even without using psycopg2
            # unicode extensions.
            if adapter is not None and adapter == psycopg2.STRING:
                _psycopg2_adapter_cache[oid] = adapter = None

        if adapter:
            # TODO: do we send the oid or the adapter
            # as the argument here?
            return adapter(value, adapter)
        else:
            return value


class Connection(psycopg2.extensions.connection):
    def __init__(self, dsn, async=0):
        super(Connection, self).__init__(dsn, async=async)
        self._nested = False

    def _super_cursor(self, *arg, **kw):
        return super(Connection, self).cursor(*arg, **kw)

    def cursor(self, nested=True):
        if nested:
            cursor = self._super_cursor(cursor_factory=Cursor)
        else:
            cursor = self._super_cursor()
        return self._set_output_format(cursor, nested)

    def _set_output_format(self, cursor, nested):
        if nested is not self._nested:
            cursor.execute("set OutputFormat='%s'" %
                        ('json_with_meta_data' if nested else 'table')
                    )
            self._nested = nested
        return cursor

# TODO: need to get per-connection adapters going
# (or get akiban to recognize bool, easier)
psycopg2.extensions.register_adapter(
        bool,
        lambda value: psycopg2.extensions.AsIs(int(value))
    )

