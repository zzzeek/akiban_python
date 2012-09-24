import json
from .api import NestedCursor

json_decoder = json.JSONDecoder()

_NESTED_OID = 5001

class AkibanResultContext(object):

    def gen_description(self, fields):  # pragma: no cover
        raise NotImplementedError()

    def typecast(self, value, oid):  # pragma: no cover
        raise NotImplementedError()

    def __init__(self, cursor, firstrow):
        self.cursor = cursor
        self.fields = _fields_from_row(firstrow)

    @property
    def arraysize(self):
        return self.cursor.arraysize

def _fields_from_row(row):
    document = json_decoder.decode(row[0])
    return _format_fields(document)

def _filter_row(row, ctx):
    if row is None:
        return None
    document = json_decoder.decode(row[0])
    return _format_row(document, ctx.fields, ctx)

def _create_rowset(document, fields, ctx):
    return [
        _format_row(row, fields, ctx)
        for row in document
    ]

def _format_row(document, fields, ctx):
    row = []
    for field in fields:
        if field['type_oid'] == _NESTED_OID:
            value = NestedCursor(
                        ctx,
                        ctx.arraysize,
                        field['akiban.fields'],
                        ctx.gen_description
            )
            value._rows.extend(
                _create_rowset(
                    document[field['name']],
                    field['akiban.fields'],
                    ctx
                )
            )

        else:
            value = ctx.typecast(
                            document[field['name']],
                            field['type_oid']
                        )
        row.append(value)
    return tuple(row)

def _format_fields(document):
    ret = []
    for attrnum, rec in enumerate(document):
        newrec = {
            'table_oid': None,
            'name': rec['name'],
            'column_attrnum': attrnum,
            'format': None,
            'type_modifier': -1,
            'type_size': -1
        }
        if 'columns' in rec:
            newrec['type_oid'] = _NESTED_OID
            newrec['akiban.fields'] = _format_fields(rec['columns'])
        else:
            newrec['type_oid'] = rec['oid']
        ret.append(newrec)
    return ret
