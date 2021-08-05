from permissible.crud.backends.sqlalchemy import QuerySchema


filter_spec = [{'field': 'name', 'op': 'is_null', 'value': ...}]
print(QuerySchema(filter_spec = filter_spec))
