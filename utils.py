class CursorProxy(object):
    def __init__(self, cursor):
        self._cursor = cursor

    def executemany(self, statement, parameters, **kwargs):
        # convert parameters to a list
        parameters = list(parameters)
        # pass through to proxied cursor
        return self._cursor.executemany(statement, parameters, **kwargs)

    def __getattr__(self, item):
        return getattr(self._cursor, item)
