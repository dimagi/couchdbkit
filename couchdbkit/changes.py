# -*- coding: utf-8 -
#
# This file is part of couchdbkit released under the MIT license.
# See the NOTICE for more information.

"""
module to fetch and stream changes from a database
"""


class ChangesStream(object):
    """\
    Change stream object.

    .. code-block:: python

        from couchdbkit import Server from couchdbkit.changes import ChangesStream

        s = Server()
        db = s['testdb']
        stream = ChangesStream(db)

        print "got change now"
        for c in stream:
            print c

        print "stream changes"
        with ChangesStream(db, feed="continuous",  heartbeat=True) as stream:
            for c in stream: print c

    """

    def __init__(self, db, **params):
        self.db = db
        self.params = params

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def __iter__(self):
        feed = self.db.cloudant_database.changes(**self.params)
        for change in feed:
            yield change

    def __next__(self):
        return self


def fold(db, fun, acc, since=0):
    """Fold each changes and accuumulate result using a function

    Args:

        @param db: Database, a database object
        @param fun: function, a callable with arity 2
        @param since: int, sequence where to start the feed

    @return: list, last acc returned

    Ex of function:

        fun(change_object,acc):
            return acc

    If the function return "stop", the changes feed will stop.


    """

    if not callable(fun):
        raise TypeError("fun isn't a callable")

    with ChangesStream(db, since=since) as st:
        for c in st:
            acc = fun(c, acc)
    return acc


def foreach(db, fun, since=0):
    """Iter each changes and pass it to the callable"""

    if not callable(fun):
        raise TypeError("fun isn't a callable")

    with ChangesStream(db, since=since) as st:
        for c in st:
            fun(c)
