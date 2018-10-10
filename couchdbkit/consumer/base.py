# -*- coding: utf-8 -
#
# This file is part of couchdbkit released under the MIT license.
# See the NOTICE for more information.


def check_callable(cb):
    if not callable(cb):
        raise TypeError("callback isn't a callable")


class ConsumerBase(object):

    def __init__(self, db, **kwargs):
        self.db = db

    def fetch(self, cb=None, **params):
        changes = self.db.cloudant_database.changes(**params)
        try:
            change = next(changes)
        except StopIteration:
            return {
                'last_seq': changes.last_seq,
                'results': []
            }
        if cb is not None:
            check_callable(cb)
            cb(change)
        else:
            return {
                'last_seq': change['seq'],
                'results': [change]
            }

    def wait_once(self, cb=None, **params):
        raise NotImplementedError

    def wait(self, cb, **params):
        raise NotImplementedError

    def wait_once_async(self, cb, **params):
        raise NotImplementedError

    def wait_async(self, cb, **params):
        raise NotImplementedError
