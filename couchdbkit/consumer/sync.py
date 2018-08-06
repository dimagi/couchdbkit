# -*- coding: utf-8 -
#
# This file is part of couchdbkit released under the MIT license.
# See the NOTICE for more information.

from __future__ import with_statement

from .base import ConsumerBase, check_callable
from ..utils import json

__all__ = ['SyncConsumer']

class SyncConsumer(ConsumerBase):

    def wait_once(self, cb=None, **params):
        if cb is not None:
            check_callable(cb)

        params.update({"feed": "longpoll"})
        changes = self.db.cloudant_database.changes(**params)
        for change in changes:
            if cb is not None:
                cb(change)
                return
            return change

    def wait(self, cb, **params):
        check_callable(cb)
        params.update({"feed": "continuous"})
        changes = self.db.cloudant_database.changes(**params)

        for change in changes:
            cb(change)
