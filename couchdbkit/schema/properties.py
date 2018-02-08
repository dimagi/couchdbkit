# -*- coding: utf-8 -
#
# This file is part of couchdbkit released under the MIT license.
# See the NOTICE for more information.
from __future__ import absolute_import
import functools
from jsonobject.properties import *
from jsonobject.base import DefaultProperty
import six

try:
    from collections import MutableSet, Iterable

    def is_iterable(c):
        return isinstance(c, Iterable)

    support_setproperty = True
except ImportError:
    support_setproperty = False

StringListProperty = functools.partial(ListProperty, six.text_type)
StringDictProperty = functools.partial(DictProperty, six.text_type)


class Property(DefaultProperty):
    def wrap(self, obj):
        try:
            return self.to_python(obj)
        except NotImplementedError:
            return super(Property, self).wrap(obj)

    def unwrap(self, obj):
        try:
            return obj, self.to_json(obj)
        except NotImplementedError:
            return super(Property, self).unwrap(obj)

    def to_python(self, value):
        raise NotImplementedError()

    def to_json(self, value):
        raise NotImplementedError()


def _not_implemented(*args, **kwargs):
    raise NotImplementedError()

dict_to_json = _not_implemented
list_to_json = _not_implemented
value_to_json = _not_implemented
dict_to_python = _not_implemented
list_to_python = _not_implemented
convert_property = _not_implemented

LazyDict = JsonDict
LazyList = JsonArray
LazySet = JsonSet
