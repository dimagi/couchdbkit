# -*- coding: utf-8 -
#
# This file is part of couchdbkit released under the MIT license.
# See the NOTICE for more information.

"""
couchdb.resource
~~~~~~~~~~~~~~~~~~~~~~

This module providess a common interface for all CouchDB request. This
module makes HTTP request using :mod:`httplib2` module or :mod:`pycurl`
if available. Just use set transport argument for this.

Example:

    >>> resource = CouchdbResource()
    >>> info = resource.get()
    >>> info['couchdb']
    u'Welcome'

"""
from __future__ import absolute_import
import base64
import re

from . import __version__
from .utils import json, url_quote
import six

USER_AGENT = 'couchdbkit/%s' % __version__


def encode_params(params):
    """ encode parameters in json if needed """
    _params = {}
    if params:
        for name, value in params.items():
            if name in ('key', 'startkey', 'endkey'):
                value = json.dumps(value)
            elif value is None:
                continue
            elif not isinstance(value, six.string_types):
                value = json.dumps(value)
            _params[name] = value
    return _params

def escape_docid(docid):
    if docid.startswith('/'):
        docid = docid[1:]
    if docid.startswith('_design'):
        docid = '_design/%s' % url_quote(docid[8:], safe='')
    else:
        docid = url_quote(docid, safe='')
    return docid

re_sp = re.compile('\s')
def encode_attachments(attachments):
    for k, v in six.iteritems(attachments):
        if v.get('stub', False):
            continue
        else:
            v['data'] = re_sp.sub('', base64.b64encode(v['data'].encode('utf-8')).decode('utf-8'))
    return attachments
