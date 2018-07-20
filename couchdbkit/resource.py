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
from datetime import datetime

from restkit import Resource, ClientResponse
from restkit.errors import ResourceError, RequestFailed
from restkit.util import url_quote, make_uri

from . import __version__
from .exceptions import ResourceNotFound, ResourceConflict, \
PreconditionFailed
from .utils import json
from .logging import request_logger
import six

USER_AGENT = 'couchdbkit/%s' % __version__

RequestFailed = RequestFailed

class CouchDBResponse(ClientResponse):

    @property
    def json_body(self):
        body = self.body_string()

        # try to decode json
        try:
            return json.loads(body)
        except ValueError:
            return body


class CouchdbResource(Resource):

    def __init__(self, uri="http://127.0.0.1:5984", **client_opts):
        """Constructor for a `CouchdbResource` object.

        CouchdbResource represent an HTTP resource to CouchDB.

        @param uri: str, full uri to the server.
        """
        client_opts['response_class'] = CouchDBResponse

        Resource.__init__(self, uri=uri, **client_opts)
        self.safe = ":/%"

    def copy(self, path=None, headers=None, **params):
        """ add copy to HTTP verbs """
        return self.request('COPY', path=path, headers=headers, **params)

    def request(self, method, path=None, payload=None, headers=None, **params):
        """ Perform HTTP call to the couchdb server and manage
        JSON conversions, support GET, POST, PUT and DELETE.

        Usage example, get infos of a couchdb server on
        http://127.0.0.1:5984 :


            import couchdbkit.CouchdbResource
            resource = couchdbkit.CouchdbResource()
            infos = resource.request('GET')

        @param method: str, the HTTP action to be performed:
            'GET', 'HEAD', 'POST', 'PUT', or 'DELETE'
        @param path: str or list, path to add to the uri
        @param data: str or string or any object that could be
            converted to JSON.
        @param headers: dict, optional headers that will
            be added to HTTP request.
        @param raw: boolean, response return a Response object
        @param params: Optional parameterss added to the request.
            Parameterss are for example the parameters for a view. See
            `CouchDB View API reference
            <http://wiki.apache.org/couchdb/HTTP_view_API>`_ for example.

        @return: tuple (data, resp), where resp is an `httplib2.Response`
            object and data a python object (often a dict).
        """
        # logging information
        start_time = datetime.utcnow()
        resp = None
        error_status = None
        has_error = False

        headers = headers or {}
        headers.setdefault('Accept', 'application/json')
        headers.setdefault('User-Agent', USER_AGENT)

        if payload is not None:
            #TODO: handle case we want to put in payload json file.
            if not hasattr(payload, 'read') and not isinstance(payload, six.string_types):
                payload = json.dumps(payload).encode('utf-8')
                headers.setdefault('Content-Type', 'application/json')

        params = encode_params(params)
        try:
            resp = Resource.request(self, method, path=path,
                             payload=payload, headers=headers, **params)
        except ResourceError as e:
            msg = getattr(e, 'msg', '')
            if e.response and msg:
                if e.response.headers.get('content-type') == 'application/json':
                    try:
                        msg = json.loads(msg)
                    except ValueError:
                        pass

            if type(msg) is dict:
                error = msg.get('reason')
            else:
                error = msg

            has_error = True
            error_status = e.status_int
            if e.status_int == 404:
                raise ResourceNotFound(error, http_code=404,
                        response=e.response)

            elif e.status_int == 409:
                raise ResourceConflict(error, http_code=409,
                        response=e.response)
            elif e.status_int == 412:
                raise PreconditionFailed(error, http_code=412,
                        response=e.response)
            else:
                raise
        except:
            raise
        finally:
            database = _get_db_from_uri(self.uri, path)
            end_time = datetime.utcnow()
            duration = end_time - start_time
            logging_context = dict(
                method=method,
                path=path,
                params=params,
                start_time=start_time,
                end_time=end_time,
                status_code=resp.status_int if resp else error_status,
                content_length=resp.headers.get('content-length') if resp else None,
                has_error=has_error,
                duration=duration,
                database=database
            )
            request_logger.debug('{} to {}/{} took {}'.format(method, database, path, duration), extra=logging_context)

        return resp

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
            v['data'] = re_sp.sub('', base64.b64encode(v['data']))
    return attachments


def _get_db_from_uri(uri, path):
    full_uri = make_uri(uri, path)
    try:
        return full_uri.split('/')[3]
    except IndexError:
        return 'unknown'
