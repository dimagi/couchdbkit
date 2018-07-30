# -*- coding: utf-8 -
#
# This file is part of couchdbkit released under the MIT license.
# See the NOTICE for more information.

"""
Client implementation for CouchDB access. It allows you to manage a CouchDB
server, databases, documents and views. All objects mostly reflect python
objects for convenience. Server and Database objects for example, can be
used as easy as a dict.

Example:

    >>> from couchdbkit import Server
    >>> server = Server()
    >>> db = server.create_db('couchdbkit_test')
    >>> doc = { 'string': 'test', 'number': 4 }
    >>> db.save_doc(doc)
    >>> docid = doc['_id']
    >>> doc2 = db.get(docid)
    >>> doc['string']
    u'test'
    >>> del db[docid]
    >>> docid in db
    False
    >>> del server['simplecouchdb_test']

"""
from __future__ import absolute_import

import base64
from collections import deque
from copy import deepcopy
from itertools import groupby
import json
from mimetypes import guess_type
import time

import cloudant
from cloudant.client import CouchDB
from cloudant.database import CouchDatabase
from cloudant.document import Document
from cloudant.error import CloudantClientException
from cloudant.security_document import SecurityDocument
from requests.exceptions import HTTPError
from restkit.util import url_quote
import six
from six.moves import filter
from six.moves.urllib.parse import urljoin, unquote

from couchdbkit.logging import error_logger
from .exceptions import InvalidAttachment, NoResultFound, \
        ResourceNotFound, ResourceConflict, BulkSaveError, MultipleResultsFound, NoLongerSupportedException
from . import resource
from .utils import validate_dbname

from .schema.util import maybe_schema_wrapper


DEFAULT_UUID_BATCH_COUNT = 1000
UNKOWN_INFO = {}


def _maybe_serialize(doc):
    if hasattr(doc, "to_json"):
        # try to validate doc first
        try:
            doc.validate()
        except AttributeError:
            pass

        return doc.to_json(), True
    elif isinstance(doc, dict):
        return doc.copy(), False

    return doc, False


class Server(object):
    """ Server object that allows you to access and manage a couchdb node.
    A Server object can be used like any `dict` object.
    """

    resource_class = resource.CouchdbResource

    def __init__(self, uri='http://127.0.0.1:5984',
            uuid_batch_count=DEFAULT_UUID_BATCH_COUNT,
            resource_class=None, resource_instance=None,
            **client_opts):

        """ constructor for Server object

        @param uri: uri of CouchDb host
        @param uuid_batch_count: max of uuids to get in one time
        @param resource_instance: `restkit.resource.CouchdbDBResource` instance.
            It alows you to set a resource class with custom parameters.
        """

        if not uri or uri is None:
            raise ValueError("Server uri is missing")

        if uri.endswith("/"):
            uri = uri[:-1]

        self.uri = uri
        self.uuid_batch_count = uuid_batch_count
        self._uuid_batch_count = uuid_batch_count

        if resource_class is not None:
            self.resource_class = resource_class

        if resource_instance and isinstance(resource_instance,
                                resource.CouchdbResource):
            resource_instance.initial['uri'] = uri
            self.res = resource_instance.clone()
            if client_opts:
                self.res.client_opts.update(client_opts)
        else:
            self.res = self.resource_class(uri, **client_opts)
        self._uuids = deque()
        # admin_party is true, because the username/pass is passed in uri for now
        self.cloudant_client = CouchDB('', '', url=uri, admin_party=True, connect=True)

    @property
    def _request_session(self):
        return self.cloudant_client.r_session

    def info(self):
        """ info of server

        @return: dict

        """
        try:
            resp = self._request_session.get(self.uri)
            resp.raise_for_status()
        except Exception:
            return UNKOWN_INFO

        return resp.json()

    def all_dbs(self):
        """ get list of databases in CouchDb host

        """
        return self.cloudant_client.all_dbs()

    def get_db(self, dbname, **params):
        """
        Try to return a Database object for dbname.

        """
        return Database(self._db_uri(dbname), server=self, **params)

    def create_db(self, dbname, **params):
        """ Create a database on CouchDb host

        @param dname: str, name of db
        @param param: custom parameters to pass to create a db. For
        example if you use couchdbkit to access to cloudant or bigcouch:

            Ex: q=12 or n=4

        See https://github.com/cloudant/bigcouch for more info.

        @return: Database instance if it's ok or dict message
        """
        return self.get_db(dbname, create=True, **params)

    get_or_create_db = create_db
    get_or_create_db.__doc__ = """
        Try to return a Database object for dbname. If
        database doest't exist, it will be created.

        """

    def delete_db(self, dbname):
        """
        Delete database
        """
        try:
            del self[dbname]
        except CloudantClientException as e:
            raise ResourceNotFound(six.text_type(e))

    #TODO: maintain list of replications
    def replicate(self, source, target, **params):
        """
        simple handler for replication

        @param source: str, URI or dbname of the source
        @param target: str, URI or dbname of the target
        @param params: replication options

        More info about replication here :
        http://wiki.apache.org/couchdb/Replication

        """
        replicator = cloudant.replicator.Replication(self.cloudant_client)
        source_db = Database(self.cloudant_client, source)
        target_db = Database(self.cloudant_client, target)
        return replicator.create_replication(source_db, target_db, **params)

    def active_tasks(self):
        """ return active tasks """
        resp = self._request_session.get(urljoin(self.uri, '/_active_tasks'))
        resp.raise_for_status()
        return resp.json()

    def uuids(self, count=1):
        resp = self._request_session.get(urljoin(self.uri, '/_uuids'), params={'count': count})
        resp.raise_for_status()
        return resp.json()

    def next_uuid(self, count=None):
        """
        return an available uuid from couchdbkit
        """
        if count is not None:
            self._uuid_batch_count = count
        else:
            self._uuid_batch_count = self.uuid_batch_count

        try:
            return self._uuids.pop()
        except IndexError:
            self._uuids.extend(self.uuids(count=self._uuid_batch_count)["uuids"])
            return self._uuids.pop()

    def __getitem__(self, dbname):
        return Database(self._db_uri(dbname), server=self)

    def __delitem__(self, dbname):
        self.cloudant_client.delete_database(dbname)

    def __contains__(self, dbname):
        try:
            self.cloudant_client[dbname]
        except KeyError:
            return False
        return True

    def __iter__(self):
        for dbname in self.all_dbs():
            yield Database(self._db_uri(dbname), server=self)

    def __len__(self):
        return len(self.all_dbs())

    def __nonzero__(self):
        return (len(self) > 0)

    def _db_uri(self, dbname):
        if dbname.startswith("/"):
            dbname = dbname[1:]

        dbname = url_quote(dbname, safe=":")
        return "/".join([self.uri, dbname])


class Database(object):
    """ Object that abstract access to a CouchDB database
    A Database object can act as a Dict object.
    """

    def __init__(self, uri, create=False, server=None, **params):
        """Constructor for Database

        @param uri: str, Database uri
        @param create: boolean, False by default,
        if True try to create the database.
        @param server: Server instance

        """
        self.uri = uri.rstrip('/')
        self.server_uri, self.dbname = self.uri.rsplit("/", 1)
        self.cloudant_dbname = unquote(self.dbname)

        if server is not None:
            if not hasattr(server, 'next_uuid'):
                raise TypeError('%s is not a couchdbkit.Server instance' %
                            server.__class__.__name__)
            self.server = server
        else:
            self.server = server = Server(self.server_uri, **params)

        self.cloudant_client = self.server.cloudant_client

        validate_dbname(self.dbname)
        self.cloudant_database = CouchDatabase(self.cloudant_client, self.cloudant_dbname)
        if create:
            self.cloudant_database.create()

        self.res = server.res(self.dbname)
        self._request_session = self.server._request_session
        self.database_url = self.cloudant_database.database_url

    def __repr__(self):
        return "<%s %s>" % (self.__class__.__name__, self.dbname)

    def _database_path(self, path):
        return '/'.join([self.database_url, path])

    def info(self):
        """
        Get database information

        @return: dict
        """
        return self.cloudant_database.metadata()

    def set_security(self, secobj):
        """ set database securrity object """
        with SecurityDocument(self.cloudant_database) as sec_doc:
            # context manager saves
            for key in sec_doc:
                del sec_doc[key]
            for k, v in secobj.items():
                sec_doc[k] = v
        return self.get_security()

    def get_security(self):
        """ get database secuirity object """
        return self.cloudant_database.get_security_document()

    def compact(self, dname=None):
        """ compact database
        @param dname: string, name of design doc. Usefull to
        compact a view.
        """
        path = "/_compact"
        if dname is not None:
            path = "%s/%s" % (path, resource.escape_docid(dname))
        path = self._database_path(path)
        res = self._request_session.post(path, headers={"Content-Type": "application/json"})
        res.raise_for_status()
        return res.json()

    def view_cleanup(self):
        return self.cloudant_database.view_cleanup()

    def flush(self):
        """ Remove all docs from a database
        except design docs."""

        # save ddocs
        all_ddocs = self.all_docs(startkey="_design", endkey="_design/"+u"\u9999", include_docs=True)
        ddocs = []
        for ddoc in all_ddocs:
            doc = ddoc['doc']
            old_atts = doc.get('_attachments', {})
            atts = {}
            for name, info in old_atts.items():
                att = {}
                att['content_type'] = info['content_type']
                att['data'] = self.fetch_attachment(ddoc['doc'], name)
                atts[name] = att

            # create a fresh doc
            doc.pop('_rev')
            doc['_attachments'] = resource.encode_attachments(atts)

            ddocs.append(doc)

        # delete db
        self.server.delete_db(self.dbname)

        # we let a chance to the system to sync
        times = 0
        while times < 10:
            if self.dbname in self.server:
                break
            time.sleep(0.2)
            times += 1

        # recreate db + ddocs
        self.server.create_db(self.dbname)
        self.bulk_save(ddocs)

    def doc_exist(self, docid):
        """Test if document exists in a database

        @param docid: str, document id
        @return: boolean, True if document exist
        """
        doc = Document(self.cloudant_database, docid)
        return doc.exists()

    def open_doc(self, docid, **params):
        """Get document from database

        Args:
        @param docid: str, document id to retrieve
        @param wrapper: callable. function that takes dict as a param.
        Used to wrap an object.
        @param **params: See doc api for parameters to use:
        http://wiki.apache.org/couchdb/HTTP_Document_API

        @return: dict, representation of CouchDB document as
         a dict.
        """
        wrapper = None
        if "wrapper" in params:
            wrapper = params.pop("wrapper")
        elif "schema" in params:
            schema = params.pop("schema")
            if not hasattr(schema, "wrap"):
                raise TypeError("invalid schema")
            wrapper = schema.wrap
        attachments = params.get('attachments', False)

        if isinstance(docid, six.text_type):
            docid = docid.encode('utf-8')
        doc = Document(self.cloudant_database, docid)
        try:
            doc.fetch()
        except HTTPError as e:
            if e.response.status_code == 404:
                raise ResourceNotFound(json.loads(e.response.content)['reason'])
            raise
        doc_dict = dict(doc)

        if attachments and '_attachments' in doc_dict:
            for attachment_name in doc_dict['_attachments']:
                attachment_data = doc.get_attachment(attachment_name, attachment_type='binary')
                doc_dict['_attachments'][attachment_name]['data'] = base64.b64encode(attachment_data)
                del doc_dict['_attachments'][attachment_name]['stub']
                del doc_dict['_attachments'][attachment_name]['length']

        if wrapper is not None:
            if not callable(wrapper):
                raise TypeError("wrapper isn't a callable")

            return wrapper(doc_dict)

        return doc_dict
    get = open_doc

    def list(self, list_name, view_name, **params):
        """ Execute a list function on the server and return the response.
        If the response is json it will be deserialized, otherwise the string
        will be returned.

        Args:
            @param list_name: should be 'designname/listname'
            @param view_name: name of the view to run through the list document
            @param params: params of the list
        """
        list_name = list_name.split('/')
        dname = list_name.pop(0)
        vname = '/'.join(list_name)
        list_path = '_design/%s/_list/%s/%s' % (dname, vname, view_name)

        return self.res.get(list_path, **params).json_body

    def show(self, show_name, doc_id, **params):
        """ Execute a show function on the server and return the response.
        If the response is json it will be deserialized, otherwise the string
        will be returned.

        Args:
            @param show_name: should be 'designname/showname'
            @param doc_id: id of the document to pass into the show document
            @param params: params of the show
        """
        show_name = show_name.split('/')
        dname = show_name.pop(0)
        vname = '/'.join(show_name)
        show_path = '_design/%s/_show/%s/%s' % (dname, vname, doc_id)

        return self.res.get(show_path, **params).json_body

    def update(self, update_name, doc_id=None, **params):
        """ Execute update function on the server and return the response.
        If the response is json it will be deserialized, otherwise the string
        will be returned.

        Args:
            @param update_name: should be 'designname/updatename'
            @param doc_id: id of the document to pass into the update function
            @param params: params of the update
        """
        update_name = update_name.split('/')
        dname = update_name.pop(0)
        uname = '/'.join(update_name)

        if doc_id is None:
            update_path = '_design/%s/_update/%s' % (dname, uname)
            return self.res.post(update_path, **params).json_body
        else:
            update_path = '_design/%s/_update/%s/%s' % (dname, uname, doc_id)
            return self.res.put(update_path, **params).json_body

    def all_docs(self, by_seq=False, **params):
        """Get all documents from a database

        This method has the same behavior as a view.

        `all_docs( **params )` is the same as `view('_all_docs', **params)`
         and `all_docs( by_seq=True, **params)` is the same as
        `view('_all_docs_by_seq', **params)`

        You can use all(), one(), first() just like views

        Args:
        @param by_seq: bool, if True the "_all_docs_by_seq" is passed to
        couchdb. It will return an updated list of all documents.

        @return: list, results of the view
        """
        if by_seq:
            try:
                return self.view('_all_docs_by_seq', **params)
            except ResourceNotFound:
                # CouchDB 0.11 or sup
                raise AttributeError("_all_docs_by_seq isn't supported on Couchdb %s" % self.server.info()[1])

        return self.view('_all_docs', **params)

    def get_rev(self, docid):
        """ Get last revision from docid (the '_rev' member)
        @param docid: str, undecoded document id.

        @return rev: str, the last revision of document.
        """
        response = self._request_session.head(self._database_path(docid))
        try:
            response.raise_for_status()
        except HTTPError as e:
            if e.response.status_code == 404:
                raise ResourceNotFound
            raise
        # https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/ETag
        return response.headers['ETag'].strip('"').lstrip('W/"')

    def save_doc(self, doc, encode_attachments=True, force_update=False,
            **params):
        """ Save a document. It will use the `_id` member of the document
        or request a new uuid from CouchDB. IDs are attached to
        documents on the client side because POST has the curious property of
        being automatically retried by proxies in the event of network
        segmentation and lost responses. (Idee from `Couchrest <http://github.com/jchris/couchrest/>`)

        @param doc: dict.  doc is updated
        with doc '_id' and '_rev' properties returned
        by CouchDB server when you save.
        @param force_update: boolean, if there is conlict, try to update
        with latest revision
        @param params, list of optionnal params, like batch="ok"

        @return res: result of save. doc is updated in the mean time
        """
        if doc is None:
            doc1 = {}
        else:
            doc1, schema = _maybe_serialize(doc)

        if '_attachments' in doc1 and encode_attachments:
            doc1['_attachments'] = resource.encode_attachments(doc['_attachments'])

        if '_id' in doc1:
            docid = doc1['_id'].encode('utf-8')
            couch_doc = Document(self.cloudant_database, docid)
            couch_doc.update(doc1)
            try:
                # Copied from Document.save to ensure that a deleted doc cannot be saved.
                headers = {}
                headers.setdefault('Content-Type', 'application/json')
                put_resp = couch_doc.r_session.put(
                    couch_doc.document_url,
                    data=couch_doc.json(),
                    headers=headers
                )
                put_resp.raise_for_status()
                data = put_resp.json()
                super(Document, couch_doc).__setitem__('_rev', data['rev'])
            except HTTPError as e:
                if e.response.status_code != 409:
                    raise

                if force_update:
                    couch_doc['_rev'] = self.get_rev(docid)
                    couch_doc.save()
                else:
                    raise ResourceConflict
            res = couch_doc
        else:
            res = self.cloudant_database.create_document(doc1)

        if 'batch' in params and ('id' in res or '_id' in res):
            doc1.update({ '_id': res.get('_id')})
        else:
            doc1.update({'_id': res.get('_id'), '_rev': res.get('_rev')})

        if schema:
            for key, value in six.iteritems(doc.__class__.wrap(doc1)):
                doc[key] = value
        else:
            doc.update(doc1)
        return {
            'id': res['_id'],
            'rev': res['_rev'],
            'ok': True,
        }

    def save_docs(self, docs, use_uuids=True, new_edits=None, **params):
        """ bulk save. Modify Multiple Documents With a Single Request

        @param docs: list of docs
        @param use_uuids: add _id in doc who don't have it already set.
        @param new_edits: When False, this saves existing revisions instead of
        creating new ones. Used in the replication Algorithm. Each document
        should have a _revisions property that lists its revision history.

        .. seealso:: `HTTP Bulk Document API <http://wiki.apache.org/couchdb/HTTP_Bulk_Document_API>`

        """

        if not isinstance(docs, (list, tuple)):
            docs = tuple(docs)
        docs1 = []
        docs_schema = []
        for doc in docs:
            doc1, schema = _maybe_serialize(doc)
            docs1.append(doc1)
            docs_schema.append(schema)

        def is_id(doc):
            return '_id' in doc

        if use_uuids:
            noids = []
            for k, g in groupby(docs1, is_id):
                if not k:
                    noids = list(g)

            uuid_count = max(len(noids), self.server.uuid_batch_count)
            for doc in noids:
                nextid = self.server.next_uuid(count=uuid_count)
                if nextid:
                    doc['_id'] = nextid

        payload = {"docs": docs1}
        if new_edits is not None:
            payload["new_edits"] = new_edits

        # update docs
        res = self._request_session.post(
            self._database_path('_bulk_docs'), data=json.dumps(payload),
            headers={"Content-Type": "application/json"}, **params)
        res.raise_for_status()
        results = res.json()

        errors = []
        for i, res in enumerate(results):
            if 'error' in res:
                errors.append(res)
                logging_context = dict(
                    method='save_docs',
                    params=params,
                    error=res['error'],
                )
                error_logger.error("save_docs error", extra=logging_context)
            else:
                if docs_schema[i]:
                    docs[i]._doc.update({
                        '_id': res['id'],
                        '_rev': res['rev']
                    })
                else:
                    docs[i].update({
                        '_id': res['id'],
                        '_rev': res['rev']
                    })
        if errors:
            raise BulkSaveError(errors, results)
        return results
    bulk_save = save_docs

    def delete_docs(self, docs, empty_on_delete=False, **params):
        """ bulk delete.
        It adds '_deleted' member to doc then uses bulk_save to save them.

        @param empty_on_delete: default is False if you want to make
        sure the doc is emptied and will not be stored as is in Apache
        CouchDB.

        .. seealso:: `HTTP Bulk Document API <http://wiki.apache.org/couchdb/HTTP_Bulk_Document_API>`


        """

        if empty_on_delete:
            for doc in docs:
                new_doc = {"_id": doc["_id"],
                        "_rev": doc["_rev"],
                        "_deleted": True}
                doc.clear()
                doc.update(new_doc)
        else:
            for doc in docs:
                doc['_deleted'] = True

        return self.bulk_save(docs, use_uuids=False, **params)

    bulk_delete = delete_docs

    def delete_doc(self, doc, **params):
        """ delete a document or a list of documents
        @param doc: str or dict,  document id or full doc.
        @return: dict like:

        .. code-block:: python

            {"ok":true,"rev":"2839830636"}
        """
        result = { 'ok': False }

        doc1, schema = _maybe_serialize(doc)

        if isinstance(doc1, dict):
            if not '_id' or not '_rev' in doc1:
                raise KeyError('_id and _rev are required to delete a doc')

            couch_doc = Document(self.cloudant_database, doc1['_id'])
            couch_doc['_rev'] = doc1['_rev']
        elif isinstance(doc1, six.string_types): # we get a docid
            couch_doc = Document(self.cloudant_database, doc1)
            couch_doc['_rev'] = self.get_rev(doc1)

        # manual request because cloudant library doesn't return result
        res = self._request_session.delete(
            couch_doc.document_url,
            params={"rev": couch_doc["_rev"]},
        )
        res.raise_for_status()
        result = res.json()

        if schema:
            doc._doc.update({
                "_rev": result['rev'],
                "_deleted": True
            })
        elif isinstance(doc, dict):
            doc.update({
                "_rev": result['rev'],
                "_deleted": True
            })
        return result

    def copy_doc(self, doc, dest=None, headers=None):
        """ copy an existing document to a new id. If dest is None, a new uuid will be requested
        @param doc: dict or string, document or document id
        @param dest: basestring or dict. if _rev is specified in dict it will override the doc
        """

        if not headers:
            headers = {}

        doc1, schema = _maybe_serialize(doc)
        if isinstance(doc1, six.string_types):
            docid = doc1
        else:
            if '_id' not in doc1:
                raise KeyError('_id is required to copy a doc')
            docid = doc1['_id']

        if dest is None:
            destination = self.server.next_uuid(count=1)
        elif isinstance(dest, six.string_types):
            if dest in self:
                dest = self.get(dest)
                destination = "%s?rev=%s" % (dest['_id'], dest['_rev'])
            else:
                destination = dest
        elif isinstance(dest, dict):
            if '_id' in dest and '_rev' in dest and dest['_id'] in self:
                destination = "%s?rev=%s" % (dest['_id'], dest['_rev'])
            else:
                raise KeyError("dest doesn't exist or this not a document ('_id' or '_rev' missig).")

        if destination:
            headers.update({"Destination": str(destination)})
            resp = self._request_session.request('copy', self._database_path(docid), headers=headers)
            resp.raise_for_status()
            return resp.json()

        return {'ok': False}

    def raw_view(self, view_path, params):
        if 'keys' in params:
            keys = params.pop('keys')
            return self.res.post(view_path, payload={ 'keys': keys }, **params)
        else:
            return self.res.get(view_path, **params)

    def view(self, view_name, schema=None, wrapper=None, **params):
        """ get view results from database. viewname is generally
        a string like `designname/viewname". It return an ViewResults
        object on which you could iterate, list, ... . You could wrap
        results in wrapper function, a wrapper function take a row
        as argument. Wrapping could be also done by passing an Object
        in obj arguments. This Object should have a `wrap` method
        that work like a simple wrapper function.

        @param view_name, string could be '_all_docs', '_all_docs_by_seq',
        'designname/viewname' if view_name start with a "/" it won't be parsed
        and beginning slash will be removed. Usefull with c-l for example.
        @param schema, Object with a wrapper function
        @param wrapper: function used to wrap results
        @param params: params of the view

        """

        if view_name.startswith('/'):
            view_name = view_name[1:]
        if view_name == '_all_docs':
            view_path = view_name
        elif view_name == '_all_docs_by_seq':
            view_path = view_name
        else:
            view_name = view_name.split('/')
            dname = view_name.pop(0)
            vname = '/'.join(view_name)
            view_path = '_design/%s/_view/%s' % (dname, vname)

        return ViewResults(self.raw_view, view_path, wrapper, schema, params)

    def search( self, view_name, handler='_fti/_design', wrapper=None, schema=None, **params):
        """ Search. Return results from search. Use couchdb-lucene
        with its default settings by default."""
        return ViewResults(self.raw_view,
                    "/%s/%s" % (handler, view_name),
                    wrapper=wrapper, schema=schema, params=params)

    def documents(self, schema=None, wrapper=None, **params):
        """ return a ViewResults objects containing all documents.
        This is a shorthand to view function.
        """
        return ViewResults(self.raw_view, '_all_docs',
                wrapper=wrapper, schema=schema, params=params)
    iterdocuments = documents

    def put_attachment(self, doc, content, name=None, content_type=None,
            content_length=None, headers=None):
        raise NoLongerSupportedException

    def delete_attachment(self, doc, name, headers=None):
        raise NoLongerSupportedException

    def fetch_attachment(self, id_or_doc, name, stream=False, headers=None):
        raise NoLongerSupportedException


    def ensure_full_commit(self):
        """ commit all docs in memory """
        path = self._database_path('_ensure_full_commit')
        res = self._request_session.post(path, headers={"Content-Type": "application/json"})
        res.raise_for_status()
        return res.json()

    def __len__(self):
        return self.info()['doc_count']

    def __contains__(self, docid):
        return self.doc_exist(docid)

    def __getitem__(self, docid):
        return self.get(docid)

    def __setitem__(self, docid, doc):
        doc['_id'] = docid
        self.save_doc(doc)

    def __delitem__(self, docid):
        self.delete_doc(docid)

    def __iter__(self):
        return self.documents().iterator()

    def __nonzero__(self):
        return (len(self) > 0)


class ViewResults(object):
    """
    Object to retrieve view results.
    """

    def __init__(self, fetch, arg, wrapper, schema, params):
        """
        Constructor of ViewResults object

        @param fetch: function (view_path, params) -> restkit.Response
        @param arg: view path to use when fetching view
        @param wrapper: function to wrap rows with
        @param schema: schema or doc_type -> schema map to wrap rows with
        (only one of wrapper, schema must be set)
        @param params: params to apply when fetching view.

        """
        assert not (wrapper and schema)
        wrap_doc = params.get('wrap_doc', schema is not None)
        if schema:
            schema_wrapper = maybe_schema_wrapper(schema, params)
            def row_wrapper(row):
                data = row.get('value')
                docid = row.get('id')
                doc = row.get('doc')
                if doc is not None and wrap_doc:
                    return schema_wrapper(doc)
                elif not data or data is None:
                    return row
                elif not isinstance(data, dict) or not docid:
                    return row
                else:
                    data['_id'] = docid
                    if 'rev' in data:
                        data['_rev'] = data.pop('rev')
                    return schema_wrapper(data)
        else:
            def row_wrapper(row):
                return row

        self._fetch = fetch
        self._arg = arg
        self.wrapper = wrapper or row_wrapper
        self.params = params or {}
        self._result_cache = None
        self._total_rows = None
        self._offset = 0
        self._dynamic_keys = []

    def iterator(self):
        self._fetch_if_needed()
        rows = self._result_cache.get('rows', [])
        wrapper = self.wrapper
        for row in rows:
            yield wrapper(row)

    def first(self):
        """
        Return the first result of this query or None if the result doesn’t contain any row.

        This results in an execution of the underlying query.
        """

        try:
            return list(self)[0]
        except IndexError:
            return None

    def one(self, except_all=False):
        """
        Return exactly one result or raise an exception.


        Raises `couchdbkit.exceptions.MultipleResultsFound` if multiple rows are returned.
        If except_all is True, raises `couchdbkit.exceptions.NoResultFound`
        if the query selects no rows.

        This results in an execution of the underlying query.
        """

        length = len(self)
        if length > 1:
            raise MultipleResultsFound("%s results found." % length)

        result = self.first()
        if result is None and except_all:
            raise NoResultFound
        return result

    def all(self):
        """ return list of all results """
        return list(self.iterator())

    def count(self):
        """ return number of returned results """
        self._fetch_if_needed()
        return len(self._result_cache.get('rows', []))

    def fetch(self):
        """ fetch results and cache them """
        # reset dynamic keys
        for key in  self._dynamic_keys:
            try:
                delattr(self, key)
            except:
                pass
        self._dynamic_keys = []

        self._result_cache = self.fetch_raw().json_body
        assert isinstance(self._result_cache, dict), 'received an invalid ' \
            'response of type %s: %s' % \
            (type(self._result_cache), repr(self._result_cache))
        self._total_rows = self._result_cache.get('total_rows')
        self._offset = self._result_cache.get('offset', 0)

        # add key in view results that could be added by an external
        # like couchdb-lucene
        for key in self._result_cache.keys():
            if key not in ["total_rows", "offset", "rows"]:
                self._dynamic_keys.append(key)
                setattr(self, key, self._result_cache[key])


    def fetch_raw(self):
        """ retrive the raw result """
        return self._fetch(self._arg, self.params)

    def _fetch_if_needed(self):
        if not self._result_cache:
            self.fetch()

    @property
    def total_rows(self):
        """ return number of total rows in the view """
        self._fetch_if_needed()
        # reduce case, count number of lines
        if self._total_rows is None:
            return self.count()
        return self._total_rows

    @property
    def offset(self):
        """ current position in the view """
        self._fetch_if_needed()
        return self._offset

    def __getitem__(self, key):
        params = self.params.copy()
        if type(key) is slice:
            if key.start is not None:
                params['startkey'] = key.start
            if key.stop is not None:
                params['endkey'] = key.stop
        elif isinstance(key, (list, tuple,)):
            params['keys'] = key
        else:
            params['key'] = key

        return ViewResults(self._fetch, self._arg, wrapper=self.wrapper, params=params, schema=None)

    def __call__(self, **newparams):
        return ViewResults(
            self._fetch, self._arg,
            wrapper=self.wrapper,
            params=dict(self.params, **newparams),
            schema=None,
        )

    def __iter__(self):
        return self.iterator()

    def __len__(self):
        return self.count()

    def __nonzero__(self):
        return bool(len(self))
