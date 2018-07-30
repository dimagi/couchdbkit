# -*- coding: utf-8 -
#
# This file is part of couchdbkit released under the MIT license.
# See the NOTICE for more information.

from .version import version_info, __version__

from .resource import  RequestFailed, CouchdbResource
from .exceptions import InvalidAttachment, DuplicatePropertyError,\
BadValueError, MultipleResultsFound, NoResultFound, ReservedWordError,\
DocsPathNotFound, BulkSaveError, ResourceNotFound, ResourceConflict, \
PreconditionFailed

from .client import Server, Database, ViewResults
from .changes import ChangesStream
from .consumer import Consumer
from .designer import document, push, pushdocs, pushapps, clone
from .external import External

from .schema import (
    Property, IntegerProperty, DecimalProperty, BooleanProperty, FloatProperty, StringProperty,
    DateTimeProperty, DateProperty, TimeProperty,
    dict_to_json, dict_to_json, dict_to_json,
    dict_to_python,
    DocumentSchema, DocumentBase, Document, StaticDocument, contain,
    QueryMixin, AttachmentMixin,
    SchemaProperty, SchemaListProperty, SchemaDictProperty,
    ListProperty, DictProperty, StringDictProperty, StringListProperty, SetProperty
)

from .logging import (LOG_LEVELS, set_logging, logger)
