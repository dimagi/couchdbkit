from __future__ import absolute_import
from __future__ import unicode_literals

import logging
import re
from io import StringIO
from unittest import TestCase


import couchdbkit.logging as mod
from couchdbkit import Server


class TestLogging(TestCase):

    @classmethod
    def setUpClass(cls):
        super(TestLogging, cls).setUpClass()
        cls.server = Server()
        cls.db = cls.server.create_db('couchdbkit_test')
        doc = {"name": "Yesterday's Heroes"}
        cls.db.save_doc(doc)
        cls.doc_id = doc["_id"]
        cls.log = CaptureLogOutput(mod.request_logger.name)

    @classmethod
    def tearDownClass(cls):
        super(TestLogging, cls).tearDownClass()
        cls.log.close()
        try:
            del cls.server['couchdbkit_test']
        except Exception:
            pass

    def setUp(self):
        self.log.clear()

    def assertRegex(self, text, regex):
        assert re.search(regex, text), "%r not matched by %r" % (text, regex)

    def test_no_logging_when_not_installed(self):
        self.assertFalse(self.log)
        self.db.get(self.doc_id)
        self.assertFalse(self.log)

    def test_install_request_logger(self):
        self.addCleanup(mod.install_request_logger())
        self.db.get(self.doc_id)
        self.assertRegex(str(self.log),
            r"^GET to couchdbkit_test/{} took \d.\d".format(self.doc_id))

    def test_extra_format(self):
        fmt = "%(status_code)s %(content_length)s %(message)s"
        with CaptureLogOutput(mod.request_logger.name, fmt=fmt) as log:
            self.addCleanup(mod.install_request_logger())
            self.db.get(self.doc_id)
            self.assertRegex(str(log),
                r"^200 \d+ GET to couchdbkit_test/...".format(self.doc_id))


class CaptureLogOutput(object):
    """Capture logging output

    Logging output for the given logger is collected immediately upon
    instantiation until closed.
    """

    def __init__(self, logger_name, level=logging.DEBUG, fmt="%(message)s"):
        self.logger = logging.getLogger(logger_name)
        self.new_level = level
        self.original_level = self.logger.level
        self.original_handlers = list(self.logger.handlers)
        for handler in self.original_handlers:
            self.logger.removeHandler(handler)
        self.output = StringIO()
        stream = logging.StreamHandler(self.output)
        stream.setFormatter(logging.Formatter(fmt))
        self.logger.addHandler(stream)
        self.logger.setLevel(level)

    def clear(self):
        self.output.seek(0)
        self.output.truncate()

    def __str__(self):
        return self.output.getvalue()

    def __repr__(self):
        return repr(str(self))

    def __len__(self):
        return self.output.tell()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def close(self):
        self.logger.setLevel(self.original_level)
        for handler in list(self.logger.handlers):
            self.logger.removeHandler(handler)
        for handler in self.original_handlers:
            self.logger.addHandler(handler)
