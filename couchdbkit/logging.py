from __future__ import absolute_import
import logging
from timeit import default_timer

LOG_LEVELS = {
    "critical": logging.CRITICAL,
    "error": logging.ERROR,
    "warning": logging.WARNING,
    "info": logging.INFO,
    "debug": logging.DEBUG
}


logger = logging.getLogger('couchdbkit')
request_logger = logging.getLogger('couchdbkit.request')
error_logger = logging.getLogger('couchdbkit.error')


def set_logging(level, handler=None):
    """
    Set level of logging, and choose where to display/save logs
    (file or standard output).
    """
    if not handler:
        handler = logging.StreamHandler()

    loglevel = LOG_LEVELS.get(level, logging.INFO)
    logger.setLevel(loglevel)
    format = r"%(asctime)s [%(process)d] [%(levelname)s] %(message)s"
    datefmt = r"%Y-%m-%d %H:%M:%S"

    handler.setFormatter(logging.Formatter(format, datefmt))
    logger.addHandler(handler)


def install_request_logger():
    """Install request logger

    Request metrics are logged to `couchdbkit.request` logger. Extra
    metrics added to the log record:

    - method
    - database
    - path
    - status_code
    - content_length
    - duration

    Returns a function that uninstalls the request logger when called.
    """
    from cloudant._client_session import ClientSession
    from requests.exceptions import HTTPError

    def request(self, method, url, **kwargs):
        start = default_timer()
        status_code = None
        content_length = None
        try:
            resp = real_request(self, method, url, **kwargs)
            status_code = resp.status_code
            content_length = resp.headers.get("content-length")
        except HTTPError as err:
            if err.response is not None:
                status_code = err.response.status_code
            raise
        finally:
            url_parts = url.split('/', 4)
            len_parts = len(url_parts)
            if len_parts == 5:
                database, path = url_parts[3:]
            elif len_parts == 4:
                database = url_parts[3]
                path = '/'
            else:
                database = '<unknown>'
                path = '<n/a>'
            info = {
                "method": method,
                "database": database,
                "path": path,
                "status_code": status_code,
                "content_length": content_length,
                "duration": default_timer() - start,
            }
            request_logger.debug(
                '%(method)s to %(database)s/%(path)s took %(duration)s',
                info,
                extra=info,
            )
        return resp

    def uninstall():
        # this is mainly for tests, so they can do proper cleanup
        ClientSession.request = real_request

    real_request = ClientSession.request
    ClientSession.request = request

    return uninstall
