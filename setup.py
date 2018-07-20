# -*- coding: utf-8 -
#
# This file is part of couchdbkit released under the MIT license.
# See the NOTICE for more information.

from __future__ import absolute_import
from imp import load_source
import os
import sys

if not hasattr(sys, 'version_info') or sys.version_info < (2, 7, 0, 'final'):
    raise SystemExit("couchdbkit requires Python 2.7 or later.")

from setuptools import setup, find_packages

# open version module
version = load_source("version", os.path.join("couchdbkit",
        "version.py"))


setup(
    name = 'jsonobject-couchdbkit',
    version = version.__version__,

    description = 'Python couchdb kit',
    long_description = open(
        os.path.join(
            os.path.dirname(__file__),
            'README.rst'
        ), 'rt'
    ).read(),
    author = 'Benoit Chesneau',
    author_email = 'benoitc@e-engura.com',
    license = 'Apache License 2',
    url = 'http://couchdbkit.org',

    classifiers = [
        'Development Status :: 4 - Beta',
        'Environment :: Other Environment',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Topic :: Database',
        'Topic :: Utilities',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
    packages = find_packages(exclude=['tests']),

    zip_safe = False,

    install_requires = [
        'restkit>=4.2.2',
        'jsonobject>=0.9.1',
        'cloudant==2.7.0',
        'six==1.11.0',
    ],
    provides=['couchdbkit'],
    obsoletes=['couchdbkit'],
    entry_points="""
    [couchdbkit.consumers]
    sync=couchdbkit.consumer.sync:SyncConsumer
    eventlet=couchdbkit.consumer.ceventlet:EventletConsumer
    gevent=couchdbkit.consumer.cgevent:GeventConsumer
    """,

    test_suite='nose.collector',
)
