# -*- coding: utf-8 -*-
#
# This file is part of couchdbkit released under the MIT license. 
# See the NOTICE for more information.

"""
Macros used by loaders. compatible with couchapp. It allow you to include code,
design docs members inside your views, shows and lists.

for example to include a code, add just after first function the line :
    
    // !code relativepath/to/some/code.js
    
To include a member of design doc and use it as a simple javascript object :

    // !json some.member.i.want.to.include
    
All in one, example of a view :

    function(doc) {
        !code _attachments/js/md5.js
        
        if (doc.type == "user") {
            doc.gravatar = hex_md5(user.email);
            emit(doc.username, doc)
        }
    }

This example includes md5.js code and uses md5 function to create gravatar hash
in your views.  So you could just store raw data in your docs and calculate
hash when views are updated.

"""
from __future__ import absolute_import
import glob
from hashlib import md5
import logging
import os
import re

from ..exceptions import MacroError
from ..utils import read_file, read_json, to_bytestring, json
import six

logger = logging.getLogger(__name__)


def package_shows(doc, funcs, app_dir, objs):
   apply_lib(doc, funcs, app_dir, objs)
         
def package_views(doc, views, app_dir, objs):
   for view, funcs in six.iteritems(views):
       if hasattr(funcs, "items"):
           apply_lib(doc, funcs, app_dir, objs)

def apply_lib(doc, funcs, app_dir, objs):
    for k, v in funcs.items():
        if not isinstance(v, six.string_types):
            continue
        else:
            logger.debug("process function: %s" % k)
            old_v = v
            try:
                funcs[k] = run_json_macros(doc, 
                    run_code_macros(v, app_dir), app_dir)
            except ValueError as e:
                raise MacroError(
                "Error running !code or !json on function \"%s\": %s" % (k, e))
            if old_v != funcs[k]:
                objs[md5(to_bytestring(funcs[k])).hexdigest()] = old_v
           
def run_code_macros(f_string, app_dir):
   def rreq(mo):
       # just read the file and return it
       path = os.path.join(app_dir, mo.group(2).strip())
       library = ''
       filenum = 0
       for filename in glob.iglob(path):            
           logger.debug("process code macro: %s" % filename)
           try:
               cnt = read_file(filename)
               if cnt.find("!code") >= 0:
                   cnt = run_code_macros(cnt, app_dir)
               library += cnt
           except IOError as e:
               raise MacroError(str(e))
           filenum += 1
           
       if not filenum:
           raise MacroError(
           "Processing code: No file matching '%s'" % mo.group(2))
       return library

   re_code = re.compile('(\/\/|#)\ ?!code (.*)')
   return re_code.sub(rreq, f_string)

def run_json_macros(doc, f_string, app_dir):
   included = {}
   varstrings = []

   def rjson(mo):
       if mo.group(2).startswith('_attachments'): 
           # someone  want to include from attachments
           path = os.path.join(app_dir, mo.group(2).strip())
           filenum = 0
           for filename in glob.iglob(path):
               logger.debug("process json macro: %s" % filename)
               library = ''
               try:
                   if filename.endswith('.json'):
                       library = read_json(filename)
                   else:
                       library = read_file(filename)
               except IOError as e:
                   raise MacroError(str(e))
               filenum += 1
               current_file = filename.split(app_dir)[1]
               fields = current_file.split('/')
               count = len(fields)
               include_to = included
               for i, field in enumerate(fields):
                   if i+1 < count:
                       include_to[field] = {}
                       include_to = include_to[field]
                   else:
                       include_to[field] = library
           if not filenum:
               raise MacroError(
               "Processing code: No file matching '%s'" % mo.group(2))
       else:	
           logger.debug("process json macro: %s" % mo.group(2))
           fields = mo.group(2).strip().split('.')
           library = doc
           count = len(fields)
           include_to = included
           for i, field in enumerate(fields):
               if not field in library:
                   logger.warning(
                   "process json macro: unknown json source: %s" % mo.group(2))
                   break
               library = library[field]
               if i+1 < count:
                   include_to[field] = include_to.get(field, {})
                   include_to = include_to[field]
               else:
                   include_to[field] = library

       return f_string

   def rjson2(mo):
       return '\n'.join(varstrings)

   re_json = re.compile('(\/\/|#)\ ?!json (.*)')
   re_json.sub(rjson, f_string)

   if not included:
       return f_string

   for k, v in six.iteritems(included):
       varstrings.append("var %s = %s;" % (k, json.dumps(v).encode('utf-8')))

   return re_json.sub(rjson2, f_string)
