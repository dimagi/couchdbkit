from __future__ import absolute_import
from __future__ import unicode_literals
from django.conf.urls.defaults import *

urlpatterns = patterns('',
    url(r'^$', 'djangoapp.greeting.views.home'),
)

