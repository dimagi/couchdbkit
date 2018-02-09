from __future__ import absolute_import
from __future__ import unicode_literals
from django.apps import apps
from django.core.management.base import BaseCommand
from couchdbkit.ext.django.loading import couchdbkit_handler

class Command(BaseCommand):
    help = 'Sync design docs to temporary ids'

    def handle(self, *args, **options):
        for app in apps.get_apps():
            couchdbkit_handler.sync(app, verbosity=2, temp='tmp')
