from __future__ import absolute_import
from django.apps import apps
from django.core.management.base import BaseCommand
from couchdbkit.ext.django.loading import couchdbkit_handler

class Command(BaseCommand):
    help = 'Sync couchdb views.'

    def handle(self, *args, **options):
        for app in apps.get_apps():
            couchdbkit_handler.sync(app, verbosity=2)
