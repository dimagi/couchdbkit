from __future__ import absolute_import
from django.apps import apps
from django.core.management.base import BaseCommand
from couchdbkit.ext.django.loading import couchdbkit_handler

class Command(BaseCommand):
    help = 'Copy temporary design docs over existing ones'

    def handle(self, *args, **options):
        for app in apps.get_apps():
            couchdbkit_handler.copy_designs(app, temp='tmp', verbosity=2)
