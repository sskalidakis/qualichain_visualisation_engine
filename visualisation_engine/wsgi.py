"""
WSGI config for visualisation_engine project.

It exposes the WSGI callable as a module-level variable named ``application``.

For more information on this file, see
https://docs.djangoproject.com/en/3.1/howto/deployment/wsgi/
"""

import os,sys
import environ

from django.core.wsgi import get_wsgi_application

env = environ.Env()
if "SETTINGS" not in env:
    settings_file = "visualisation_engine.settings.dev"
else:
    environ.Env.read_env()
    settings_file = env("SETTINGS")

os.environ.setdefault('DJANGO_SETTINGS_MODULE', settings_file)
sys.path.append('/opt/visualisation_engine')

application = get_wsgi_application()
