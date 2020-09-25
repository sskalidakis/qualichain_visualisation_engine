
#!/usr/bin/env bash

python manage.py collectstatic --noinput --settings=${SETTINGS}

echo "Run Gunicorn"
#python manage.py runserver 0:${PORT} --settings=${SETTINGS}
gunicorn --workers=4 -b 0.0.0.0:${PORT} visualisation_engine.wsgi:application
