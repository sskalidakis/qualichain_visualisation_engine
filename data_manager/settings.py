import os

# =================================
#   POSTGRES SETTINGS
# =================================
ENGINE_STRING = 'postgresql+psycopg2://{}:{}@{}:{}/{}'.format(
    os.environ.get('POSTGRESS_USER', 'admin'),
    os.environ.get('POSTGRESS_PASSWORD', 'admin'),
    os.environ.get('POSTGRESS_HOST','qualichain.epu.ntua.gr'),
    os.environ.get('POSTGRESS_DB', 5432),
    os.environ.get('POSTGRESS_DB', 'api_db')
)
