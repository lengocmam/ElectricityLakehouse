import os

SECRET_KEY = os.getenv("SUPERSET_SECRET_KEY", "change-me")
SQLALCHEMY_DATABASE_URI = os.getenv(
    "SUPERSET_DB_URI",
    "postgresql+psycopg2://superset:superset@postgres:5432/superset",
)

TALISMAN_ENABLED = False
WTF_CSRF_ENABLED = True

CACHE_CONFIG = {
    "CACHE_TYPE": "SimpleCache",
    "CACHE_DEFAULT_TIMEOUT": 300,
    "CACHE_KEY_PREFIX": "superset_",
}
DATA_CACHE_CONFIG = CACHE_CONFIG

FEATURE_FLAGS = {
    "ENABLE_TEMPLATE_PROCESSING": True,
}
