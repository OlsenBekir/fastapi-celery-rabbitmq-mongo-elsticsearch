import os

MONGO_URL = os.getenv("MONGO_URL", "mongodb://mongo-db:27017")
CELERY_BROKER_URL = os.getenv(
    "CELERY_BROKER_URL",
    "pyamqp://guest:guest@rabbitmq:5672//"
)
ELASTICSEARCH_URL = os.getenv(
    "ELASTICSEARCH_URL",
    "http://elasticsearch:9200"
)

MONGO_DB_NAME = "mydb"
MESSAGES_COLLECTION = "messages"
JOBS_COLLECTION = "jobs"

ELASTICSEARCH_INDEX = "messages"
