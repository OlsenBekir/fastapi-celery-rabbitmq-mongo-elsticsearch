from celery import Celery
from config import CELERY_BROKER_URL

celery_app = Celery(
    "tasks",
    broker=CELERY_BROKER_URL
)

celery_app.conf.task_routes = {
    "tasks.save_message_async": {"queue": "celery"},
    "tasks.reindex_message": {"queue": "indexing"},
    "tasks.reindex_all_messages": {"queue": "indexing"},
    "tasks.reindex_all_messages_bulk": {"queue": "indexing"},
}
