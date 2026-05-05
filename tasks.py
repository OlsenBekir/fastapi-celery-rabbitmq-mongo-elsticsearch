from datetime import datetime, timezone
from bson import ObjectId
from celery_app import celery_app
from database import messages_collection, jobs_collection
from search import es
from config import ELASTICSEARCH_INDEX

def utcnow():
    return datetime.now(timezone.utc)

# @celery_app.task(bind=True, name="tasks.save_message_async")
# def save_message_async(self, melding: str):
    task_id = self.request.id

    jobs_collection.insert_one({
        "task_id": task_id,
        "type": "save_message",
        "status": "processing",
        "created_at": utcnow(),
        "updated_at": utcnow()
    })

    try:
        doc = {
            "task_id": task_id,
            "melding": melding,
            "status": "saved",
            "created_at": utcnow()
        }

        result = messages_collection.insert_one(doc)
        mongo_id = str(result.inserted_id)

        es.index(
            index=ELASTICSEARCH_INDEX,
            id=mongo_id,
            document={
                "mongo_id": mongo_id,
                "task_id": task_id,
                "melding": melding,
                "status": "saved",
                "created_at": doc["created_at"],
                "indexed_at": utcnow()
            }
        )

        jobs_collection.update_one(
            {"task_id": task_id},
            {
                "$set": {
                    "status": "completed",
                    "mongo_id": mongo_id,
                    "updated_at": utcnow()
                }
            }
        )

        return mongo_id

    except Exception as e:
        jobs_collection.update_one(
            {"task_id": task_id},
            {
                "$set": {
                    "status": "failed",
                    "error": str(e),
                    "updated_at": utcnow()
                }
            }
        )
        raise


    # ///////////////////////////////////////////


@celery_app.task(bind=True, name="tasks.reindex_message")
def reindex_message(self, mongo_id: str):
    doc = messages_collection.find_one({"_id": ObjectId(mongo_id)})

    if not doc:
        raise ValueError(f"Message not found: {mongo_id}")

    es.index(
        index=ELASTICSEARCH_INDEX,
        id=mongo_id,
        document={
            "mongo_id": mongo_id,
            "task_id": doc.get("task_id"),
            "melding": doc.get("melding"),
            "status": doc.get("status"),
            "created_at": doc.get("created_at"),
            "indexed_at": utcnow()
        }
    )

    return mongo_id


# /////////////////////////////



@celery_app.task(bind=True, name="tasks.reindex_all_messages")
def reindex_all_messages(self):
    count = 0

    for doc in messages_collection.find({}):
        mongo_id = str(doc["_id"])

        es.index(
            index=ELASTICSEARCH_INDEX,
            id=mongo_id,
            document={
                "mongo_id": mongo_id,
                "task_id": doc.get("task_id"),
                "melding": doc.get("melding"),
                "status": doc.get("status"),
                "created_at": doc.get("created_at"),
                "indexed_at": utcnow()
            }
        )

        count += 1

    return {"indexed": count}


# //////////////////////////////






# /////////////////////






from elasticsearch.helpers import bulk

@celery_app.task(bind=True, name="tasks.reindex_all_messages_bulk")
def reindex_all_messages_bulk(self):
    actions = []

    for doc in messages_collection.find({}):
        mongo_id = str(doc["_id"])

        actions.append({
            "_index": ELASTICSEARCH_INDEX,
            "_id": mongo_id,
            "_source": {
                "mongo_id": mongo_id,
                "task_id": doc.get("task_id"),
                "melding": doc.get("melding"),
                "status": doc.get("status"),
                "created_at": doc.get("created_at"),
                "indexed_at": utcnow()
            }
        })

    if not actions:
        return {"indexed": 0}

    success, errors = bulk(es, actions, raise_on_error=False)

    return {
        "indexed": success,
        "errors": errors
    }


# /////////////////////////   


# @celery_app.task(
#     bind=True,
#     name="tasks.save_message_async",
#     autoretry_for=(Exception,),
#     retry_backoff=True,
#     retry_kwargs={"max_retries": 3}
# )
# def save_message_async(self, melding: str):
#     task_id = self.request.id

#     messages_collection.update_one(
#         {"task_id": task_id},
#         {
#             "$setOnInsert": {
#                 "task_id": task_id,
#                 "melding": melding,
#                 "created_at": utcnow()
#             },
#             "$set": {
#                 "status": "processing",
#                 "updated_at": utcnow()
#             }
#         },
#         upsert=True
#     )


# ////////////////////////



@celery_app.task(
    bind=True,
    name="tasks.save_message_async",
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_kwargs={"max_retries": 3}
)
def save_message_async(self, melding: str):
    task_id = self.request.id
    now = utcnow()

    jobs_collection.update_one(
        {"task_id": task_id},
        {
            "$setOnInsert": {
                "task_id": task_id,
                "type": "save_message",
                "created_at": now
            },
            "$set": {
                "status": "processing",
                "updated_at": now
            }
        },
        upsert=True
    )

    messages_collection.update_one(
        {"task_id": task_id},
        {
            "$setOnInsert": {
                "task_id": task_id,
                "melding": melding,
                "created_at": now
            },
            "$set": {
                "status": "saved",
                "updated_at": now
            }
        },
        upsert=True
    )

    doc = messages_collection.find_one({"task_id": task_id})
    mongo_id = str(doc["_id"])

    es.index(
        index=ELASTICSEARCH_INDEX,
        id=mongo_id,
        document={
            "mongo_id": mongo_id,
            "task_id": task_id,
            "melding": doc["melding"],
            "status": doc["status"],
            "created_at": doc["created_at"],
            "indexed_at": utcnow()
        }
    )

    jobs_collection.update_one(
        {"task_id": task_id},
        {
            "$set": {
                "status": "completed",
                "mongo_id": mongo_id,
                "updated_at": utcnow()
            }
        }
    )

    return mongo_id