from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from bson import ObjectId
from bson.errors import InvalidId

from tasks import save_message_async
from database import messages_collection, jobs_collection
from mongo_indexes import ensure_mongo_indexes
from search import ensure_index, es
from config import ELASTICSEARCH_INDEX
from tasks import save_message_async, reindex_all_messages


from tasks import reindex_all_messages_bulk

app = FastAPI(title="FastAPI + Celery + MongoDB + Elasticsearch")

class Melding(BaseModel):
    melding: str

@app.on_event("startup")
def startup():
    ensure_mongo_indexes()
    ensure_index()

@app.post("/melding_async")
def create_melding_async(melding: Melding):
    task = save_message_async.delay(melding.melding)
    return {
        "status": "queued",
        "task_id": task.id
    }

@app.get("/jobs/{task_id}")
def get_job(task_id: str):
    job = jobs_collection.find_one({"task_id": task_id})

    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    job["_id"] = str(job["_id"])
    return job


# ///////////////////////////////////////////////////





@app.get("/messages/{mongo_id}")
def get_message(mongo_id: str):
    try:
        object_id = ObjectId(mongo_id)
    except InvalidId:
        raise HTTPException(status_code=400, detail="Invalid Mongo ObjectId")

    doc = messages_collection.find_one({"_id": object_id})

    if not doc:
        raise HTTPException(status_code=404, detail="Message not found")

    doc["_id"] = str(doc["_id"])
    return doc


@app.get("/search")
def search_messages(q: str):
    response = es.search(
        index=ELASTICSEARCH_INDEX,
        query={
            "match": {
                "melding": q
            }
        },
        sort=[
            {"created_at": {"order": "desc"}}
        ]
    )

    hits = []
    for hit in response["hits"]["hits"]:
        hits.append({
            "id": hit["_id"],
            "score": hit["_score"],
            "source": hit["_source"]
        })

    return {
        "query": q,
        "count": len(hits),
        "results": hits
    }

# ////////////////////////

@app.get("/search/highlight")
def search_messages_highlight(q: str):
    response = es.search(
        index=ELASTICSEARCH_INDEX,
        query={
            "match": {
                "melding": q
            }
        },
        highlight={
            "fields": {
                "melding": {}
            }
        }
    )

    hits = []
    for hit in response["hits"]["hits"]:
        hits.append({
            "id": hit["_id"],
            "score": hit["_score"],
            "melding": hit["_source"]["melding"],
            "highlight": hit.get("highlight", {})
        })

    return {
        "query": q,
        "results": hits
    }

# //////////////////////////////



@app.get("/search/filter")
def search_messages_filter(q: str, status: str = "saved"):
    response = es.search(
        index=ELASTICSEARCH_INDEX,
        query={
            "bool": {
                "must": [
                    {"match": {"melding": q}}
                ],
                "filter": [
                    {"term": {"status": status}}
                ]
            }
        },
        sort=[
            {"created_at": {"order": "desc"}}
        ]
    )

    return {
        "query": q,
        "status": status,
        "results": [
            {
                "id": hit["_id"],
                "score": hit["_score"],
                "source": hit["_source"]
            }
            for hit in response["hits"]["hits"]
        ]
    }



# //////////////////////////////////////




@app.post("/admin/reindex-bulk")
def reindex_bulk():
    task = reindex_all_messages_bulk.delay()
    return {
        "status": "queued",
        "task_id": task.id
    }


# ////////






@app.post("/admin/reindex")
def reindex():
    task = reindex_all_messages.delay()
    return {
        "status": "queued",
        "task_id": task.id
    }