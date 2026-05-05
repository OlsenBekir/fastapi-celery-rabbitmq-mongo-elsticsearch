from elasticsearch import Elasticsearch
from config import ELASTICSEARCH_URL, ELASTICSEARCH_INDEX

es = Elasticsearch(ELASTICSEARCH_URL)

def ensure_index():
    try:
        if es.indices.exists(index=ELASTICSEARCH_INDEX):
            return

        es.indices.create(
            index=ELASTICSEARCH_INDEX,
            mappings={
                "properties": {
                    "mongo_id": {"type": "keyword"},
                    "task_id": {"type": "keyword"},
                    "melding": {"type": "text"},
                    "status": {"type": "keyword"},
                    "created_at": {"type": "date"},
                    "indexed_at": {"type": "date"}
                }
            }
        )

    except Exception as e:
        print("⚠️ Elasticsearch hazır değil, index oluşturulamadı:", e)