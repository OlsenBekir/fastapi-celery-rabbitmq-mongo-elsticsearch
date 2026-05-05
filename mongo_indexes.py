from database import messages_collection, jobs_collection

def ensure_mongo_indexes():
    messages_collection.create_index("task_id")
    messages_collection.create_index("created_at")
    jobs_collection.create_index("task_id", unique=True)
    jobs_collection.create_index("status")
