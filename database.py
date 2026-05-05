from pymongo import MongoClient
from config import MONGO_URL, MONGO_DB_NAME, MESSAGES_COLLECTION, JOBS_COLLECTION

client = MongoClient(MONGO_URL)
db = client[MONGO_DB_NAME]

messages_collection = db[MESSAGES_COLLECTION]
jobs_collection = db[JOBS_COLLECTION]
