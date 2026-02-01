from pymongo import MongoClient

def get_mongo_collection():
    client = MongoClient("mongodb://localhost:27017/")
    db = client["sentiment_db"]
    collection = db["tweets"]
    return collection
