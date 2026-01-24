from fastapi import FastAPI,Query, HTTPException
from pydantic import HttpUrl
import os
import redis
import json
from motor.motor_asyncio import AsyncIOMotorClient

app = FastAPI(title="Distributed Crawler API")

# Connect to Redis
# In production, use environment variables for host/port
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")

def get_mongo_url():
    user = os.getenv("MONGO_USER")
    password = os.getenv("MONGO_PASS")
    host = os.getenv("MONGO_HOST")
    db_name = os.getenv("MONGO_DB", "crawler_db")

    # If using Atlas (Cloud), we use the srv protocol
    if "mongodb.net" in host:
        return f"mongodb+srv://{user}:{password}@{host}/{db_name}?retryWrites=true&w=majority"

    # Fallback for local Docker
    return f"mongodb://{user}:{password}@{host}:27017/{db_name}?authSource=admin"


mongo_client = AsyncIOMotorClient(get_mongo_url())


redis_client = redis.Redis(
    host=REDIS_HOST,
    port=6379,
    db=0,
    decode_responses=True
)
db = mongo_client.crawler_db

@app.post("/crawl")
async def queue_url(url: HttpUrl):
    url_str = str(url)

    # Check if we've already crawled this recently
    if redis_client.sismember("visited_urls", url_str):
        return {"message": "URL already processed or in progress"}

    # Push to the 'frontier' queue
    redis_client.lpush("url_queue", url_str)
    return {"status": "queued", "url": url_str}


@app.get("/stats")
async def get_stats():
    return {
        "queue_size": redis_client.llen("url_queue"),
        "visited_count": redis_client.scard("visited_urls"),
        "errors_count": redis_client.get("crawl_errors") or 0
    }


@app.get("/system-health")
async def system_health():
    clients = redis_client.client_list()
    worker_count = len(clients) - 1

    return {
        "active_worker_connections": max(0, worker_count),
        "queue_depth": redis_client.llen("url_queue"),
        "details": clients
    }
@app.on_event("startup")
async def create_indexes():
    # This ensures the 'content' field is indexed for text searching
    await db.pages_metadata.create_index([("content", "text")])
    print("✅ Text index created on 'content' field")


@app.get("/search")
async def search_pages(q: str = Query(..., min_length=1, description="The word to search for")):
    cursor = db.pages_metadata.find(
        {"$text": {"$search": q}},
        {"url": 1, "title": 1, "_id": 0}
    )

    results = await cursor.to_list(length=100)

    return {
        "query": q,
        "count": len(results),
        "urls": [doc["url"] for doc in results],
        "details": results
    }