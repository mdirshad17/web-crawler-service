import redis
import asyncio
import aiohttp
from bs4 import BeautifulSoup
import time
import os

from motor.motor_asyncio import AsyncIOMotorClient


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
collection = db.pages_metadata



async def crawl():
    print("Worker started. Waiting for URLs...")

    async with aiohttp.ClientSession() as session:
        while True:
            # 1. Grab a URL from the queue (Blocking Pop)
            # This waits until a URL is available
            _, url = redis_client.brpop("url_queue")

            # 2. Check if another worker got to it first (Race condition protection)
            if redis_client.sismember("visited_urls", url):
                continue
            redis_client.sadd("visited_urls", url)
            print(f"[*] Crawling: {url}")
            # redis_client.incr("visited_urls");
            try:
                async with session.get(url, timeout=10) as response:
                    if response.status == 200:
                        html = await response.text()
                        soup = BeautifulSoup(html, 'html.parser')
                        for script_or_style in soup(["script", "style", "header", "footer", "nav"]):
                            script_or_style.decompose()
                        clean_text = soup.get_text(separator=' ', strip=True)
                        clean_text = " ".join(clean_text.split())
                        final_content = clean_text[:2000]

                        title = soup.title.string if soup.title else "No Title"
                        await save_to_mongo(url, title,final_content)

                        for link in soup.find_all('a', href=True):
                            new_url = link['href']
                            if new_url.startswith('http'):
                                # Only add if not visited to prevent infinite loops
                                if not redis_client.sismember("visited_urls", new_url):
                                    redis_client.lpush("url_queue", new_url)

                    else:
                        redis_client.incr("crawl_errors")
            except Exception as e:
                print(f"[!] Error crawling {url}: {e}")
                redis_client.incr("crawl_errors")

            # Politeness delay to avoid hitting servers too hard
            await asyncio.sleep(1)

async def save_to_mongo(url, title, content):
    document = {
        "url": url,
        "title": title,
        "content": content,
        "timestamp": asyncio.get_event_loop().time()
    }

    await collection.update_one(
        {"url": url},
        {"$set": document},
        upsert=True
    )
    print(f"💾 Saved to Mongo: {url}")

if __name__ == "__main__":
    asyncio.run(crawl())