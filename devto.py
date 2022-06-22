import requests
import json
import pprint
from sqlitedict import SqliteDict
from datetime import datetime

cache = SqliteDict("dev.to", autocommit=True)
if not cache.get("pages"):
    cache["pages"] = {}

pages = cache["pages"]

"""
get posts
keep track of pages added
check for task state
get queue
add data
"""

host = "http://127.0.0.1:5000"
collection = "devto"

def set_schema(collection, schema):
    res = requests.post(
        f"{host}/api/indexes/{collection}/schema",
        json=schema
    )

    print(res.text)

def get_posts(page=1, pagesize=200):
    res = requests.get(f"https://dev.to/api/articles?page={page}&per_page={pagesize}")
    try:
        data = json.loads(res.text)
        batch = []
        fields = ["type_of", "id", "title", "description", 
                "readable_publish_date", "url", "comments_count", "public_reactions_count", "collection_id",
                "published_timestamp", "social_image", "created_at", "published_at", "tag_list",
                "cover_image", "user"]
        
        for d in data:
            _data = {field:d.get(field) for field in fields}
            date = datetime.fromisoformat(_data["published_timestamp"][:-1])
            _data["published_timestamp"] = date.strftime("%Y-%m-%d")
            batch.append(_data)
        return batch
    except json.JSONDecodeError as err:
        return 

def get_pages(n=10):
    pages = cache["pages"]
    max_page = 0
    if pages:
        max_page = max([int(k) for k in pages])

    return [max_page + i for i in range(1, n)]

def add_data(page, batch):
    pages = cache["pages"]
    res = requests.post(f"{host}/api/indexes/{collection}/objects", json=batch)
    try:
        jres = json.loads(res.text)
        pages[page] = {
            "status": "pending",
            "task_id": jres["taskId"],
            "page": page
        }

        cache["pages"] = pages
        print(jres)
    except json.JSONDecodeError as err:
        print(err)
        pass

def do_posts():
    npages = get_pages(n=5)
    print(npages)

    for p in npages:
        batch = get_posts(p)
        add_data(p, batch)

schema = [
    {"name": "id", "type": "id"},
    {"name": "title", "type": "text"},
    {"name": "description", "type": "text"},
    {"name": "public_reactions_count", "type": "number"},
    {"name": "comments_count", "type": "number"},
    {"name": "tag_list", "type": "text", "index": False, "facet": True},
    {"name": "published_timestamp", "type": "date"}
]

set_schema(collection, schema)
do_posts()
