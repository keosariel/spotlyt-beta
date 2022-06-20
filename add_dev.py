import json
import pprint 
import requests
import threading
from datetime import datetime
import time

with open("dev.json", "r") as f:
    data = json.loads(f.read())
    
host = "http://127.0.0.1:5000"
collection = "devposts"

def set_schema(collection, schema):
    res = requests.post(
        f"{host}/api/indexes/{collection}/schema",
        json=schema
    )

    print(res.text)

def add_objects(collection, data):

    i = 0
    j = len(data)
    for i in range(i, i+j, 100):
        batch = []

        for ba in data[i:i+100]:
            t = ""
            if ba["published"]:
                try:
                    t = datetime.fromisoformat(ba["published"]).strftime("%Y-%m-%d")
                except ValueError as e:
                    print("e >>> ", ba["published"])
            
            ba["published"] = t
            batch.append(ba)
            
        threading.Thread(target=requests.post, kwargs=dict(
            url=f"{host}/api/indexes/{collection}/objects",
            json=batch
        )).start()

        print(i)

        if i % 1000 == 0 and i != 0:
            time.sleep(5)

schema = [
    {"name": "title", "type": "text"},
    {"name": "summary", "type": "text"},
    {"name": "site", "type": "text", "index": False, "facet": True},
    {"name": "id", "type": "id"},
    {"name": "published", "type": "date"},
    {"name": "tags", "type": "text", "index": False, "facet": True}
]

set_schema(collection, schema)
add_objects(collection, data)
