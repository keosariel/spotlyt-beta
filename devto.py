import requests
import json
import pprint
from sqlitedict import SqliteDict
from datetime import datetime
from backports.datetime_fromisoformat import MonkeyPatch
MonkeyPatch.patch_fromisoformat()

cache = SqliteDict("dev.to", autocommit=True)
host = "http://34.125.94.232"
collection = "devto"

def set_schema(collection, schema):
    res = requests.post(
        f"{host}/api/indexes/{collection}/schema",
        json=schema
    )

    print(res.text)

def get_posts(page=1, pagesize=200, username=None):
    if username:
        res = requests.get(f"https://dev.to/api/articles?page={page}&per_page={pagesize}&username={username}")
    else:
        res = requests.get(f"https://dev.to/api/articles?page={page}&per_page={pagesize}")

    try:
        data = json.loads(res.text)
        batch = []
        usernames = cache.get("usernames", {})
        fields = ["type_of", "id", "title", "description",
                "readable_publish_date", "url", "comments_count", "public_reactions_count", "collection_id",
                "published_timestamp", "social_image", "created_at", "published_at", "tag_list",
                "cover_image", "user"]

        for d in data:
            _data = {field:d.get(field) for field in fields}
            date = datetime.fromisoformat(_data["published_timestamp"][:-1])
            _data["published_timestamp"] = date.strftime("%Y-%m-%d")
            user = d["user"]["username"]
            if user not in usernames:
                usernames[user] = {}

            batch.append(_data)

        cache["usernames"] = usernames

        return batch
    except json.JSONDecodeError as err:
        return

def get_pages(n=10, field="article", username=None):

    if username and field=="user":
        pages = cache["usernames"].get(username, None)
    else:
        pages = cache.get(field, {})

    max_page = 0
    if pages:
        max_page = max([int(k) for k in pages])

    return [max_page + i for i in range(1, n)]

def add_data(page, batch, username=None):
    usernames = cache.get("usernames", {})

    if username:
        pages = usernames.get(username, {})
    else:
        pages = cache.get(field, {})

    if not batch:
        print("batch empty")
        return None

    res = requests.post(f"{host}/api/indexes/{collection}/objects", json=batch)
    try:
        jres = json.loads(res.text)
        pages[page] = {
            "status": "pending",
            "task_id": jres["taskId"],
            "page": page,
            "count": len(batch)
        }

        if username:
            usernames[username] = pages
            cache["usernames"] = usernames
        else:
            cache[field] = pages
        print(jres, " => ", username)
    except json.JSONDecodeError as err:
        print(err, " => add_data", res.text)
        pass

def get_username():
    usernames = cache.get("usernames", {})
    names = sorted(usernames.keys())
    u = 0
    
    for name in names:
        u += 1
        pages = usernames[name]
        if pages:
            max_page = max([v for k, v in pages.items()], key=lambda x:x["page"])
            if max_page["count"] >= 200:
                print(f"users: {u} => total users: {len(usernames)}")
                return name
        else:
            print(f"users: {u} => total users: {len(usernames)}")
            return name

def do_posts(field):
    username = None
    n = 5
    if field == "user":
        username = get_username()
        npages = get_pages(n=n, field="user", username=username)
    else:
        npages = get_pages(n=n, field=field)

    print("USER ==> ", username)

    for p in npages:
        batch = get_posts(p, username=username)

        add_data(p, batch, username)

    if username:
        print(cache.get("usernames", {}).get(username))

schema = [
    {"name": "id", "type": "id"},
    {"name": "title", "type": "text"},
    {"name": "description", "type": "text"},
    {"name": "public_reactions_count", "type": "number"},
    {"name": "comments_count", "type": "number"},
    {"name": "tag_list", "type": "text", "index": False, "facet": True},
    {"name": "published_timestamp", "type": "date"}
]

if __name__ == "__main__":
    import sys

    args = sys.argv
    if len(args) < 2:
        raise ValueError("Field type needed in arguments")

    field = args[1]

    set_schema(collection, schema)
    do_posts(field=field)

    # print(cache.get(field))
