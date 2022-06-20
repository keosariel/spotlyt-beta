from blacksheep import Application, Request, Response, json, Content
from blacksheep.server.controllers import Controller, APIController, get, put, post, delete, patch
from blacksheep.server.openapi.v3 import OpenAPIHandler
from openapidocs.v3 import Info
import asyncio
import threading

from spotlyt import Index
import time

__version__ = "0.0.1"

app = Application()

docs = OpenAPIHandler(info=Info(title="Example API", version="0.0.1"))
docs.bind_app(app)

# - handle exceptions

app.use_cors(
    allow_methods="*",
    allow_origins="*",
    allow_headers="*",
    max_age=300
)

def api_error(msg, status, error=True):
    return json({"message": msg, "error": error}, status=status)

def api_success(data, status=200):
    return json(data, status=status)


"""
- delete collection
- delete document
- get logs
- weighting
"""


class Indexes(APIController):

    @get("/")
    async def index_info(database: Index, request: Request):
        """Get index info"""
        return await database.info()
    
    @get("/version")
    def index(self, request):
        """Get index version"""
        return {"version": __version__}

    @post("/:collection_name/objects")
    async def add_object(database: Index, request: Request, collection_name: str):
        """Add object to index"""
        
        data = await request.json()

        if type(data) is dict:
            data = [data]

        elif type(data) is not list:
            return api_error("Invalid data", 400)

        task_id = await database.add_documents(collection_name, data)

        asyncio.create_task(database.index_documents())

        return api_success({"taskId": task_id})

    @get("/tasks/:task_id")
    async def get_task(database: Index, task_id: str):
        """Get task info"""
        
        task = await database.get_task(task_id, doc=False)
        return api_success(task)
    
    @get("/tasks")
    async def get_tasks(database: Index):
        """Get task info"""
        
        tasks = await database.get_tasks(just_id=True)
        return api_success(tasks)

    @post("/:collection_name/search")
    async def search(request: Request, database: Index, collection_name: str):
        """Query index"""
        data = await request.json()
        data = {} if not data else data
        querystring = data.get("query", None)

        if querystring is None:
            return api_error("Missing `query`", 400)
        
        offset = data.get("offset", 0)
        pagesize = data.get("pagesize", 10)
        fields = data.get("fields", "*")
        facets = data.get("facets", None)
        ranges = data.get("ranges", None)
        to_highlight = data.get("highlight", [])

        t0 = time.time()

        results = await database.query(
            querystring=querystring,
            collection_name=collection_name,
            offset=offset,
            pagesize=pagesize,
            fields=fields,
            facets=facets,
            ranges=ranges,
            to_highlight=to_highlight
        )

        t1 = time.time()
        t = "{:.3f}ms".format((t1-t0) * 1000)

        results["_spotlyt_query_time"] = t

        return results
    
    @get("/:collection_name/objects/:object_id")
    async def get_object(database: Index, collection_name: str, object_id: str):
        """Get object by ID"""
        
        object = await database.get_document(collection_name, object_id)

        return []
    
    @delete("/:collection_name/objects")
    async def delete_object(request: Request, database: Index, collection_name: str):
        """Get object by ID"""

        data = await request.json()

        if not isinstance(data, list):
            return api_error("Invalid data", 400)
        
        task_id = await database.delete_documents(collection_name, data)
        
        asyncio.create_task(database.index_documents())

        return api_success({"taskId": task_id})
    
    @post("/:collection_name/settings")
    async def set_settings(request: Request, database: Index, collection_name: str):
        """Set configuration"""

        data = await request.json()

        if type(data) is not dict:
            return api_error("Invalid data", 400)

        await database.set_settings(collection_name, data)

        return json({"message": "Settings updated"})

    @get("/:collection_name/settings")
    async def get_settings(request: Request, database: Index, collection_name: str):
        """Set configuration"""

        settings = await database.get_settings(collection_name)

        return json(settings)
    
    @post("/:collection_name/schema")
    async def set_schema(request: Request, database: Index, collection_name: str):
        """Set schema"""

        data = await request.json()

        if type(data) is not list:
            return api_error("Invalid data", 400)

        await database.set_schema(collection_name, data)

        return json({"message": "Schema updated"})

    @get("/:collection_name/schema")
    async def get_schema(request: Request, database: Index, collection_name: str):
        """Set schema"""


        schema = await database.get_schema(collection_name)

        return json(schema)

    @delete("/:collection_name")
    async def delete_collection(request: Request, database: Index, collection_name: str):
        """Delete collection"""

        return json({"message": "Collection deleted"})
    

spotlyt_index = Index()
app.services.add_instance(spotlyt_index)