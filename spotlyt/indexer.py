import collections
import os
import re
import jsonschema
import logging
import json
from functools import reduce
from .constants import *
from .utils import Table
from datetime import datetime
from spotlyt.tools import get_uuid, docify, from_base64
from spotlyt.searcher import *
from spotlyt.highlight import Highlight, fragment_text


# TODO: lower case all fields i.e collection_name'
# TODO: convsert datetime to int
# TODO: collapse

_index_name = re.compile("^[a-zA-Z0-9_]+$")

LOGGER_FORMAT = '%(levelname)s - %(message)s'

logging.basicConfig(format=LOGGER_FORMAT)
log = logging.getLogger()
log.setLevel(logging.DEBUG)

class Index:

    def __init__(self, name="index", language="english", 
            basedir="/index" # os.getcwd()
        ):
        """Initialize a new Spotlyt instance.
        """

        if not _index_name.fullmatch(name):
            raise ValueError("Invalid index name")

        self.index_name = name

        if not os.path.exists(basedir):
            raise ValueError("Index directory does not exist")
        
        self.basedir = os.path.join(basedir, INDEX)

        if not os.path.exists(self.basedir):
            os.makedirs(self.basedir)

        self.metadata = Table(
            os.path.join(self.basedir, "config.db"),
            table="config"
        )

        self.tasks_queue = Table(
            os.path.join(self.basedir, CONFIG),
            table="tasks_queue"
        )

        self.logs_table = Table(
            os.path.join(self.basedir, LOGS),
            table="logs"
        )

        self.index_path = os.path.join(self.basedir, self.index_name)
        self.database = None
        self.language = language
        self.hightlighter = Highlight(language)

        self.is_indexing = False

        # create or open a database for writing
        try:
            database = xapian.WritableDatabase(self.index_path, xapian.DB_CREATE_OR_OPEN)
            database.close() # close the database to since we would like to open it for writing later
        except xapian.DatabaseLockError:
            pass
        
        # opeing database for reading only
        try:
            database = xapian.Database(self.index_path)
        except xapian.DatabaseOpeningError:
            raise ValueError(f"Unable to open index at {self.index_path}")
        
        self.database = database

    async def set_language(self, language):
        """Set the language to use for stemming.
        """

        if language not in LANGUAGES:
            raise ValueError("Language is unavailable")

        self.language = language
        self.metadata.set({"language": language}, "language")

        self.hightlighter.language = language

        await self.log(
            "Set language to {}".format(language),
            LOG_INFO,
            operation="Index.Update",
            timestamp=datetime.now()
        )
    
    async def get_language(self):
        """Get the language to use for stemming.
        """
        
        data = self.metadata.get(
                "language",
                {"language": "english"})

        return data["language"]

     
    async def log(self, message, level, operation, timestamp=datetime.now()):
        """Log a message to the logs table.

        Args:
            message (str): The message to log.
            level (int): The level of the message.
            operation (str): The operation that caused the message.
            timestamp (datetime): The timestamp of the message.
        """

        level = level.upper()
        timestamp = timestamp.isoformat()

        if level not in LOG_LEVELS:
            raise ValueError("Invalid log level")

        # if operation not in LOG_OPERATIONS:
        #     raise ValueError("Invalid log operation")

        self.logs_table.set({"message": message, "level": level, "operation": operation, "timestamp": timestamp}, "message")

        log.info(f"[{timestamp}] - [{operation}] - [{level}] - {message}")

    async def set_schema(self, collection_name, schema):
        """Set the schema to use for indexing.
        """

        try: 
            jsonschema.validate(instance=schema, schema=COLLECTION_SCHEMA)
        except jsonschema.ValidationError as err:
            self.log(level=LOG_ERROR, operation="Index.Collection", 
                        message=f"Invalid schema for collection '{collection_name}': {err}")
            raise ValueError(f"Invalid schema: {err.message}")

        collection = self.metadata.get(collection_name, default=dict())
        collection_schema = collection.get("schema", dict())

        existing_fields = []
        id_field = None
        schema_fields = {}
        slot = SLOT_START

        schema = { field["name"] : field for field in schema }

        if collection:
            slot = collection.get("_spotlyt_max_slot", slot)

        for field_name, field in schema.items():    
            
            if field_name in existing_fields:
                await self.log(level=LOG_ERROR, operation="Index.Collection", 
                        message=f"Duplicate field '{field_name}' in schema for collection '{collection_name}'")
                raise ValueError("Duplicate field name")
            
            existing_fields.append(field_name)

            if field_name in collection_schema.keys():
                if field["type"] != collection_schema[field_name]["type"]:
                    await self.log(level=LOG_ERROR, operation="Index.Collection", 
                        message=f"Field '{field_name}' has changed type in schema for collection '{collection_name}'")
                    raise ValueError("Field type changed")
                else:
                    field_index = collection_schema[field_name].get("index", False)
                    collection_schema[field_name]["index"] = field.get("index", field_index)

                    field_facet = collection_schema[field_name].get("facet", False)
                    collection_schema[field_name]["facet"] = field.get("facet", field_facet)
                continue
            
            slot += 1

            current_field = {k:v for k,v in field.items() if k in ["name", "type"]}

            if field["type"] == "text":
                current_field["index"] = field.get("index", True)
            
            elif field["type"] == "id":
                if id_field:
                    await self.log(level=LOG_ERROR, operation="Index.Collection", 
                        message=f"Duplicate id field in collection '{collection_name}' schema")
                    raise ValueError(f"Duplicate id field `{field_name}`")

                id_field = field["name"]
            
            current_field["facet"] = field.get("facet", False)
            current_field["slot"] = slot

            schema_fields[field["name"]] = current_field

        collection["_spotlyt_max_slot"] = slot
        collection["schema"] = {**collection_schema, **schema_fields}

        self.metadata.set(collection, collection_name)  
        await self.log(level=LOG_INFO, operation="Index.Collection",
                    message=f"Updated schema for collection '{collection_name}'")
        
    async def get_schema(self, collection_name):
        """Get the schema for a collection.
        """

        collection = self.metadata.get(collection_name, default=dict())
        return collection.get("schema", dict())

    async def get_schema_field(self, collection_name, field_name):
        """Get the schema field for a collection.
        """

        collection = self.metadata.get(collection_name, default=dict())
        return collection.get("schema", dict()).get(field_name, dict())
    
    async def set_settings(self, collection_name, settings):
        """Set the settings to use for indexing.
        """

        try: 
            jsonschema.validate(instance=settings, schema=SETTINGS_SCHEMA)
        except jsonschema.ValidationError as err:
            self.log(level=LOG_ERROR, operation="Index.Settings", 
                        message="Invalid settings: {}".format(err))
            raise ValueError(f"Invalid schema: {err.message}")

        collection = self.metadata.get(collection_name, default=dict())
        collection_settings = collection.get("settings", dict())

        collection["settings"] = {**collection_settings, **settings}

        self.metadata.set(collection, collection_name)
        await self.log(level=LOG_INFO, operation="Index.Settings",
                    message="Updated settings for collection '{}'".format(collection_name))

    async def get_settings(self, collection_name):
        """Get the settings for a collection.
        """

        collection = self.metadata.get(collection_name, default=dict())
        return collection.get("settings", dict())

    async def get_settings_field(self, collection_name, field_name, default=None):
        """Get the settings field for a collection.
        """

        collection = self.metadata.get(collection_name, default=dict())
        return collection.get("settings", dict()).get(field_name, default)

    async def add_task(self, collection_name, documents, action="indexDocument"):
        """Add a task to the queue.

        Args:
            collection_name (str): The name of the collection.
            documents (list): The documents to index.
            action (str): The action to perform on the documents.
        """

        if action not in TASK_ACTIONS:
            raise ValueError("Invalid task action")

        if not isinstance(documents, list):
            raise ValueError("Documents must be a list.")
        
        if not documents:
            raise ValueError("Documents must not be empty.")
        
        if documents:   
            task_id = get_uuid()

            task = {
                "collection": collection_name,
                "action": action,
                "documents": documents,
                "timeAdded": datetime.now().isoformat(),
                "status": "pending",
                "hasError": False,
                "id": task_id,
                "documentsLength": len(documents)
            }

            self.tasks_queue.set(task, task_id)
            await self.log(level=LOG_INFO, operation="Index.Task",
                        message=f"Added task '{task_id}' to queue")
            
            return task_id
    
    async def get_task(self, task_id, doc=True):
        """Get a task from the queue.
        """
        task = self.tasks_queue.get(task_id)
        if task and not doc:
            task.pop("documents")

        return task

    async def get_tasks(self, collection_name="*", status="pending", just_id=False):
        """Get all tasks for a collection.
        """

        tasks = []

        for task_id, task in self.tasks_queue.items(sort=True):
            if collection_name != "*" and task["collection"] != collection_name:
                continue
            
            if task["status"] == status:
                if just_id:
                    tasks.append(task_id)
                else:
                    tasks.append(task)
        
        return tasks

    async def add_documents(self, collection_name, documents):
        """Add documents to the index.
        """
        
        return await self.add_task(collection_name, documents, action="indexDocument")
    
    async def delete_documents(self, collection_name, documents):
        """Delete documents from the index.
        """

        return await self.add_task(collection_name, documents, action="deleteDocument")
    
    async def index_documents(self):
        """Index documents in the queue.
        """

        if not await self.is_locked():
            try:
                database = xapian.WritableDatabase(self.index_path, xapian.DB_CREATE_OR_OPEN)
            except xapian.DatabaseLockError:
                return

            self.is_indexing = True
            tasks = await self.get_tasks()

            for task in tasks:
                if task["action"] == "indexDocument":
                    collection_name = task["collection"]
                    schema = await self.get_schema(collection_name)

                    if not schema:
                        await self.log(level=LOG_ERROR, operation="Index.Index",
                            message=f"No schema found for collection '{collection_name}'")
                        task["hasError"] = True
                        continue
                    
                    language = await self.get_settings_field(collection_name, "language", "english")
                    try:
                        docs = docify(task["documents"], schema, 
                                    database, collection_name, language)
                    except Exception as err:
                        await self.log(level=LOG_ERROR, operation="Index.Document",
                                    message=f"Error parsing documents: {err}")

                        task["status"] = "failed"
                        task["hasError"] = True
                        self.tasks_queue.set(task, task["id"])
                        continue

                    async for docid, xdoc in docs:
                        if docid is None:
                            await self.log(level=LOG_ERROR, operation="Index.Document",
                                        message=f"Error indexing document in collection `{task['collection']}`, task `{task['id']}` with message `{xdoc.message}`")
                            task["hasError"] = True
                            continue
                        database.replace_document(docid, xdoc)
                                
                elif task["action"] == "deleteDocument":
                    for doc in task["documents"]:
                        docid = doc.get("id")
                        if docid: database.delete_document(docid)

                await self.log(level=LOG_INFO, operation="Index.Document",
                                message=f"Completed task {task['id']}")

                task["status"] = "completed"
                task["documents"] = []
                self.tasks_queue.set(task, task["id"])

                if self.database:
                    self.database.reopen()

            database.commit()
            database.close()

            self.is_indexing = False

        else:
            self.is_indexing = True
        
    async def is_locked(self):
        """Check if the index is locked."""
        return self.database.locked()

    async def get_collections(self):
        """Get the collections in the index."""

        return list(self.metadata.ids())

    async def info(self):
        """Get the index info.
        """
        await self.reopen()

        collections = await self.get_collections()
        info = {
            "totalRecords": self.database.get_doccount(),
            "totalCollections": collections,
            "collectionsConfiguration": [ self.metadata.get(x) for x in collections ]
        }

        return info

    async def reopen(self):
        """Reopen the database.  Called before most query methods."""
        self.database.reopen()

    async def add_synonym(self, term, synonym):
        """Add a synonym to the database."""

        self.database.add_synonym(term, synonym)

    async def remove_synonym(self, term, synonym):
        """Remove a synonym from the database."""

        self.database.remove_synonym(term, synonym)
        
    async def get_logs(self, offset=0, pagesize=10):
        """Get index logs."""

        data = list(self.logs_table.items(sort=True))
        return data

    async def hightlight_text(self, text, targets, limit=10):
        """Highlight text."""
        tokens = await self.hightlighter.split_text(text)
        fragments = fragment_text(tokens)
        highlights = []
        i = 0
        async for fragment in fragments:
            h = await self.hightlighter.highlight_words(fragment, targets=targets)
            if i:
                h[1] = "..."+h[1].strip()
            highlights.append(h)
            i += 1

            if i >= limit:
                break
        
        return sorted(highlights, key=lambda x: x[0], reverse=True)

    async def retry(self, function, *args, **kwargs):
        """Retry a function."""
        d = 0
        while d <= RETRY:
            try:
                return await function(*args, **kwargs)
            except xapian.DatabaseModifiedError as err:
                await self.log(level=LOG_ERROR, operation="Index.Retry",
                            message=f"Error: {err}")
                self.reopen()

            d += 1
        
        raise err

    async def query(self, querystring, collection_name="*", 
                    offset=0, pagesize=10, fields=None,
                    facets=[], ranges=[], highlight=False,
                    collapse=False, to_highlight=[],
                    spell_check=True, stopwords=True, synonyms=False):
        """Query the index.

        Args:
            querystring (str): The query string.
            collection_name (str): The name of the collection.
            spell_check (bool): Whether to spell check the query.
            stopwords (bool): Whether to remove stopwords from the query.
            synonyms (bool): Whether to replace synonyms in the query.
        """

        queryparser = await self.retry(query_parser, self.database, self.language)
        query_correction = ""

        if spell_check:
            new_querystring = await self.retry(do_spellcheck, queryparser, querystring)
            query_correction = new_querystring
            if new_querystring: querystring = new_querystring
        
        if stopwords:
            querystring = await do_stopwords(querystring, self.language)


        queryparser.parse_query(querystring, await do_flags(spell_check, synonyms))

        schema = await self.get_schema(collection_name)

        if not schema:
            raise ValueError("Collection not found")
        
        collections = await self.get_collections()

        if collection_name != "*":
            if collection_name not in collections:
                raise ValueError("Collection not found")
            
            collections = [ collection_name ]
        
        query = await join_query(
            xapian.Query.OP_OR,
            *[xapian.Query(f'XSPOTLYTCOLLECTION:{c_name}') for c_name in collections]
        )

        fields = fields or "*"

        if fields == "*":
            fields = ( (field["slot"], field_name) 
                            for field_name, field in schema.items() 
                                if field["type"] == "text" )

        else:
            fields = ( (schema[field_name]["slot"], field_name)
                        for field_name in fields 
                            if field_name in schema
                                if schema[field_name]["type"] == "text" )
        
        if fields:
            field_query = await query_fields(querystring, queryparser, fields)
            query = await join_query(xapian.Query.OP_FILTER, query, field_query)

        facets = facets or dict()

        if not isinstance(facets, dict):
            raise ValueError("Facets must be a JSON object")

        facets = ((field_name, facet) for field_name, facet in facets.items()
                        if field_name in schema 
                            if schema[field_name].get("type") == "text" 
                                and schema[field_name].get("facet", False))
        
        if facets:
            facets_query = await query_facets(querystring, queryparser, facets)
            query = await join_query(xapian.Query.OP_AND, query, facets_query)

        ranges = ranges or dict()

        if not isinstance(ranges, dict):
            raise ValueError("Ranges must be a JSON object")
        
        ranges = ((schema[field_name]["slot"], field_name, value) 
                    for field_name, value in ranges.items()
                        if field_name in schema
                            if schema[field_name].get("type") == "number"  
                                or schema[field_name].get("type") == "date")

        enquire = xapian.Enquire(self.database)

        if ranges:
            range_queries, keymaker = await query_ranges(queryparser, ranges)
            
            if range_queries:
                try:
                    query = await join_query(xapian.Query.OP_AND, query, range_queries)
                except ValueError as e:
                    raise ValueError(f"Error querying ranges: {e}")

            enquire.set_sort_by_key_then_relevance(keymaker, False)

        if collapse:
            pass
        
        enquire.set_query(query)

        matches = enquire.get_mset(offset, pagesize)
        matches_data = []

        if to_highlight:
            querytokens = await self.hightlighter.split_text(querystring)

        for match in matches:
            data = json.loads(match.document.get_data())
            data["_spotlytHighlights"] = {}

            for field_name in to_highlight:
                if isinstance(data.get(field_name, None), str):
                    highlights = ""

                    for h in await self.hightlight_text(data[field_name], querytokens):
                        highlights += h[1].strip()

                    data["_spotlytHighlights"][field_name] = highlights

            data["_spotlytRank"] = match.rank + 1
            data["_spotlytDocId"] = from_base64(match.document.get_value(2))
            matches_data.append(data)

        return {
            "query": querystring,
            "queryCorrection": query_correction,
            "results": matches_data,
            "totalResults": matches.get_matches_estimated(),
            "totalRecords": self.database.get_doccount(),
            "offset": offset,
            "pagesize": pagesize,
            "highlight": highlight
        }

    def __repr__(self):
        return "Spotlyt(documents=%s)" % (self.database.get_doccount())
