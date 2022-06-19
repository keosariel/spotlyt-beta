import xapian

# global prefixes for the stored fields
TERM_PREFIXES = {
    "ID": "Q",
    "field": "X"
}

DEFAULT_FLAGS = (
    xapian.QueryParser.FLAG_AUTO_SYNONYMS | 
    xapian.QueryParser.FLAG_PARTIAL
)

OP_AND = xapian.Query.OP_AND
OP_OR  = xapian.Query.OP_OR
OP_FILTER = xapian.Query.OP_FILTER

# valid field types to be serialized as values in Xapian
FIELD_TYPES = { "text", "number", "date", "boolean", "geo", "id" }

# languages availabe for stemming
LANGUAGES = (
    xapian.Stem.get_available_languages()
    .decode('utf8')
    .split()
)

LOG_DEBUG = "DEBUG"
LOG_INFO = "INFO"
LOG_WARNING = "WARNING"
LOG_ERROR = "ERROR"
LOG_CRITICAL = "CRITICAL"

LOG_LEVELS = [LOG_DEBUG, LOG_INFO, LOG_WARNING, LOG_ERROR, LOG_CRITICAL]

COLLECTION_SCHEMA = {
    "type": "array",
    "items": {
        "type": "object",
        "properties": {
            "name": {
                "type": "string",
                "pattern": "[a-zA-Z_][a-zA-z0-9_]+$",
                "minLength": 1,
                "maxLength": 150
            },
            "type": {
                "type": "string",
                "enum": list(FIELD_TYPES),
                "default": "text",
                "description": "The type of the field is text"
            },
            "index": {
                "type": "boolean",
                "default": True,
                "description": "Whether the field is indexed"
            },
            "facet": {
                "type": "boolean",
                "default": False,
                "description": "Whether to facet this field"
            }
        },
        "required": ["name", "type"],
        "additionalProperties": False
    },
}


SETTINGS_SCHEMA = {
    "type": "object",
    "properties": {
        "typoTolerance": {
            "type": "boolean",
            "default": True
        },
        "synonym": {
            "type": "boolean",
            "default": False,
            "description": "Enable synonyms for the query parser"
        },
        "stopWords": {
            "type": "array",
            "items": {
                "type": "string"
            },
            "default": [],
            "description": "List of stop words to be removed from the query"
        },
        "language": {
            "type": "string",
            "enum": LANGUAGES,
            "default": "english",
            "description": "Language to use for stemming"
        },
        "stemming": {
            "type": "boolean",
            "default": False,
            "description": "Enable stemming for the query parser"
        },
        "collapse": {
            "type": "boolean",
            "default": False,
            "description": "Enable collapsing of search results"
        },
        "partialQuery": {
            "type": "boolean",
            "default": False,
            "description": "If true, the field will be used in partial queries"
        },
    },
    "additionalProperties": False
}


INDEX = "__spotlyt__"
LOGS = "spotlyt.logs"
TASK_QUEUE = "spotlyt.tasks"
CONFIG = "spotlyt.config"

INDEX_CREATED = "Index.New"
INDEX_OPENED = "Index.Opened"
INDEX_DELETED = "Index.Deleted"
INDEX_CLOSED = "Index.Closed"
INDEX_ADD = "Index.Add"
INDEX_DELETE = "Index.Delete"
INDEX_UPDATE = "Index.Update"
INDEX_SEARCH = "Index.Search"

LOG_OPERATIONS = [
    INDEX_CREATED,
    INDEX_OPENED,
    INDEX_DELETED,
    INDEX_CLOSED,
    INDEX_ADD,
    INDEX_DELETE,
    INDEX_UPDATE,
    INDEX_SEARCH
]

SLOT_START = 10

TASK_ACTIONS = [
    "indexDocument", 
    "deleteDocument"
]

RETRY = 5