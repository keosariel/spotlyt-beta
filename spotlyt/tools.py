import xapian
import uuid
import base64
import datetime
import re
import json
from spotlyt.constants import *

_date_re  = re.compile(r'(?P<year>[0-9]{4})(?P<month>[0-9]{2})(?P<day>[0-9]{2})$')
_date_re2 = re.compile(r'(?P<year>[0-9]{4})([-/.,])(?P<month>[0-9]{2})\2(?P<day>[0-9]{2})$')

def get_uuid():
    return str(uuid.uuid4())

async def docify(data, schema, database, collection_name, language="english"):
    """Create a xapian.Document from a list of dictionaries.

    Args:
        data (list): List of dictionaries.
        schema (dict): Schema of the collection.
        database (xapian.Database): Database to use.
        collection_name (str): Name of the collection.
        language (str): Language to use for stemming.
    """

    if not schema:
        raise LookupError(f"No schema found for collection {collection_name}")

    indexer = xapian.TermGenerator()
    indexer.set_stemmer(xapian.Stem(language))
    indexer.set_database(database)
    indexer.set_flags(indexer.FLAG_SPELLING)

    for datum in data:
        yield await to_doc(datum, database, schema, collection_name, indexer, language)

async def to_doc(datum, database, schema, collection_name, indexer, language="english"):
    """Create a xapian.Document from a dictionary.

    Args:
        data (dict): Dictionary.
        database (xapian.Database): Database to use.
        schema (dict): Schema of the collection.
        collection_name (str): Name of the collection.
        language (str): Language to use for stemming.
    """
    try:
        data = await parse_doc(datum, schema)
    except ValueError as e:
        return (None, e)

    doc = xapian.Document()
    indexer.set_document(doc)
    document_id = None
    inc_term_pos = False

    for field_name, field in schema.items():
        value = data.get(field_name, "")

        if value == "":
            continue

        field_slot = field["slot"]
        field_type = field["type"]

        if field_type == "id":
            document_id = value
        else:
            field_prefix = TERM_PREFIXES["field"]
            field_prefix = field_prefix + field_name.upper()

        if field.get("facet", False):
            if len(value) > 150:
                break  
            
            doc.add_boolean_term(field_prefix+":{}".format(value))
            doc.add_value(field_slot, value)

        doc.add_boolean_term(f"XSPOTLYTCOLLECTION:{collection_name}")
        doc.add_value(0, collection_name)
        
        if field_type == "text":
            indexer.index_text(value, 1, field_prefix)
            # doc.add_term(value)
            # doc.add_value(field_slot, value)
            indexer.index_text(value, 1, field_prefix)

            if inc_term_pos:
                indexer.increase_termpos()
            else:
                inc_term_pos = True

            indexer.index_text(value)

        elif field_type == "number" or field_type == "date":
            if value:
                doc.add_value(field_slot, xapian.sortable_serialise(value))
        
        doc.set_data(json.dumps(datum).encode('utf8'))

    if not document_id:
        document_id = get_uuid()
    
    document_id = TERM_PREFIXES["ID"] + document_id
    doc.add_term(document_id)
            
    return (document_id, doc)

async def parse_doc(doc, schema):
    """Parse a document into a dictionary."""
    
    if not isinstance(doc, dict):
        raise ValueError("Document must be in JSON format")

    parsed_doc = {}
    for field_name, field in schema.items():
        
        if field_name in doc:
            field_value = doc[field_name]
            field_type = field["type"]

            if field_type == "text":
                parsed_value = str(field_value)
            elif field_type == "date":
                parsed_value = await parse_date(field_value)
            elif field_type == "number":
                parsed_value = await parse_number(field_value)
            elif field_type == "boolean":
                parsed_value = await parse_boolean(field_value)
            # elif field_type == "geo":
            #     parsed_value = _parse_geo(field_value)
            elif field_type == "id":
                parsed_value = to_base64(field_value)

            parsed_doc[field_name] = parsed_value

    return parsed_doc

async def parse_number(value):
    """Parse a number from a string."""

    try:
        return abs(int(float(value)))
    except ValueError:
        try:
            return abs(int(value))
        except ValueError:
            return None

async def parse_date(value):
    """Check if a value is a date."""

    if (hasattr(value, 'year')
        and hasattr(value, 'month')
        and hasattr(value, 'day')):
        return datetime.date(value.year, value.month, value.day)
    
    match = _date_re.match(value)

    if match is None:
        match = _date_re2.match(value)

    if match:
        year, month, day = (int(i) for i in match.group('year', 'month', 'day'))
        return datetime.date(year, month, day)

    return None

# async def _parse_geopoints(value):
#     """Check if a value is a geopoint."""

#     if type(value) is not list:
#         return None
    
#     if len(value) != 2:
#         return None
    
#     for point in value:
#         if type(point) is not float or type(point) is not int:
#             return None

#     return value

async def parse_boolean(value):
    """Check if a value is a boolean."""

    value = str(value).lower().strip()

    if value == "true":
        return True
    elif value == "false":
        return False
    
    return None

def to_base64(data):
    """Encode data in base64.
    """

    _data = str(data).encode('ascii')
    _data_bytes = base64.b64encode(_data)

    return _data_bytes.decode('ascii')

def from_base64(data):
    """Decode data from base64.
    """

    _data = base64.b64decode(data)
    return _data.decode('ascii')
