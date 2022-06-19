import xapian
import re
from spotlyt.snowball import stopwords
from spotlyt.constants import *

_split_re = re.compile(r'<\w+[^>]*>|</\w+>|[\w\']+|\s+|[^\w\'\s<>/]+')
_range_re = re.compile("(?P<start>[0-9]+)\.\.(?P<end>[0-9]+)?(:(?P<mode>asc|desc))?")

async def query_parser(database, language):
    qp = xapian.QueryParser()
    qp.set_stemmer(xapian.Stem(language))
    qp.set_stemming_strategy(xapian.QueryParser.STEM_SOME)
    qp.set_database(database)

    return qp


async def do_spellcheck(queryparser, querystring):    
    queryparser.parse_query(querystring, xapian.QueryParser.FLAG_SPELLING_CORRECTION)
    return queryparser.get_corrected_query_string().decode("utf8")


async def do_stopwords(querystring, language):
    global stopwords
    _stopwords = stopwords.get(language)
    querylist = await split(querystring)
    querystring = "".join([x if x not in _stopwords else "-"+x  for x in querylist])

    return querystring

async def split(text):
    return _split_re.findall(text)

async def do_flags(spell_check=False, synonyms=False):
    flag = (xapian.QueryParser.FLAG_PARTIAL 
            | xapian.QueryParser.FLAG_LOVEHATE
            | xapian.QueryParser.FLAG_PHRASE
            | xapian.QueryParser.FLAG_WILDCARD)

    if spell_check:
        flag |= xapian.QueryParser.FLAG_SPELLING_CORRECTION
    
    if synonyms:
        flag |= (xapian.QueryParser.FLAG_AUTO_SYNONYMS | 
                 xapian.QueryParser.FLAG_AUTO_MULTIWORD_SYNONYMS |
                 xapian.QueryParser.FLAG_SYNONYM)

    return flag 
    
async def join_query(op, *querylist):
    _query = None
    for i, query in enumerate(querylist):
        if i == 0:  
            _query = query
        else:
            if query:
                _query = xapian.Query(op, _query, query)
        
    return _query

async def query_fields(querystring, queryparser, fields):
    queries = []
    for _, field in fields:
        field_prefix = TERM_PREFIXES["field"] + field.upper()
        field_query = queryparser.parse_query(querystring, 1, field_prefix)

        queries.append(field_query)
    
    return await join_query(xapian.Query.OP_OR, *queries)

async def query_facets(querystring, queryparser, facets):
    queries = []
    for field_name, facet in facets:
        field_prefix = TERM_PREFIXES["field"] + field_name.upper()
        queries.append(
            xapian.Query('{}:{}'.format(field_prefix, facet))
        )
    
    return await join_query(xapian.Query.OP_AND, *queries)

async def query_ranges(queryparser, ranges):
    
    queries = []

    keymaker = xapian.MultiValueKeyMaker()

    for field_slot, _, value in ranges:
        if not isinstance(value, str):
            raise ValueError("Range value must be a string")

        match = _range_re.fullmatch(value)

        start, end, mode = match.group("start", "end", "mode")
        range_str = f"{start}..{ end if end else '' }"

        queryparser.add_valuerangeprocessor(
            xapian.NumberValueRangeProcessor(field_slot)
        )

        range_query = queryparser.parse_query(range_str)
        queries.append(range_query)

        if mode:
            if mode == "asc":
                keymaker.add_value(field_slot, False)
            elif mode == "desc":
                keymaker.add_value(field_slot, True)
    
    return await join_query(xapian.Query.OP_AND, *queries), keymaker
        

