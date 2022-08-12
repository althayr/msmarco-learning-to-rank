import elasticsearch
from config import *


def get_queries_index(type):
    if type == 'dev':
        return DEV_QUERIES_INDEX
    elif type == 'train':
        return TRAIN_QUERIES_INDEX
    elif type == 'eval':
        return EVAL_QUERIES_INDEX
    raise ValueError

def get_scoreddocs_index(type):
    if type == 'dev':
        return DEV_SCOREDDOCS_INDEX
    elif type == 'train':
        return TRAIN_SCOREDDOCS_INDEX
    elif type == 'eval':
        return EVAL_SCOREDDOCS_INDEX
    raise ValueError

def get_qrels_index(type):
    if type == 'dev':
        return DEV_QRELS_INDEX
    elif type == 'train':
        return TRAIN_QRELS_INDEX
    raise ValueError


def get_hits_from_response(response):
    return [doc["_source"] for doc in response["hits"]["hits"]]

def clean_search_fields(query):
    "Removes fields compatible only with elastic search API"
    return {k: v for k,v in query.items() if k not in ['size', '_source', 'sort']}

def es_scroll_generator(
    host,
    index,
    body,
    timeout=60,
    scroll='1m',
    debug=False,
    batch=True
):
    es = elasticsearch.Elasticsearch(
        host,
        timeout=timeout,
    )
    if debug:
        n_docs = es.count(
            index=index,
            body=clean_search_fields(body),
        )["count"]
    results = es.search(index=index, body=body, request_timeout=timeout, scroll=scroll)
    docs = get_hits_from_response(results)
    scroll_size = len(docs)
    scroll_id = results["_scroll_id"]
    processed = scroll_size
    while scroll_size > 0:
        # Before scroll, process current batch of hits
        if batch:
            yield docs
        else:
            for doc in docs:
                yield doc
        results = es.scroll(scroll_id=scroll_id, scroll=scroll)
        # Update the scroll ID
        scroll_id = results["_scroll_id"]
        # Get the number of results that returned in the last scroll
        docs = get_hits_from_response(results)
        scroll_size = len(docs)
        processed += scroll_size
        if debug:
            print(f"Processed : {processed:,} / {n_docs:,} ({100*processed/n_docs:.6f}%)")
    es.clear_scroll(body={"scroll_id": scroll_id})


def queries_from_index_factory(host, index, limit=None):
    body = {'size': 10000, "sort": [{"query_id.keyword": "asc"}],}
    yield from documents_from_index_factory(host, index, limit, body)


def documents_from_index_factory(host, index, limit=None, body=None):
    es = elasticsearch.Elasticsearch(host)
    n_queries = es.count(
        index=index,
        body=clean_search_fields(body),
    )["count"]
    
    def gen():
        i = 1
        for batch in es_scroll_generator(host, index, body):
            for query_doc in batch:
                if limit and i > limit:
                    return
                i += 1
                yield query_doc
    
    return n_queries, gen

