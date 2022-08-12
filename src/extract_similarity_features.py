import traceback
import tqdm
import argparse
import elasticsearch
from config import *
from utils import *
import pymongo
import multiprocessing as mp
import pdb
import itertools
from functools import partial
from collections import defaultdict

import warnings

warnings.filterwarnings("ignore", message=".*elastic.*")


def get_es_client():
    es = elasticsearch.Elasticsearch(
        hosts=[ES_HOST], timeout=ES_TIMEOUT, max_retries=MAX_RETRIES, retry_on_timeout=True
    )
    return es


def get_similarity_features(es, query_doc, scoreddocs):

    # stores each document feature since they can be in different orders on each query
    doc_features_map = defaultdict(lambda: {})
    for field in ["body", "anchor_text", "title", "url", "whole_document"]:
        body = {
            "size": len(scoreddocs),
            "_source": False,
            # "explain": True,
            "query": {
                "bool": {
                    # Restrict only for the pre scored documents
                    "filter": {
                        "terms": {
                            "doc_id": [doc['doc_id'] for doc in scoreddocs]
                            }
                        },
                    # Search for field of interest
                    "should": {
                        "match": {field: query_doc['text']}
                    }
                }
            }
        }
        response = es.search(index=MSMARCO_DOCS_INDEX, body=body)
        for hit in response['hits']['hits']:
            doc_id = hit['_id']
            doc_features_map[doc_id][field] = hit['_score']

    features = []
    for doc_id, doc_features in doc_features_map.items():
        features.append({
            'doc_id': doc_id,
            'query_id': query_doc['query_id']
        })
        features[-1].update(doc_features)
    return features


class MaxTriesException(Exception):
    def __init__(self, msg, *args, **kwargs):
        super().__init__(msg, *args, **kwargs)


def extract_features_for_all_docs(query_doc, type):
    try:
        # Get the pre scored documents
        es = get_es_client()
        scoreddocs_index = get_scoreddocs_index(type)
        body = {"size": 100, "query": {"match": {"query_id": query_doc['query_id']}}}
        response = es.search(
            index=scoreddocs_index,
            body=body
        )
        query_rated_docs = get_hits_from_response(response)

        # Feature for the best 100 documents
        query_doc_features = get_similarity_features(es, query_doc, query_rated_docs)
        return query_doc_features
    except Exception:
        traceback.print_exc()
        # if is still i'm assuming this is a problem with this particular document
        es = get_es_client()
        if es.cluster.health()['status'] == 'green':
            print(f"Skipping query specific error - query_id : `{query_doc['query_id']}`...")
            return []
        else:
            raise



if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--type", choices=["dev", "train", "eval"], type=str, default="dev"
    )
    parser.add_argument("--limit", default=None, type=int)
    parser.add_argument("--coll", default=None, type=str)
    parser.add_argument("--export-every", default=None, type=int)
    parser.add_argument("--workers", default=None, type=int)
    parser.add_argument("--db", default=None, type=str)
    parser.add_argument("--query-id", default=None, type=str)
    parser.add_argument("--similarity",
                        choices=['bm25', 'boolean', 'lmir.dir', 'lmir.jm', 'tfidf', 'word2vec', 'elmo'],
                        type=str)
    args = parser.parse_args()


    queries_index = get_queries_index(args.type)
    n_queries, queries_generator = queries_from_index_factory(
        host=ES_HOST,
        index=queries_index,
        limit=args.limit
    )


    if args.query_id:
        def generator():
            doc = {
                "query_id": args.query_id,
                "text": "what is rs currency to the dollar"
            }
            yield doc
    else:
        # Since this is a long running process that can fail
        # we add wrapper to ignore previously processed ids
        with pymongo.MongoClient(MONGODB_HOST) as client:
            coll = client[args.db][args.coll]
            coll.create_index("doc_id")
            coll.create_index("query_id")
            coll.create_index([("query_id", 1), ("doc_id", 1)], unique=True)

            n_processed = coll.count_documents({})
            cursor = coll.find({}, {'_id': 0, 'query_id': 1}, batch_size=1000)
            pbar = tqdm.tqdm(cursor, total=n_processed, desc="Retrieving query ids to skip")
            ids_to_skip = {doc["query_id"] for doc in pbar}
        

        def generator():
            print(f"Skipping {len(ids_to_skip):,} {100*len(ids_to_skip)/n_queries:.2f}% query ids")
            skipped = 0
            for query_doc in queries_generator():
                if ids_to_skip and query_doc['query_id'] in ids_to_skip:
                    if skipped % 10000 == 0:
                        print(f"Skipped {skipped:,} queries...")
                    skipped += 1
                else:
                    yield query_doc

    if not args.workers:
        for query_doc in tqdm.tqdm(queries_generator(), total=n_queries):
            qid = query_doc['query_id']

    else:
        # Get queries from the specified set
        get_query_document_features = partial(
            extract_features_for_all_docs,
            type=args.type
        )
     
        with mp.Pool(args.workers) as p:
            with pymongo.MongoClient(MONGODB_HOST) as client:
                
                pbar = tqdm.tqdm(p.imap(get_query_document_features, generator()), total=n_queries-len(ids_to_skip))

                batch = []
                for best_docs_features in pbar:
                    batch.append(best_docs_features)

                    if len(batch) % args.export_every == 0:
                        features_coll = client[args.db][args.coll]
                        docs = list(itertools.chain.from_iterable(batch))
                        features_coll.insert_many(docs)
                        batch = []

                if batch:
                    features_coll = client[args.db][args.coll]
                    docs = list(itertools.chain.from_iterable(batch))
                    features_coll.insert_many(docs)
