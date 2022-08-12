from calendar import c
import tqdm
import pymongo
import traceback
import elasticsearch
import multiprocessing as mp
from config import *
from utils import es_scroll_generator, get_hits_from_response


SCORED_DOCS_INDEX = 'dev_scoreddocs'


def _get_es_client():
    return elasticsearch.Elasticsearch(hosts=[ES_HOST])

def get_termsvector(doc_id, max_retries=3):
    for i in range(1, max_retries+1):
        try:
            es = _get_es_client()
            response = es.termvectors(
                id=doc_id,
                index=MSMARCO_DOCS_INDEX,
                fields="body,anchor_text,title,url,whole_document",
                term_statistics=True,
                field_statistics=True,
                offsets=False,
                positions=False,
            )
            del es
            return response
        except:
            traceback.print_exc()
    raise ValueError("Reached operation max retries")


if __name__ == "__main__":


    def generate_doc_ids(ids_to_skip=None):
        body = {
            "size": ES_SCROLL_BATCH_SIZE,
            "query": {
                "match_all": {}
            },
            "sort": [
                {"doc_id": "asc"},
            ]
        }
        skipped = 0
        for batch_docs in es_scroll_generator(ES_HOST, MSMARCO_DOCS_INDEX, body, scroll=SCROLL_EXPIRATION):
            for doc in batch_docs:
                doc_id = doc['doc_id']
                if ids_to_skip and doc_id in ids_to_skip:
                    if skipped % 100000 == 0:
                        print(f"Skipped {skipped:,} documents...")
                    skipped += 1
                else:
                    yield doc['doc_id']
    es = _get_es_client()
    assert es.cluster.health()['status'] == 'green'
    n_docs = es.count(index=MSMARCO_DOCS_INDEX)['count']

    with pymongo.MongoClient(MONGODB_HOST) as client:
        features_coll = client[FEATURES_DB][TERM_VECTORS_COLL]
        features_coll.create_index('doc_id', unique=True)

        n_processed = features_coll.count_documents({})
        cursor = features_coll.find({}, {'_id': 0, 'doc_id': 1}, batch_size=1000)
        pbar = tqdm.tqdm(cursor, total=n_processed, desc="Retrieving ids to skip")
        ids_to_skip = [doc['doc_id'] for doc in pbar]
        ids_to_skip = set(ids_to_skip)
    

    with mp.Pool(WORKERS) as p:
        with pymongo.MongoClient(MONGODB_HOST) as client:
            n_skip = len(ids_to_skip)
            pbar = tqdm.tqdm(
                p.imap(get_termsvector, generate_doc_ids(ids_to_skip=ids_to_skip)),
                total=n_docs,
                initial=n_skip
            )

            batch = []
            for response in pbar:
                response['doc_id'] = response['_id']
                to_insert = {k: v for k,v in response.items() if k in ['doc_id', 'term_vectors']}
                batch.append(to_insert)
                if len(batch) % MONGO_INSERT_BATCH_SIZE == 0:
                    features_coll = client[FEATURES_DB][TERM_VECTORS_COLL]
                    features_coll.insert_many(batch)
                    batch = []
            if batch:
                features_coll = client[FEATURES_DB][TERM_VECTORS_COLL]
                features_coll.insert_many(batch)


