import json
import requests
import traceback
import elasticsearch
from collections import deque
from elasticsearch import helpers
from pprint import pprint

MSMARCO_INDEX = 'msmarco-documents'
MSMARCO_PATH = '../datasets/msmarco-documents.jsonl'


def get_ms_marco_documents(path=MSMARCO_PATH):
    with open(path, 'r') as f:
        for line in f:
            doc = json.loads(line)
            doc['_id'] = doc['doc_id']
            yield doc


def batch_iterator(generator, batch_size):
    batch = []
    for i, doc in enumerate(generator, 1):
        batch.append(doc)
        if i % batch_size == 0:
            yield batch
            batch = []
    yield batch

    
if __name__ == "__main__":
    es = elasticsearch.Elasticsearch(
        hosts=["localhost:9200"]
    )

    # Restore the index
    if es.indices.exists(MSMARCO_INDEX):
        es.indices.delete(MSMARCO_INDEX)
    es.indices.create(MSMARCO_INDEX)

    batch = []
    for i, batch in enumerate(batch_iterator(get_ms_marco_documents(), 1000000), 1):
        try:
            print(f"Inserting {len(batch):,} documents on {MSMARCO_INDEX}")
            response = helpers.bulk(es, batch, index=MSMARCO_INDEX)
        except Exception:
            print(f"Error in batch {i}")

