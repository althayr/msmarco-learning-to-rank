import os
import tqdm
import json
import argparse
import traceback
import elasticsearch
from collections import deque
from elasticsearch import helpers
from pprint import pprint
from config import *

import pdb

def get_generic_document_iterator(path, id_field=None, limit=None):
    with open(path, 'r') as f:
        for i, line in enumerate(f, 1):
            if limit and i == limit + 1:
                return
            doc = json.loads(line)
            if doc['doc_id'] != 'D2765617':
                continue
            pdb.set_trace()
            if id_field:
                doc['_id'] = doc[id_field]
            yield doc

def get_document_indexer(path, id_field=None, limit=None):
    return get_generic_document_iterator(path, id_field, limit)


def get_anchor_text_indexer(path, index):
    "Adds anchor text field to the original msmarco documents index"
    for doc in get_document_indexer(path):
        query = {
            '_op_type': 'update',
            '_index': index,
            '_id': doc['doc_id'],
            'doc': {
                'anchor_text': doc['text'],
            }
        }
        yield query

def get_concat_field_updater(path, index):
    for doc in get_document_indexer(path):
        concat_document_field = ' '.join([
            doc['url'],
            doc['title'],
            doc['body'],
            doc.get('anchor_text', '')
        ])
        query = {
            '_op_type': 'update',
            '_index': index,
            '_id': doc['doc_id'],
            'doc': {
                'whole_document': concat_document_field,
            }
        }
        yield query

def batch_iterator(generator, batch_size):
    batch = []
    for i, doc in enumerate(generator, 1):
        batch.append(doc)
        if i % batch_size == 0:
            yield batch
            batch = []
    yield batch


def compute_in_batches(json_path, index, generator):
    f = os.popen(f"wc -l {json_path}")
    count = int(f.read().split(" ")[0])

    batch = []
    batch_size = 10000
    n_batches = count // batch_size
    iterator = batch_iterator(generator, batch_size)
    es = elasticsearch.Elasticsearch(hosts=[ES_HOST])
    i = 0
    for i, batch in tqdm.tqdm(enumerate(iterator, 1), total=n_batches):
        try:
            response = helpers.bulk(es, batch, index=index)
        except Exception:
            traceback.print_exc()
            print(f"Error in batch {i}")
            break


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    args = parser.parse_args()

    es = elasticsearch.Elasticsearch(
        hosts=["localhost:9200"]
    )
    print("Populating original documents...")
    compute_in_batches(
        MSMARCO_DOCS_PATH,
        MSMARCO_DOCS_INDEX,
        get_document_indexer(path=MSMARCO_DOCS_PATH, id_field='doc_id')
    )

    print("Adding `anchor_text` field...")
    compute_in_batches(
        MSMARCO_ANCHORS_PATH,
        MSMARCO_DOCS_INDEX,
        get_anchor_text_indexer(path=MSMARCO_ANCHORS_PATH, index=MSMARCO_DOCS_INDEX)
    )

    print("Adding concatenated `whole_document` field...")
    compute_in_batches(
        MSMARCO_DOCS_PATH,
        MSMARCO_DOCS_INDEX,
        get_concat_field_updater(path=MSMARCO_DOCS_PATH, index=MSMARCO_DOCS_INDEX)
    )

    print("Populating dev queries...")
    compute_in_batches(
        DEV_QUERIES_PATH,
        DEV_QUERIES_INDEX,
        get_generic_document_iterator(DEV_QUERIES_PATH)
    )

    print("Populating dev scoreddocs...")
    compute_in_batches(
        DEV_SCOREDDOCS_PATH,
        DEV_SCOREDDOCS_INDEX,
        get_generic_document_iterator(DEV_SCOREDDOCS_PATH)
    )

    print("Populating dev qrels...")
    compute_in_batches(
        DEV_QRELS_PATH,
        DEV_QRELS_INDEX,
        get_generic_document_iterator(DEV_QRELS_PATH)
    )

    print("Populating train queries...")
    compute_in_batches(
        TRAIN_QUERIES_PATH,
        TRAIN_QUERIES_INDEX,
        get_generic_document_iterator(TRAIN_QUERIES_PATH)
    )

    print("Populating train scoreddocs...")
    compute_in_batches(
        TRAIN_SCOREDDOCS_PATH,
        TRAIN_SCOREDDOCS_INDEX,
        get_generic_document_iterator(TRAIN_SCOREDDOCS_PATH)
    )

    print("Populating train qrels...")
    compute_in_batches(
        TRAIN_QRELS_PATH,
        TRAIN_QRELS_INDEX,
        get_generic_document_iterator(TRAIN_QRELS_PATH)
    )

    print("Populating eval queries...")
    compute_in_batches(
        EVAL_QUERIES_PATH,
        EVAL_QUERIES_INDEX,
        get_generic_document_iterator(EVAL_QUERIES_PATH)
    )

    print("Populating eval scoreddocs...")
    compute_in_batches(
        EVAL_SCOREDDOCS_PATH,
        EVAL_SCOREDDOCS_INDEX,
        get_generic_document_iterator(EVAL_SCOREDDOCS_PATH)
    )

