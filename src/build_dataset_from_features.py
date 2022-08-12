import pymongo
import argparse
import numpy as np
import pandas as pd
import tqdm
from config import *
from utils import *
import pdb

import warnings

warnings.filterwarnings("ignore", message=".*elastic.*")

def get_collection(host, db, coll):
    return pymongo.MongoClient(host)[db][coll]


def build_document_features():
    # Document features are extracted from the termsvector API
    coll = get_collection(MONGODB_HOST, FEATURES_DB, 'document_features_v2')
    query = {}
    projection = {"_id": 0}
    n_docs = coll.count_documents(query)
    cursor = tqdm.tqdm(coll.find(query, projection), total=n_docs, desc='Querying document features')
    df = pd.DataFrame(list(cursor))
    return df


def build_scoreddocs_dataframe(type, query_ids):
    body = {
        "size": 10000,
        "query": {"terms": {"query_id": query_ids}}
    }
    index = get_scoreddocs_index(type)
    n_docs, gen = documents_from_index_factory(host=[ES_HOST], index=index, body=body)
    docs = [doc for doc in tqdm.tqdm(gen(), total=n_docs, desc="Building scoreddocs dataframe")]
    df = pd.DataFrame(docs)
    return df


def build_query_document_features(type, query_ids):
    similarity_collection_map = {
        'bm25': f'{type}_bm25',
        'lmir_dir': f'{type}_lmir_dir',
        'lmir_jm': f'{type}_lmir_jm'
    }
    document_fields = ['url', 'title', 'body', 'anchor_text', 'whole_document']

    df_merged = build_scoreddocs_dataframe(type, query_ids)
    for similarity, collection in similarity_collection_map.items():
        print(f"Fetching data for {similarity} on collection : {collection}")
        coll = get_collection(MONGODB_HOST, FEATURES_DB, collection)
        query = {"query_id": {"$in": query_ids}}
        projection = {"_id": 0}
        n_docs = coll.count_documents(query)
        cursor = tqdm.tqdm(coll.find(query, projection), total=n_docs, desc=f"Building query-doc features - similarity `{similarity}`")
        df = pd.DataFrame(list(cursor))

        # Possible missing variables
        for col in document_fields:
            if col not in df.columns:
                print(col, df.columns)
                df[col] = 0
        
        col_name_map = {col: f'{similarity}_{col}' for col in document_fields}
        df = df.rename(columns=col_name_map)
        df_merged = df_merged.merge(df, on=['query_id', 'doc_id'], how='left')
    
    df_merged = (
        df_merged
        .sort_values(by=['query_id', 'doc_id'])
        .fillna(0)
    )
    return df_merged


def get_source_queries(type, sample_frac=None):
    index = get_queries_index(type)
    n_queries, gen = queries_from_index_factory(host=[ES_HOST], index=index)

    qids = set()
    for doc in tqdm.tqdm(gen(), total=n_queries, desc=f"Fetching queries related to `{type}` dataset"):
        qids.add(doc['query_id'])

    if not sample_frac:
        return list(qids)
    
    np.random.seed(42)
    n_samples = int(sample_frac * n_queries)
    samples = np.random.choice(list(qids), size=n_samples)
    return list(samples)


def get_relevance_labels(type, query_ids):
    body = {
        "size": 100,
        "query": {"terms": {"query_id": query_ids}}
    }
    index = get_qrels_index(type)
    n_docs, gen = documents_from_index_factory(host=[ES_HOST], index=index, body=body)
    relevance_map = {
        doc['query_id']: doc['doc_id']
        for doc in tqdm.tqdm(gen(), total=n_docs, desc="Fetching document relevancy grade (qrels)")
    }
    return relevance_map



if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--type", choices=["dev", "train", "eval"], type=str, default="dev"
    )
    parser.add_argument("--output", "-o", type=str)
    args = parser.parse_args()

    sample_frac = 0.10 if args.type == "train" else None
    sampled_qids = get_source_queries(args.type, sample_frac)
    
    query_relevant_document_map = get_relevance_labels(args.type, sampled_qids)

    df_doc = build_document_features()
    df_query_doc = build_query_document_features(args.type, sampled_qids)
    df = df_query_doc.merge(df_doc, how='left', on='doc_id')

    print(df.columns)
    print(df.shape, len(sampled_qids))

    query_relevant_document = df['query_id'].map(query_relevant_document_map)
    df['label'] = query_relevant_document == df['doc_id']
    df['label'] = df['label'].astype(int)

    print(df.label.value_counts())
    print(df.label.value_counts(True))

    df.to_parquet(args.output)


