MSMARCO_DOCS_INDEX = "msmarco-documents"
MSMARCO_DOCS_PATH = "../datasets/msmarco-documents.jsonl"
MSMARCO_ANCHORS_PATH = "../datasets/anchor-text.jsonl"
MSMARCO_FEATURES_INDEX = "msmarco-features"

TRAIN_QUERIES_INDEX = "train_queries"
TRAIN_SCOREDDOCS_INDEX = "train_scoreddocs"
TRAIN_QRELS_INDEX = "train_qrels"
DEV_QUERIES_INDEX = "dev_queries"
DEV_SCOREDDOCS_INDEX = "dev_scoreddocs"
DEV_QRELS_INDEX = "dev_qrels"
EVAL_QUERIES_INDEX = "eval_queries"
EVAL_SCOREDDOCS_INDEX = "eval_scoreddocs"

TRAIN_QUERIES_PATH = "../datasets/train/queries.jsonl"
TRAIN_SCOREDDOCS_PATH = "../datasets/train/scoreddocs.jsonl"
TRAIN_QRELS_PATH = "../datasets/train/qrels.jsonl"
DEV_QUERIES_PATH = "../datasets/dev/queries.jsonl"
DEV_SCOREDDOCS_PATH = "../datasets/dev/scoreddocs.jsonl"
DEV_QRELS_PATH = "../datasets/dev/qrels.jsonl"
EVAL_QUERIES_PATH = "../datasets/eval/queries.jsonl"
EVAL_SCOREDDOCS_PATH = "../datasets/eval/scoreddocs.jsonl"

ES_HOST = "http://localhost:9200"
ES_TIMEOUT = 30
MAX_RETRIES = 3
SCROLL_EXPIRATION = "1m"


# Config for the script that generates 
# term statistics
ES_SCROLL_BATCH_SIZE = 5000
MONGO_INSERT_BATCH_SIZE = 15000
WORKERS = 12
MONGODB_HOST = 'localhost:27017'
FEATURES_DB = 'features'
TERM_VECTORS_COLL = 'term_vectors'

OUTPUT_FILE = "features.parquet.gzip"

MSMARCO_DOCS_INDEX_CONFIG = {
    "mappings": {
        "properties": {
            "anchor_text": {
                "type": "text",
                "term_vector": "with_positions_offsets",
                "store": True,
            },
            "body": {
                "type": "text",
                "term_vector": "with_positions_offsets",
                "store": True,
            },
            "concat_document_fields": {
                "type": "text",
                "term_vector": "with_positions_offsets",
                "store": True,
            },
            "doc_id": {"type": "keyword"},
            "title": {
                "type": "text",
                "term_vector": "with_positions_offsets",
                "store": True,
            },
            "url": {
                "type": "text",
                "term_vector": "with_positions_offsets",
                "store": True,
            },
        }
    },
    "settings": {"index": {"number_of_shards": "4", "number_of_replicas": "0"}},
}
