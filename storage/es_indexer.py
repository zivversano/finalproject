"""
storage/es_indexer.py
Kafka → Elasticsearch real-time indexer.
Consumes from Kafka topics and bulk-indexes records into Elasticsearch
so Kibana can visualize live transit data.

Indices created:
  - transit-bus-positions
  - transit-trip-updates
  - transit-train-positions
  - transit-service-alerts
"""

import json
import os
import logging
from datetime import datetime, timezone

from kafka import KafkaConsumer
from elasticsearch import Elasticsearch, helpers

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

ES_HOST     = os.getenv("ELASTICSEARCH_HOST", "http://localhost:9200")
KAFKA_BROKERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

# Map Kafka topic keys → Elasticsearch index names
TOPIC_INDEX_MAP = {
    "bus_positions":    "transit-bus-positions",
    "trip_updates":     "transit-trip-updates",
    "train_positions":  "transit-train-positions",
    "service_alerts":   "transit-service-alerts",
    "delay_events":     "transit-delay-events",
}

# Index settings applied at creation time (if index doesn't exist)
INDEX_SETTINGS = {
    "settings": {
        "number_of_shards":   1,
        "number_of_replicas": 0,
        "refresh_interval":   "5s",
    },
    "mappings": {
        "dynamic": "true",
        "properties": {
            "timestamp":     {"type": "date", "ignore_malformed": True},
            "recorded_at":   {"type": "date", "ignore_malformed": True},
            "latitude":      {"type": "float"},
            "longitude":     {"type": "float"},
            "location":      {"type": "geo_point"},
            "delay_minutes": {"type": "float"},
            "operator_name": {"type": "keyword"},
            "route_id":      {"type": "keyword"},
            "vehicle_id":    {"type": "keyword"},
            "line_ref":      {"type": "keyword"},
            "trip_id":       {"type": "keyword"},
            "stop_id":       {"type": "keyword"},
        }
    }
}


def get_es_client() -> Elasticsearch:
    return Elasticsearch(ES_HOST, request_timeout=10)


def ensure_index(es: Elasticsearch, index_name: str) -> None:
    """Create the index with mappings if it doesn't exist yet."""
    if not es.indices.exists(index=index_name):
        es.indices.create(index=index_name, body=INDEX_SETTINGS)
        log.info(f"Created Elasticsearch index: {index_name}")


def _add_geo_point(doc: dict) -> dict:
    """If lat/lon present, add a geo_point field for Kibana Maps."""
    lat = doc.get("latitude") or doc.get("lat")
    lon = doc.get("longitude") or doc.get("lon")
    if lat and lon:
        try:
            doc["location"] = {"lat": float(lat), "lon": float(lon)}
        except (TypeError, ValueError):
            pass
    return doc


def index_topic(topic_key: str, max_msgs: int = 2000, consumer_timeout_ms: int = 5000) -> int:
    """
    Consume up to `max_msgs` messages from a Kafka topic and bulk-index
    them into the matching Elasticsearch index.

    Returns the number of documents indexed.
    """
    from config.settings import KAFKA_TOPICS

    kafka_topic  = KAFKA_TOPICS[topic_key]
    index_name   = TOPIC_INDEX_MAP[topic_key]
    group_id     = f"es-indexer-{topic_key}"

    es = get_es_client()
    ensure_index(es, index_name)

    consumer = KafkaConsumer(
        kafka_topic,
        bootstrap_servers=KAFKA_BROKERS,
        group_id=group_id,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=consumer_timeout_ms,
        max_poll_records=500,
    )

    actions = []
    count   = 0
    for msg in consumer:
        if count >= max_msgs:
            break
        payload = msg.value
        # Payload is {"data": {...}, "fetched_at": "...", "topic": "..."}
        doc = payload.get("data", payload)
        doc["_indexed_at"] = datetime.now(timezone.utc).isoformat()
        doc = _add_geo_point(doc)
        actions.append({
            "_index": index_name,
            "_source": doc,
        })
        count += 1

    consumer.commit()
    consumer.close()

    if actions:
        success, errors = helpers.bulk(es, actions, raise_on_error=False)
        if errors:
            log.warning(f"ES bulk errors for {index_name}: {errors[:3]}")
        log.info(f"Indexed {success} docs into {index_name}")
        return success

    log.info(f"No messages to index from {kafka_topic}")
    return 0


def index_all_topics(max_msgs: int = 2000) -> dict:
    """Index all transit topics. Returns a summary dict."""
    results = {}
    for topic_key in TOPIC_INDEX_MAP:
        try:
            n = index_topic(topic_key, max_msgs=max_msgs)
            results[topic_key] = n
        except Exception as e:
            log.error(f"Failed to index {topic_key}: {e}")
            results[topic_key] = 0
    return results


if __name__ == "__main__":
    import sys
    topic = sys.argv[1] if len(sys.argv) > 1 else None
    if topic:
        n = index_topic(topic)
        print(f"Indexed {n} documents into transit-{topic.replace('_', '-')}")
    else:
        results = index_all_topics()
        print("Indexing complete:", results)
