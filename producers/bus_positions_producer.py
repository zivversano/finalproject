"""
producers/bus_positions_producer.py
Fetches real-time bus positions from Israel MOT GTFS-RT feed.
→ Kafka topic: bus-positions
Runs every 30 seconds.

Data source: https://gtfs.mot.gov.il/gtfsfiles/VehiclePositions.pb
Protocol Buffers (GTFS Realtime format - standard public API)
"""

import requests
from google.transit import gtfs_realtime_pb2
import sys
sys.path.append("..")
from config.settings import GTFS_RT_VEHICLE_URL, KAFKA_TOPICS, OPERATORS
from producers.base_producer import BaseProducer


class BusPositionsProducer(BaseProducer):

    def __init__(self):
        super().__init__("bus_positions")
        self.url = GTFS_RT_VEHICLE_URL

    def get_topic(self) -> str:
        return KAFKA_TOPICS["bus_positions"]

    def fetch_data(self) -> list:
        """
        Download and parse GTFS-RT VehiclePositions protobuf.
        Returns list of normalized vehicle position dicts.
        """
        try:
            response = requests.get(self.url, timeout=15)
            response.raise_for_status()
        except requests.RequestException as e:
            self.logger.error(f"Failed to fetch VehiclePositions: {e}")
            return []

        feed = gtfs_realtime_pb2.FeedMessage()
        try:
            feed.ParseFromString(response.content)
        except Exception as e:
            self.logger.error(f"Failed to parse protobuf: {e}")
            return []

        records = []
        for entity in feed.entity:
            if not entity.HasField("vehicle"):
                continue
            vehicle = entity.vehicle
            record = self._normalize(entity.id, vehicle, feed.header.timestamp)
            if record:
                records.append(record)

        self.logger.info(f"Parsed {len(records)} bus positions from GTFS-RT feed")
        return records

    def _normalize(self, entity_id: str, vehicle, feed_timestamp: int) -> dict:
        """Normalize GTFS-RT VehiclePosition to flat dict."""
        try:
            pos     = vehicle.position
            trip    = vehicle.trip
            vd      = vehicle.vehicle

            # Filter out non-bus operators (skip trains handled separately)
            operator_id = trip.route_id.split("-")[0] if trip.route_id else ""

            return {
                "vehicle_id":       vd.id or entity_id,
                "entity_id":        entity_id,
                "trip_id":          trip.trip_id,
                "route_id":         trip.route_id,
                "operator_id":      operator_id,
                "operator_name":    self._get_operator_name(operator_id),
                "direction_id":     trip.direction_id,
                "start_date":       trip.start_date,
                "latitude":         round(pos.latitude, 6),
                "longitude":        round(pos.longitude, 6),
                "bearing":          round(pos.bearing, 1) if pos.bearing else None,
                "speed_kmh":        round(pos.speed * 3.6, 1) if pos.speed else None,
                "current_stop_seq": vehicle.current_stop_sequence,
                "stop_id":          vehicle.stop_id,
                "current_status":   self._stop_status(vehicle.current_status),
                "timestamp":        vehicle.timestamp or feed_timestamp,
                "congestion_level": vehicle.congestion_level,
            }
        except Exception as e:
            self.logger.warning(f"Failed to normalize entity {entity_id}: {e}")
            return None

    def _get_operator_name(self, operator_id: str) -> str:
        reverse = {v: k for k, v in OPERATORS.items()}
        return reverse.get(operator_id, f"operator_{operator_id}")

    def _stop_status(self, status_code: int) -> str:
        return {0: "incoming", 1: "stopped", 2: "in_transit"}.get(status_code, "unknown")


if __name__ == "__main__":
    producer = BusPositionsProducer()
    try:
        producer.run_once()
    finally:
        producer.close()