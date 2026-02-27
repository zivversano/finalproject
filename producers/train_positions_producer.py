"""
producers/train_positions_producer.py
Fetches real-time Israel Railways train data.
→ Kafka topic: train-positions
Runs every 30 seconds.

Data source:
  Primary:   https://israelrail.azurewebsites.net  (Israel Railways public API)
  Fallback:  GTFS-RT VehiclePositions (filtered for rail operator)

Israel Railways station board API returns arrivals/departures per station
with real-time delay information.
"""

import requests
from datetime import datetime, timezone
import sys
sys.path.append("..")
from config.settings import RAIL_API_URL, MAJOR_TRAIN_STATIONS, KAFKA_TOPICS, DELAY_THRESHOLD_SECONDS
from producers.base_producer import BaseProducer


class TrainPositionsProducer(BaseProducer):

    def __init__(self):
        super().__init__("train_positions")
        self.api_url  = RAIL_API_URL
        self.stations = MAJOR_TRAIN_STATIONS
        self.headers  = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

    def get_topic(self) -> str:
        return KAFKA_TOPICS["train_positions"]

    def fetch_data(self) -> list:
        """Fetch departures/arrivals for all major stations."""
        records = []
        today = datetime.now().strftime("%Y-%m-%d")

        for station_id, station_name in self.stations.items():
            station_records = self._fetch_station(station_id, station_name, today)
            records.extend(station_records)

        # Deduplicate by train number + origin station
        seen = set()
        unique = []
        for r in records:
            key = f"{r['train_number']}_{r['origin_station_id']}"
            if key not in seen:
                seen.add(key)
                unique.append(r)

        self.logger.info(
            f"Fetched {len(unique)} unique trains across {len(self.stations)} stations"
        )
        return unique

    def _fetch_station(self, station_id: str, station_name: str, date: str) -> list:
        """Fetch station board for a single station."""
        try:
            resp = requests.get(
                self.api_url,
                params={
                    "stationId": station_id,
                    "date":      date,
                    "hour":      datetime.now().hour,
                },
                headers=self.headers,
                timeout=10,
            )
            if resp.status_code == 404:
                return []
            resp.raise_for_status()

            data = resp.json()
            trains = data.get("result", {}).get("trains", []) or data.get("Data", []) or []
            return [self._normalize(t, station_id, station_name) for t in trains if t]

        except requests.RequestException as e:
            self.logger.warning(f"Station {station_name} ({station_id}) fetch failed: {e}")
            return []
        except Exception as e:
            self.logger.warning(f"Station {station_name} parse error: {e}")
            return []

    def _normalize(self, train: dict, queried_station: str, station_name: str) -> dict:
        """Normalize Israel Railways API response."""
        # Delay calculation
        scheduled_dep = train.get("departureTime") or train.get("DepartureTime", "")
        actual_dep    = train.get("actualDepartureTime") or train.get("ActualDepartureTime", "")
        delay_min     = self._calc_delay_minutes(scheduled_dep, actual_dep)

        return {
            "train_number":        str(train.get("trainno") or train.get("TrainNumber", "")),
            "origin_station_id":   str(train.get("orignStation") or train.get("OriginStation", "")),
            "dest_station_id":     str(train.get("destStation") or train.get("DestinationStation", "")),
            "queried_station_id":  queried_station,
            "queried_station_name": station_name,
            "platform":            train.get("platform") or train.get("Platform"),
            "scheduled_departure": scheduled_dep,
            "actual_departure":    actual_dep,
            "scheduled_arrival":   train.get("arrivalTime") or train.get("ArrivalTime", ""),
            "actual_arrival":      train.get("actualArrivalTime") or train.get("ActualArrivalTime", ""),
            "delay_minutes":       delay_min,
            "delay_seconds":       delay_min * 60 if delay_min else 0,
            "is_delayed":          delay_min is not None and delay_min * 60 >= DELAY_THRESHOLD_SECONDS,
            "is_cancelled":        bool(train.get("isCancelled") or train.get("IsCancelled", False)),
            "stops_count":         len(train.get("stops", [])),
            "operator":            "israel_railways",
            "timestamp":           int(datetime.now(timezone.utc).timestamp()),
        }

    def _calc_delay_minutes(self, scheduled: str, actual: str) -> int:
        """Calculate delay in minutes between two HH:MM time strings."""
        if not scheduled or not actual:
            return None
        try:
            def to_minutes(t):
                parts = t.replace(":", "").strip()
                if len(parts) >= 4:
                    return int(parts[:2]) * 60 + int(parts[2:4])
                return 0
            diff = to_minutes(actual) - to_minutes(scheduled)
            return max(diff, 0)  # negative = early, treat as 0
        except Exception:
            return None


if __name__ == "__main__":
    producer = TrainPositionsProducer()
    try:
        producer.run_once()
    finally:
        producer.close()