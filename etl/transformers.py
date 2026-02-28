"""
etl/transformers.py
All ETL transformation logic for Israel Transit data.
Cleaning, enrichment, delay classification, KPI calculations.
"""

import re
import logging
from datetime import datetime, timezone

logger = logging.getLogger("etl.transformers")

# ─────────────────────────────────────────
# ISRAEL GEOGRAPHIC BOUNDING BOX
# ─────────────────────────────────────────
ISRAEL_BOUNDS = {
    "lat_min": 29.5,  "lat_max": 33.3,
    "lon_min": 34.2,  "lon_max": 35.9,
}

# ─────────────────────────────────────────
# ROUTE TYPE MAPPING (GTFS standard)
# ─────────────────────────────────────────
ROUTE_TYPE_MAP = {
    0: "tram",     1: "subway",   2: "rail",
    3: "bus",      4: "ferry",    5: "cable_car",
    6: "gondola",  7: "funicular",
}

# ─────────────────────────────────────────
# TIME PERIOD CLASSIFICATION
# ─────────────────────────────────────────
def classify_time_period(hour: int) -> str:
    """Classify hour into transit service periods."""
    if 6 <= hour < 9:    return "morning_rush"   # פיק בוקר
    if 9 <= hour < 12:   return "mid_morning"
    if 12 <= hour < 15:  return "midday"
    if 15 <= hour < 19:  return "evening_rush"   # פיק ערב
    if 19 <= hour < 23:  return "evening"
    return "off_peak"                             # לילה / מוקדם


class BusPositionTransformer:
    """Transforms and validates raw bus position data."""

    def transform(self, data: dict) -> dict:
        lat = data.get("latitude", 0)
        lon = data.get("longitude", 0)
        now = datetime.now(timezone.utc)

        return {
            "vehicle_id":       self._clean_id(data.get("vehicle_id", "")),
            "trip_id":          self._clean_id(data.get("trip_id", "")),
            "route_id":         data.get("route_id", ""),
            "route_short_name": self._extract_route_name(data.get("route_id", "")),
            "operator_id":      data.get("operator_id", ""),
            "operator_name":    data.get("operator_name", ""),
            "direction_id":     data.get("direction_id"),
            "latitude":         lat,
            "longitude":        lon,
            "is_valid_location": self._is_in_israel(lat, lon),
            "bearing":          data.get("bearing"),
            "speed_kmh":        data.get("speed_kmh"),
            "speed_category":   self._speed_category(data.get("speed_kmh")),
            "stop_id":          data.get("stop_id", ""),
            "current_status":   data.get("current_status", "unknown"),
            "timestamp":        data.get("timestamp"),
            "hour_of_day":      now.hour,
            "time_period":      classify_time_period(now.hour),
            "day_of_week":      now.strftime("%A"),
            "is_weekend":       now.weekday() >= 4,   # Friday=4, Saturday=5 in Israel
            "processed_at":     now.isoformat(),
        }

    def _clean_id(self, val: str) -> str:
        return str(val).strip() if val else ""

    def _extract_route_name(self, route_id: str) -> str:
        """Extract human-readable route number from GTFS route_id."""
        if not route_id:
            return ""
        # Israel GTFS route IDs often formatted as "OPERATOR-ROUTENUMBER-..."
        parts = route_id.split("-")
        return parts[1] if len(parts) > 1 else route_id[:10]

    def _is_in_israel(self, lat: float, lon: float) -> bool:
        return (ISRAEL_BOUNDS["lat_min"] <= lat <= ISRAEL_BOUNDS["lat_max"] and
                ISRAEL_BOUNDS["lon_min"] <= lon <= ISRAEL_BOUNDS["lon_max"])

    def _speed_category(self, speed_kmh) -> str:
        if speed_kmh is None: return "unknown"
        if speed_kmh == 0:    return "stopped"
        if speed_kmh < 20:    return "slow"
        if speed_kmh < 50:    return "normal"
        if speed_kmh < 80:    return "fast"
        return "very_fast"


class TripUpdateTransformer:
    """Transforms trip update records and calculates delay KPIs."""

    def transform(self, data: dict) -> dict:
        delay_sec = data.get("delay_seconds", 0) or 0
        now       = datetime.now(timezone.utc)

        return {
            "trip_id":            data.get("trip_id", ""),
            "route_id":           data.get("route_id", ""),
            "route_short_name":   self._extract_route_name(data.get("route_id", "")),
            "direction_id":       data.get("direction_id"),
            "start_date":         data.get("start_date", ""),
            "start_time":         data.get("start_time", ""),
            "stop_id":            data.get("stop_id", ""),
            "stop_sequence":      data.get("stop_sequence"),
            "arrival_delay_sec":  data.get("arrival_delay"),
            "departure_delay_sec": data.get("departure_delay"),
            "delay_seconds":      delay_sec,
            "delay_minutes":      round(delay_sec / 60, 1),
            "delay_category":     self._delay_category(delay_sec),
            "is_delayed":         data.get("is_delayed", False),
            "is_cancelled":       data.get("is_cancelled", False),
            "is_early":           delay_sec < -60,
            "schedule_relationship": data.get("schedule_relationship", "scheduled"),
            "hour_of_day":        now.hour,
            "time_period":        classify_time_period(now.hour),
            "day_of_week":        now.strftime("%A"),
            "is_weekend":         now.weekday() >= 4,
            "processed_at":       now.isoformat(),
        }

    def _extract_route_name(self, route_id: str) -> str:
        if not route_id: return ""
        parts = route_id.split("-")
        return parts[1] if len(parts) > 1 else route_id[:10]

    def _delay_category(self, delay_sec: int) -> str:
        if delay_sec <= 0:    return "on_time_or_early"
        if delay_sec < 180:   return "minor"          # < 3 min
        if delay_sec < 600:   return "moderate"       # 3-10 min
        if delay_sec < 1800:  return "severe"         # 10-30 min
        return "critical"                             # > 30 min


class TrainPositionTransformer:
    """Transforms Israel Railways data."""

    def transform(self, data: dict) -> dict:
        delay_min = data.get("delay_minutes") or 0
        now       = datetime.now(timezone.utc)

        return {
            "train_number":          data.get("train_number", ""),
            "origin_station_id":     data.get("origin_station_id", ""),
            "dest_station_id":       data.get("dest_station_id", ""),
            "queried_station_id":    data.get("queried_station_id", ""),
            "queried_station_name":  data.get("queried_station_name", ""),
            "platform":              data.get("platform"),
            "scheduled_departure":   data.get("scheduled_departure", ""),
            "actual_departure":      data.get("actual_departure", ""),
            "scheduled_arrival":     data.get("scheduled_arrival", ""),
            "actual_arrival":        data.get("actual_arrival", ""),
            "delay_minutes":         delay_min,
            "delay_seconds":         delay_min * 60,
            "delay_category":        self._delay_category(delay_min),
            "is_delayed":            data.get("is_delayed", False),
            "is_cancelled":          data.get("is_cancelled", False),
            "operator":              "israel_railways",
            "hour_of_day":           now.hour,
            "time_period":           classify_time_period(now.hour),
            "day_of_week":           now.strftime("%A"),
            "is_weekend":            now.weekday() >= 4,
            "processed_at":          now.isoformat(),
        }

    def _delay_category(self, delay_min: int) -> str:
        if delay_min <= 0:    return "on_time_or_early"
        if delay_min < 3:     return "minor"
        if delay_min < 10:    return "moderate"
        if delay_min < 30:    return "severe"
        return "critical"


class ServiceAlertTransformer:
    """Transforms and enriches service alert records."""

    def transform(self, data: dict) -> dict:
        now = datetime.now(timezone.utc)

        # Determine if currently active
        now_ts       = int(now.timestamp())
        active_start = data.get("active_start")
        active_end   = data.get("active_end")
        is_active    = (
            (active_start is None or now_ts >= active_start) and
            (active_end is None   or now_ts <= active_end)
        )

        return {
            "alert_id":           data.get("alert_id", ""),
            "cause":              data.get("cause", "unknown"),
            "effect":             data.get("effect", "unknown"),
            "severity":           data.get("severity", "low"),
            "header_text":        (data.get("header_text") or "")[:500],
            "description_text":   (data.get("description_text") or "")[:1000],
            "url":                data.get("url", ""),
            "is_active":          is_active,
            "active_start":       active_start,
            "active_end":         active_end,
            "affected_routes_count": data.get("routes_count", 0),
            "affected_stops_count":  data.get("stops_count", 0),
            "affected_routes":    ",".join(data.get("affected_routes", []))[:500],
            "affected_agencies":  ",".join(data.get("affected_agencies", []))[:200],
            "impact_score":       self._impact_score(data),
            "hour_of_day":        now.hour,
            "time_period":        classify_time_period(now.hour),
            "processed_at":       now.isoformat(),
        }

    def _impact_score(self, data: dict) -> float:
        """
        Composite impact score (0-100):
        severity + number of routes/stops affected.
        """
        severity_weight = {"high": 50, "medium": 25, "low": 10}.get(data.get("severity", "low"), 10)
        routes_weight   = min(data.get("routes_count", 0) * 2, 30)
        stops_weight    = min(data.get("stops_count",  0) * 0.5, 20)
        return round(severity_weight + routes_weight + stops_weight, 1)