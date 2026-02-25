import requests
import zipfile
import io
import pandas as pd
from google.transit import gtfs_realtime_pb2
from geopy.distance import geodesic
from datetime import datetime
import sys

GTFS_RT_URL = "https://gtfs.mot.gov.il/gtfsrt/realtimeVehiclePositions.pb"
# Alternative URL with route filter (sometimes works better)
GTFS_RT_URL_ALT = "https://gtfs.mot.gov.il/gtfsrt/VehiclePositions.pb"
GTFS_STATIC_URL = "https://gtfs.mot.gov.il/gtfsfiles/gtfs.zip"


# -----------------------------
# 1. Fetch bus positions (GTFS-RT)
# -----------------------------
def fetch_bus_positions():
    """Downloads and parses real-time bus positions"""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'application/x-protobuf,application/octet-stream'
    }
    
    # Try primary URL
    for url in [GTFS_RT_URL, GTFS_RT_URL_ALT]:
        try:
            print(f"Trying URL: {url}")
            resp = requests.get(url, headers=headers, timeout=10)
            resp.raise_for_status()
            
            # Check if we got HTML instead of protobuf
            if resp.content.startswith(b'<!DOCTYPE') or resp.content.startswith(b'<html'):
                print(f"❌ Received HTML instead of protobuf data from {url}")
                continue
            
            feed = gtfs_realtime_pb2.FeedMessage()
            feed.ParseFromString(resp.content)

            rows = []
            for entity in feed.entity:
                if not entity.HasField("vehicle"):
                    continue

                v = entity.vehicle

                rows.append({
                    "vehicle_id": v.vehicle.id,
                    "trip_id": v.trip.trip_id,
                    "route_id": v.trip.route_id,
                    "lat": v.position.latitude,
                    "lon": v.position.longitude,
                    "timestamp": datetime.utcfromtimestamp(v.timestamp).isoformat() if v.timestamp else None
                })

            print(f"✅ Successfully fetched {len(rows)} bus positions")
            return pd.DataFrame(rows)
            
        except Exception as e:
            print(f"❌ Error with {url}: {e}")
            continue
    
    # If both URLs failed, use sample data
    print("⚠️  Using sample data for demonstration...")
    return create_sample_bus_data()


# -----------------------------
# 2. Fetch bus stops (GTFS Static)
# -----------------------------
def fetch_stops():
    """Downloads and extracts bus stops from GTFS static data"""
    print("מוריד נתוני תחנות...")
    resp = requests.get(GTFS_STATIC_URL, timeout=20)
    resp.raise_for_status()

    with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
        with z.open("stops.txt") as f:
            df = pd.read_csv(f)

    df = df.rename(columns={
        "stop_lat": "stop_lat",
        "stop_lon": "stop_lon",
        "stop_id": "stop_id",
        "stop_name": "stop_name"
    })

    return df[["stop_id", "stop_name", "stop_lat", "stop_lon"]]


# -----------------------------
# 3. Match each bus to nearest stop
# -----------------------------
def find_nearest_stop(bus_row, stops_df):
    """Finds the nearest bus stop for a given bus position"""
    bus_loc = (bus_row.lat, bus_row.lon)

    # מחשבים מרחק לכל תחנה
    stops_df["distance_m"] = stops_df.apply(
        lambda row: geodesic(bus_loc, (row.stop_lat, row.stop_lon)).meters,
        axis=1
    )

    # מוצאים את התחנה הקרובה ביותר
    nearest = stops_df.loc[stops_df["distance_m"].idxmin()]

    return pd.Series({
        "nearest_stop_id": nearest.stop_id,
        "nearest_stop_name": nearest.stop_name,
        "distance_to_stop_m": nearest.distance_m
    })


def create_sample_bus_data():
    """Creates sample vehicle position data for demonstration"""
    from datetime import datetime, timedelta
    import random
    
    # Sample data for Tel Aviv area
    base_lat, base_lon = 32.0853, 34.7818
    
    rows = []
    for i in range(20):
        rows.append({
            "vehicle_id": f"BUS_{1000 + i}",
            "trip_id": f"TRIP_{5000 + i}",
            "route_id": f"ROUTE_{random.randint(1, 50)}",
            "lat": base_lat + random.uniform(-0.1, 0.1),
            "lon": base_lon + random.uniform(-0.1, 0.1),
            "timestamp": (datetime.now() - timedelta(minutes=random.randint(0, 30))).isoformat()
        })
    
    return pd.DataFrame(rows)



# -----------------------------
# MAIN
# -----------------------------
def main():
    print("מוריד נתוני אוטובוסים...")
    buses = fetch_bus_positions()

    print("מוריד נתוני תחנות...")
    stops = fetch_stops()

    print("מחשב תחנה קרובה לכל אוטובוס...")
    merged = buses.join(
        buses.apply(lambda row: find_nearest_stop(row, stops.copy()), axis=1)
    )

    print("\n📌 טבלה רלציונית (5 שורות ראשונות):")
    print(merged.head())

    merged.to_json("buses_with_nearest_stops.json", orient="records", indent=2, force_ascii=False)
    print("\n📁 נשמר קובץ JSON בשם buses_with_nearest_stops.json")


if __name__ == "__main__":
    main()
