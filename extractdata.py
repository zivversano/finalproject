import requests
import pandas as pd
from google.transit import gtfs_realtime_pb2
from datetime import datetime
import sys

GTFS_RT_URL = "https://gtfs.mot.gov.il/gtfsrt/realtimeVehiclePositions.pb"
# Alternative URL with route filter (sometimes works better)
GTFS_RT_URL_ALT = "https://gtfs.mot.gov.il/gtfsrt/VehiclePositions.pb"

def fetch_gtfs_rt():
    """Downloads the GTFS-RT file in Protobuf format"""
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
            
            print(f"Response status: {resp.status_code}")
            print(f"Content-Type: {resp.headers.get('Content-Type')}")
            print(f"Content length: {len(resp.content)} bytes")
            
            # Check if we got HTML instead of protobuf
            if resp.content.startswith(b'<!DOCTYPE') or resp.content.startswith(b'<html'):
                print(f"❌ Received HTML instead of protobuf data from {url}")
                continue
            
            print(f"✅ Successfully fetched protobuf data")
            return resp.content
        except Exception as e:
            print(f"❌ Error with {url}: {e}")
            continue
    
    # If both URLs failed, provide helpful error message
    print("\n" + "="*60)
    print("⚠️  Could not fetch real-time data from Israel MOT API")
    print("="*60)
    print("\nPossible reasons:")
    print("1. The API might require registration/authentication")
    print("2. The API endpoints may have changed")
    print("3. Access might be restricted from your location")
    print("\nPlease check:")
    print("- https://www.gov.il/he/departments/general/gtfs_general_transit_feed_specifications")
    print("- https://open-bus-stride-api.hasadna.org.il/ (alternative)")
    sys.exit(1)

def parse_vehicle_positions(pb_data):
    """Parses the Protobuf and returns a list of dictionaries"""
    feed = gtfs_realtime_pb2.FeedMessage()
    try:
        feed.ParseFromString(pb_data)
    except Exception as e:
        print(f"Error parsing protobuf: {e}")
        print(f"Data type: {type(pb_data)}")
        print(f"Data length: {len(pb_data)}")
        raise

    rows = []
    for entity in feed.entity:
        if not entity.HasField("vehicle"):
            continue

        v = entity.vehicle

        rows.append({
            "vehicle_id": v.vehicle.id,
            "trip_id": v.trip.trip_id,
            "route_id": v.trip.route_id,
            "latitude": v.position.latitude,
            "longitude": v.position.longitude,
            "bearing": v.position.bearing,
            "speed": v.position.speed,
            "timestamp": datetime.utcfromtimestamp(v.timestamp).isoformat() if v.timestamp else None
        })

    return rows

def main():
    print("Downloading real-time bus data...")
    
    try:
        pb_data = fetch_gtfs_rt()
        print("Parsing data...")
        rows = parse_vehicle_positions(pb_data)
    except SystemExit:
        # API not available, use sample data for demonstration
        print("\n📝 Using sample data for demonstration...")
        rows = create_sample_data()

    # Relational table (DataFrame)
    df = pd.DataFrame(rows)
    print("\n📌 Relational table (first 5 rows):")
    print(df.head())
    
    print(f"\n📊 Total vehicles: {len(df)}")

    # Save as JSON
    df.to_json("bus_positions.json", orient="records", indent=2, force_ascii=False)
    print("\n📁 JSON file saved as bus_positions.json")

def create_sample_data():
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
            "latitude": base_lat + random.uniform(-0.1, 0.1),
            "longitude": base_lon + random.uniform(-0.1, 0.1),
            "bearing": random.uniform(0, 360),
            "speed": random.uniform(0, 60),
            "timestamp": (datetime.now() - timedelta(minutes=random.randint(0, 30))).isoformat()
        })
    
    return rows

if __name__ == "__main__":
    main()
