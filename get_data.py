import pandas as pd
from pyopensky.rest import REST
import duckdb
import os
from datetime import datetime

print("Attempting to connect to OpenSky Network...")

try:
    # 1. Fetch data from OpenSky
    rest = REST()
    states = rest.states()
    
    flight_data = []
    if states and states.states:
        for s in states.states:
            flight_data.append({
                "icao24": s.icao24,
                "callsign": s.callsign.strip() if s.callsign else None,
                "origin_country":s.origin_country,
                "last_position":s.last_position,
                "timestamp_utc":s.timestamp,
                "longitude": s.longitude,
                "latitude": s.latitude,
                "altitude": s.baro_altitude,
                "on_ground": s.onground,
                "velocity": s.groundspeed,
                "track":s.track,
                "vertical_rate": s.vertical_rate,
                "sensors":s.sensors,
                "geoaltitude":s.geoaltitude,
                "squawk":s.squawk,
                "spi":s.spi,
                "position_source":s.position_source
            })

    if not flight_data:
        print("No flight data received. Exiting.")
        exit()

    # 2. Create DataFrame and add a timestamp
    df = pd.DataFrame(flight_data)
    df = df.dropna(subset=['latitude', 'longitude'])
    #df = df[df['on_ground'] == False] to filter planes onground
    df['updated_at_utc'] = datetime.now(datetime.UTC) # Add a timestamp
    
    print(f"Fetched {len(df)} airborne flights.")

    # 3. Connect to MotherDuck using the token from GitHub Secrets
    token = os.environ.get("MOTHERDUCK_TOKEN")
    if not token:
        print("Error: MOTHERDUCK_TOKEN not found.")
        exit(1)
        
    con = duckdb.connect(f'md:?motherduck_token={token}')

    # 4. Create/replace the table with the new data
    # This replaces the entire table every time for simplicity.
    con.sql("INSERT INTO flights.main.flight_data SELECT * FROM df")
    
    print(f"Successfully saved {len(df)} flights to MotherDuck.")
    con.close()

except Exception as e:
    print(f"An error occurred: {e}")