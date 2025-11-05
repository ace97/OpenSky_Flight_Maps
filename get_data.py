import pandas as pd
from pyopensky.rest import REST
import duckdb
import os
from datetime import datetime, timezone

print("Attempting to connect to OpenSky Network...")

try:
    # 1. Fetch data from OpenSky (which returns a DataFrame)
    rest = REST()
    df = rest.states()

    # 2. Check if the DataFrame is empty
    if df is None or df.empty:
        print("No flight data received from OpenSky. Exiting.")
        exit()

    print(f"Received {len(df)} total states from API.")

    # 3. Rename columns to match your desired schema
    rename_map = {
        'timestamp': 'timestamp_utc', 
        'onground': 'on_ground', 
        'groundspeed': 'velocity',
        'track': 'track_heading', 
        'baro_altitude': 'altitude',
        'geoaltitude': 'geo_altitude'
    }
    df = df.rename(columns=rename_map)

    # 4. Filter data
    df = df.dropna(subset=['latitude', 'longitude'])

    if df.empty:
        print("No usable flight data after filtering. Exiting.")
        exit()

    # 5. Add our own 'updated_at_utc' timestamp
    df['updated_at_utc'] = datetime.now(timezone.utc)
    
    # 6. Define the final 18 columns for the database
    columns_to_keep = [
        "icao24", "callsign", "origin_country", "last_position",
        "timestamp_utc", "longitude", "latitude", "altitude",
        "on_ground", "velocity", "track_heading", "vertical_rate",
        "geo_altitude", "squawk", "spi", "position_source",
        "updated_at_utc" # This is our custom timestamp column
    ]
    
    # Filter for columns that actually exist in the DataFrame
    final_columns = [col for col in columns_to_keep if col in df.columns]
    
    # FIX FOR SettingWithCopyWarning: Use .copy()
    df_final = df[final_columns].copy()
    
    # Clean callsign
    if 'callsign' in df_final.columns:
        df_final['callsign'] = df_final['callsign'].str.strip()

    print(f"Prepared {len(df_final)} flights for database insert.")
    print(f"DataFrame columns ({len(df_final.columns)}): {list(df_final.columns)}")

    # 7. Connect to MotherDuck
    token = os.environ.get("MOTHERDUCK_TOKEN")
    if not token:
        print("Error: MOTHERDUCK_TOKEN not found in GitHub Secrets.")
        exit(1)
        
    con = duckdb.connect(f'md:?motherduck_token={token}')
    print("Successfully connected to MotherDuck.")

    # 8. Ensure the table exists (This MUST now have 17 columns + 1 update timestamp = 18 total)
    # The schema below has been corrected to explicitly include all 18 columns 
    # that are present in df_final.
    con.sql("""
        CREATE TABLE IF NOT EXISTS flights.main.flight_data (
            icao24 VARCHAR,
            callsign VARCHAR,
            origin_country VARCHAR,
            last_position BIGINT,
            timestamp_utc BIGINT,
            longitude DOUBLE,
            latitude DOUBLE,
            altitude DOUBLE,
            on_ground BOOLEAN,
            velocity DOUBLE,
            track_heading DOUBLE,
            vertical_rate DOUBLE,
            geo_altitude DOUBLE,
            squawk VARCHAR,
            spi BOOLEAN,
            position_source DOUBLE,
            updated_at_utc TIMESTAMPTZ
        )
    """)
    print("Table 'flights.main.flight_data' is ready.")

    # 9. Insert the data.
    con.sql("INSERT INTO flights.main.flight_data SELECT * FROM df_final")
    
    print(f"Successfully saved {len(df_final)} flights to MotherDuck.")
    con.close()

except Exception as e:
    print(f"An error occurred: {e}")
    import traceback
    traceback.print_exc()