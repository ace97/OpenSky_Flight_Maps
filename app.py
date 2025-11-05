import streamlit as st
import pandas as pd
import plotly.express as px
import duckdb
from datetime import datetime

# --- Page Configuration ---
st.set_page_config(
    page_title="Live Flight Tracker",
    page_icon="✈️",
    layout="wide",
)

# --- Helper Function ---
@st.cache_data(ttl=60)  # Cache the data for 60 seconds
def load_data():
    """Connects to MotherDuck and fetches the flight data."""
    try:
        # Get the token from Streamlit Secrets
        token = st.secrets["MOTHERDUCK_TOKEN"]
        
        # Connect to MotherDuck
        con = duckdb.connect(f'md:?motherduck_token={token}')
        
        # Query the data
        df = con.sql("SELECT * FROM flights.main.flight_data").df()
        
        # Get the last update time
        last_updated_utc = con.sql("SELECT max(updated_at_utc) FROM flights.main.flight_data").fetchone()[0]
        
        con.close()
        return df, last_updated_utc
        
    except (duckdb.Error, TypeError, KeyError) as e:
        # Handle errors (e.g., table not created yet or secret not set)
        print(f"Error loading data: {e}")
        return pd.DataFrame(), None

# --- Load Data ---
df, last_updated = load_data()

display_time = "Never"
if last_updated:
    # Convert from string/datetime to a more readable format
    if isinstance(last_updated, str):
        last_updated = datetime.fromisoformat(last_updated)
    display_time = last_updated.strftime('%Y-%m-%d %I:%M:%S %p UTC')


# --- Main Application ---
st.title("✈️ Live Flight Tracker (via MotherDuck)")
st.markdown(f"Displaying flight data from the OpenSky Network. Last Updated: **{display_time}**")

if df.empty:
    st.warning("No flight data is currently available. The data fetcher might be running for the first time or failed.")
else:
    # --- Create the Map ---
    st.subheader("Global Flight Map")
    df['callsign'] = df['callsign'].fillna('N/A')

    fig = px.scatter_mapbox(
        df,
        lat="latitude",
        lon="longitude",
        hover_name="callsign",
        hover_data={
            "altitude": ":.0f ft",
            "velocity": ":.0f m/s",
            "track": ":.0f°",
            "latitude": False,
            "longitude": False
        },
        color_discrete_sequence=["#00BFFF"], # Deep sky blue
        zoom=1,
        height=600,
    )
    
    fig.update_layout(
        mapbox_style="carto-positron", # A clean, light map style
        margin={"r": 0, "t": 0, "l": 0, "b": 0},
    )
    
    st.plotly_chart(fig, use_container_width=True)

    # --- Show Raw Data ---
    with st.expander("Show Raw Flight Data (from MotherDuck)"):
        st.dataframe(df)