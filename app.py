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
    """Connects to MotherDuck and fetches the *latest* flight data."""
    try:
        token = st.secrets["MOTHERDUCK_TOKEN"]
        con = duckdb.connect(f'md:?motherduck_token={token}')
        
        # --- SQL Query to get the latest state for each aircraft ---
        sql_query = """
            WITH RankedFlights AS (
                SELECT 
                    *,
                    ROW_NUMBER() OVER(
                        PARTITION BY icao24 
                        ORDER BY updated_at_utc DESC
                    ) as rn
                FROM flights.main.flight_data
            )
            SELECT * FROM RankedFlights WHERE rn = 1
        """
        
        df = con.sql(sql_query).df()
        
        # Get the last update time
        last_updated_utc = con.sql("SELECT max(updated_at_utc) FROM flights.main.flight_data").fetchone()[0]
        
        con.close()
        return df, last_updated_utc
        
    except (duckdb.Error, TypeError, KeyError) as e:
        print(f"Error loading data: {e}")
        return pd.DataFrame(), None

# --- Load Data ---
df, last_updated = load_data()

display_time = "Never"
if last_updated:
    if isinstance(last_updated, str):
        last_updated = datetime.fromisoformat(last_updated)
    display_time = last_updated.strftime('%Y-%m-%d %I:%M:%S %p UTC')

# --- Main Application ---
st.title("✈️ Live Flight Tracker (Historical)")
st.markdown(f"Displaying data from OpenSky. Last Data Point: **{display_time}**")

if df.empty:
    st.warning("No flight data is currently available.")
else:
    # --- CRITICAL FIX: HANDLE NULL VALUES FOR PLOTTING ---
    # Plotly/Streamlit crashes if it encounters NaN in columns used for plotting.
    numeric_cols = ['latitude', 'longitude', 'altitude', 'velocity', 'track_heading']
    
    # Fill NaN values in critical numeric columns with 0
    df[numeric_cols] = df[numeric_cols].fillna(0)
    
    # Fill missing callsigns with a default string
    df['callsign'] = df['callsign'].fillna('N/A')
    
    # --- Create the Map ---
    st.subheader("Global Flight Map")

    fig = px.scatter_mapbox(
        df,
        lat="latitude",
        lon="longitude",
        hover_name="callsign",
        hover_data={
            "altitude": ":.0f ft",
            "velocity": ":.0f m/s",
            "track_heading": ":.0f°",
            "latitude": False,
            "longitude": False
        },
        color_discrete_sequence=["#00BFFF"],
        zoom=1,
        height=600,
    )
    
    fig.update_layout(
        mapbox_style="carto-positron",
        margin={"r": 0, "t": 0, "l": 0, "b": 0},
    )
    
    st.plotly_chart(fig, use_container_width=True)

    # --- Show Raw Data ---
    with st.expander("Show Latest State for All Flights"):
        st.dataframe(df)