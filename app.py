import streamlit as st
import pandas as pd
import plotly.express as px
import duckdb
from datetime import datetime, timezone 

# --- Page Configuration ---
st.set_page_config(
    page_title="Live Flight Tracker",
    page_icon="✈️",
    layout="wide",
)

# --- Helper Function ---
@st.cache_data(ttl=60) # Cache the data for 60 seconds
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
        # Changed print to st.error for better Streamlit display
        st.error(f"Error loading data from MotherDuck: {e}")
        return pd.DataFrame(), None

# --- Load Data ---
df, last_updated = load_data()

display_time = "Never"
if last_updated:
    # Ensure timezone awareness for display
    if isinstance(last_updated, str):
        # Ensure we use timezone.utc from the import
        last_updated = datetime.fromisoformat(last_updated).replace(tzinfo=timezone.utc)
    elif last_updated.tzinfo is None:
        last_updated = last_updated.replace(tzinfo=timezone.utc)
        
    display_time = last_updated.strftime('%Y-%m-%d %I:%M:%S %p UTC')

# --- Main Application ---
st.title("✈️ Live Flight Tracker (Historical)")
st.markdown(f"Displaying data from OpenSky. Last Data Point: **{display_time}**")

if df.empty:
    st.warning("No flight data is currently available.")
else:
    # --- Data Cleaning for Plotting ---
    numeric_cols = ['latitude', 'longitude', 'altitude', 'velocity', 'track_heading']
    df[numeric_cols] = df[numeric_cols].fillna(0)
    df['callsign'] = df['callsign'].fillna('N/A')
    
    # --- Create the Map ---
    st.subheader("Global Flight Map")

    # Use px.scatter_geo for a clean, single, non-repeating world map
    fig = px.scatter_geo(
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
        projection="natural earth", # Use a common global projection
        height=600,
    )
    
    # Configure the Geo layout to style the map
    fig.update_geos(
        showocean=True,
        oceancolor="lightblue",
        showland=True,
        landcolor="lightgray",
        showcoastlines=True,
        coastlinecolor="DarkGray",
        showcountries=True,
        countrycolor="DarkGray",
        # Ensure the whole world is shown
        lataxis_range=[-90, 90],
        lonaxis_range=[-180, 180],
        # Remove the subplot background color/frame
        bgcolor='rgba(0,0,0,0)',
        framecolor='rgba(0,0,0,0)'
    )

    fig.update_layout(
        margin={"r": 0, "t": 0, "l": 0, "b": 0},
    )
    
    st.plotly_chart(fig, use_container_width=True)

    # --- Show Raw Data ---
    with st.expander("Show Latest State for All Flights"):
        st.dataframe(df)