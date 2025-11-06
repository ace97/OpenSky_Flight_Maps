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
            select *
            from flights.main.flight_data
            where updated_at_utc = 
            (select max(updated_at_utc) from flights.main.flight_data)
        """
        
        df = con.sql(sql_query).df()
        
        # Get the last update time
        last_updated_utc = con.sql("SELECT max(updated_at_utc) FROM flights.main.flight_data").fetchone()[0]
        
        con.close()
        return df, last_updated_utc
        
    except (duckdb.Error, TypeError, KeyError) as e:
        st.error(f"Error loading data from MotherDuck: {e}")
        return pd.DataFrame(), None

# --- Load Data ---
df, last_updated = load_data()

display_time = "Never"
if last_updated:
    # Ensure timezone awareness for display
    if isinstance(last_updated, str):
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
    
    # Ensure origin_country is treated as string and clean potential NaNs
    df['origin_country'] = df['origin_country'].astype(str).fillna('Unknown')
    
    # --- Filter Widgets ---
    st.subheader("Filter Flights")
    
    col1, col2 = st.columns(2)
    
    # 1. Country of Origin Filter
    # Get unique countries, sort them, and prepend an "All" option
    countries = sorted(df['origin_country'].unique())
    selected_country = col1.selectbox(
        "Select Country of Origin:",
        options=['All'] + countries,
        index=0
    )
    
    # 2. Call Sign Filter (only show callsigns for the selected country, if one is chosen)
    if selected_country != 'All':
        df_filtered = df[df['origin_country'] == selected_country].copy()
    else:
        df_filtered = df.copy()
        
    # Get unique callsigns from the (already filtered) data, sort, and prepend "All"
    callsigns = sorted(df_filtered['callsign'].unique())
    selected_callsign = col2.selectbox(
        "Select Call Sign:",
        options=['All'] + callsigns,
        index=0
    )
    
    # --- Apply Final Filtering ---
    if selected_callsign != 'All':
        # Filter again by callsign
        df_final = df_filtered[df_filtered['callsign'] == selected_callsign]
    else:
        # If 'All' callsigns selected, use the country-filtered data
        df_final = df_filtered
        
    # --- Display Status ---
    st.info(f"Showing **{len(df_final)}** of **{len(df)}** total flight records.")
    
    if df_final.empty:
        st.warning("No flights match the current filter selection.")
        st.stop() # Stop execution if there is no data to plot
        
    # --- Create the Map ---
    st.subheader("Global Flight Map")

    # Use px.scatter_geo for a clean, single, non-repeating world map
    fig = px.scatter_geo(
        df_final, # Use the final filtered DataFrame
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
        projection="natural earth",
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
        lataxis_range=[-90, 90],
        lonaxis_range=[-180, 180],
        bgcolor='rgba(0,0,0,0)',
        framecolor='rgba(0,0,0,0)'
    )

    fig.update_layout(
        margin={"r": 0, "t": 0, "l": 0, "b": 0},
    )
    
    st.plotly_chart(fig, use_container_width=True)

    # --- Show Raw Data ---
    with st.expander("Show Latest State for Filtered Flights"):
        # Display the filtered DataFrame, explicitly removing the internal 'rn' column
        st.dataframe(df_final.drop(columns=['rn']))