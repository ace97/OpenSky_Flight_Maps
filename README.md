# ✈️ OpenSky Flight Tracker

This project is a real-time, serverless flight tracker that visualizes live aircraft positions on an interactive global map.

It automatically fetches data from the [OpenSky Network](https://opensky-network.org/), stores the historical data in a cloud database, and displays the most recent position for every tracked aircraft in a web app.

## 🚀 Live Demo

**(Link to Streamlit app)**

[![Streamlit App](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://openskyflightmaps.streamlit.app/)

## 🛠️ Tech Stack

This project is 100% free to build and host, using a modern serverless data stack:

- **Data Ingestion (ETL):** [GitHub Actions](https://github.com/features/actions)
- **Data Source:** [OpenSky Network API](https://opensky-network.org/apidoc/rest.html)
- **Python Library:** `pyopensky`
- **Database (DWH):** [MotherDuck](https://motherduck.com/) (Serverless DuckDB)
- **Frontend & Hosting:** [Streamlit Community Cloud](https://streamlit.io/cloud)

## 🏗️ Architecture

The application works in a continuous loop, separated into two parts:

1.  **Backend (Data Ingestion):** A GitHub Action runs on a 2-hour schedule. It executes the `get_data.py` script, which fetches live data from the OpenSky Network, cleans it, and inserts the new records into a MotherDuck database table.
2.  **Frontend (Web App):** The `app.py` Streamlit app is hosted on Streamlit Community Cloud. When a user visits the app, it runs a SQL query against the MotherDuck database to get the _single most recent location_ for every aircraft, then displays those locations on an interactive map.

## 📁 File Overview

Here is a breakdown of the key files in this repository:

### 1. `get_data.py`

This is the backend ETL (Extract, Transform, Load) script. Its job is to:

- Connect to the OpenSky Network API using `pyopensky`.
- Fetch the current state vectors for all aircraft.
- Clean the data (rename columns, filter nulls, add a timestamp).
- Connect to MotherDuck using a secure token.
- Ensure the `flights.main.flight_data` table exists.
- Insert the new batch of flight data into the table.

### 2. `app.py`

This is the complete frontend web application. Its job is to:

- Load the `MOTHERDUCK_TOKEN` from Streamlit Secrets.
- Connect to the MotherDuck database.
- Run a SQL query to fetch only the latest record from DWH.
- Handle data cleaning for visualization (e.g., filling `NaN` values).
- Provide interactive filters for **Country of Origin** and **Call Sign**.
- Display the final, filtered data on a `plotly.express.scatter_geo` map, which provides a clean global view.
- Display the latest(filtered) raw data in an expandable section.

### 3. `.github/workflows/fly.yml`

This is the GitHub Actions workflow file that automates our data pipeline.

- `on: workflow_dispatch:` Allows you to run the job manually from the GitHub "Actions" tab.
- `on: schedule: - cron: '15 */2 * * *'` Automatically triggers the script to run every 2 hour (15 minutes past the hour).
- It checks out the code, sets up Python, installs dependencies from `requirements.txt`, and finally runs the `get_data.py` script with the `MOTHERDUCK_TOKEN` provided as an environment variable.

### 4. `requirements.txt`

A simple list of the Python libraries required for the project to run (e.g., `streamlit`, `pandas`, `duckdb`, `pyopensky`, `plotly`).

## 📖 Step-by-Step Setup & Deployment Guide

Follow these steps to deploy your own version of this flight tracker.

### Step 1: Prerequisites

Create free accounts for the following services:

1.  [**GitHub**](https://github.com/): To host the code and run the Actions.
2.  [**MotherDuck**](https://motherduck.com/): To be our database.
3.  [**Streamlit Community Cloud**](https://share.streamlit.io/): To host our web app.

### Step 2: Get MotherDuck Token

1.  Log in to your MotherDuck account.
2.  A connection token is provided on the main dashboard. Copy this token. It starts with `md:?motherduck_token=...`.
3.  **You only need the token part** (the long string of characters _after_ the equals sign).

### Step 3: Fork Repository

Fork this repository to your own GitHub account.

### Step 4: Configure GitHub Secrets (for the Backend)

This allows the GitHub Action (`get_data.py`) to write data to your database.

1.  In your new GitHub repository, go to **Settings** > **Secrets and variables** > **Actions**.
2.  Click **New repository secret**.
3.  **Name:** `MOTHERDUCK_TOKEN`
4.  **Value:** Paste the MotherDuck token you copied in Step 2.

### Step 5: Manually Run the Action (Important!)

You must run the action _once_ to create the `flights.main.flight_data` table before your app can read from it.

1.  In your repository, go to the **Actions** tab.
2.  On the left, click on **"Fetch Flight Data"**.
3.  You will see a message: "This workflow has a workflow_dispatch event." Click the **Run workflow** button on the right.
4.  Wait for the job to complete successfully. Your MotherDuck database is now populated.

### Step 6: Deploy to Streamlit Cloud (for the Frontend)

1.  Log in to [Streamlit Community Cloud](https://share.streamlit.io/) with your GitHub account.
2.  Click **"New app"** and choose **"From existing repo"**.
3.  **Repo:** Select the repository you just forked.
4.  **Branch:** `main` (or `master`).
5.  **Main file path:** `app.py`
6.  Click **"Deploy!"**. The app will try to run but will fail on the first attempt (this is expected).

### Step 7: Configure Streamlit Secrets (for the Frontend)

This allows your Streamlit app (`app.py`) to read from your database.

1.  Your app will likely show an error. In the bottom-right corner, click **Manage app**.
2.  Go to the **Settings** (gear icon) page.
3.  Go to the **Secrets** section.
4.  Paste the following text into the secrets box:
    ```toml
    MOTHERDUCK_TOKEN = "your_motherduck_token_goes_here"
    ```
5.  Replace `your_motherduck_token_goes_here` with the same token from Step 2.
6.  Click **Save**. Streamlit will reboot your app.

### Step 8: You're Live!

Your app should now be fully functional. It will display the flight data from your MotherDuck database, which will be automatically updated every 15 minutes by your GitHub Action.

## 📈 Possible Next Steps

- **Data Retention:** Add a second GitHub Action (`.github/workflows/cleanup.yml`) that runs daily to delete data older than 7 days, keeping the database size manageable.
- **More Analytics:** Add new charts to `app.py` to show "Top 10 Origin Countries" or "Altitude Distribution" using the `df_final` DataFrame.
- **Alerts:** Use the historical data to find flights that have changed their `squawk` code to an emergency value (e.g., 7700).
