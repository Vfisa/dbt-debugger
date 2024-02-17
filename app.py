import json
import sys
import requests
from datetime import datetime
import numpy as np  # np mean, np random
import pandas as pd  # read csv, df manipulation
import plotly.express as px  # interactive charts
import streamlit as st

startpath = "data/in/files/"
endpath = "data/out/files/"
filename_json = "data/in/files/run_results.json"
dataset_url = "https://raw.githubusercontent.com/Vfisa/dbt-debugger/main/data/in/files/run_results.json"

# Function to load JSON data from URL into a Pandas DataFrame
def load_data_from_url(url):
    response = requests.get(url)
    data = response.json()
    df = pd.DataFrame(data["results"])
    return df

# Function to calculate time metrics
def grab_time_metrics(df):
    start_time = df["compile_started_at"].min()
    end_time = df["execute_completed_at"].max()
    duration = (end_time - start_time).total_seconds()
    start = start_time.strftime("%H:%M:%S")
    end = end_time.strftime("%H:%M:%S")
    return start, end, duration

# Function to extract values from JSON objects in the "timing" column
def extract_timing(row):
    compile_started_at = ''
    compile_completed_at = ''
    execute_started_at = ''
    execute_completed_at = ''
    for item in row:
        if item['name'] == 'compile':
            compile_started_at = item['started_at']
            compile_completed_at = item['completed_at']
        elif item['name'] == 'execute':
            execute_started_at = item['started_at']
            execute_completed_at = item['completed_at']
    return compile_started_at, compile_completed_at, execute_started_at, execute_completed_at

def preprocess_dataframe(df):
    # Apply function to the DataFrame
    df['compile_started_at'], df['compile_completed_at'], df['execute_started_at'], df['execute_completed_at'] = zip(*df['timing'].apply(extract_timing))

    # Drop the original "timing" column if necessary
    df.drop(columns=['timing', 'thread_id', 'adapter_response','message','failures', 'compiled', 'compiled_code','relation_name'], inplace=True)

    # Types
    df["type"] = df['unique_id'].str.split(".", expand=True)[0]

    # Datetime
    dt_list = ["compile_started_at","compile_completed_at","execute_started_at","execute_completed_at"]
    for a in dt_list:
        df[a] = pd.to_datetime(df[a])
    df["compile_duration"] = (df["compile_completed_at"] - df["compile_started_at"]).dt.total_seconds()
    df["execute_duration"] = (df["execute_completed_at"] - df["execute_started_at"]).dt.total_seconds()

    return df

def main():
    # Title of the app
    st.title("dbt run")

    # Button to load data in the side menu
    if st.sidebar.button("Load Artifacts"):
        # Load data from the provided URL
        df = load_data_from_url(dataset_url)

        # Preprocess the DataFrame
        df = preprocess_dataframe(df)

        # Calculate time metrics
        start, end, raw_duration = grab_time_metrics(df)
        raw_duration = round(raw_duration, 2)
        duration = round(df["execution_time"].sum(), 2)

        # create four columns for KPIs
        kpi1, kpi2, kpi3, kpi4 = st.columns(4)

        # Display KPIs
        kpi1.metric(
            label="Start",
            value=start
        )

        kpi2.metric(
            label="End",
            value=end
        )

        kpi3.metric(
            label="Raw duration",
            value=raw_duration
        )

        kpi4.metric(
            label="Duration",
            value=duration
        )

        # charting
        fig = px.timeline(df,
            x_start="execute_started_at",
            x_end="execute_completed_at",
            y="unique_id",
            color="execution_time",
            hover_data=["unique_id","execute_started_at", "execute_completed_at", "execution_time", "type"],
            labels={
                "execute_start": "start",
                "execute_end": "end",
                "unique_id": "id",
                "type": "type",
                "execution_time": "duration"
            },
            color_continuous_scale=px.colors.sequential.Redor
        )
        fig.update_yaxes(autorange="reversed")
        fig.update_xaxes(rangeslider_visible=True)
        st.plotly_chart(fig)

        # Create a collapsible section for the table display
        with st.expander("View Table"):
            # Show DataFrame as table
            st.write(df)

if __name__ == "__main__":
    main()
