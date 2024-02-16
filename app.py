import time  # to simulate a real time data, time loop
import json
import sys
from datetime import datetime
import numpy as np  # np mean, np random
import pandas as pd  # read csv, df manipulation
import plotly.express as px  # interactive charts
import streamlit as st  # 🎈 data web app development

startpath = "data/in/files/"
endpath = "data/out/files/"
filename_json = "data/in/files/run_results.json"


def load_json(filename_json):
    """
    load json file and return dict
    """
    print("Loading: {}".format(filename_json))

    with open(filename_json,"r") as file:
        data = json.load(file)
        file.close()
    
    return data


def grab_time_metrics(df):
    """
    Calculate time metrics, min, max, duration
    """
    # calculate
    start_time = df["compile_started_at"].min()
    end_time = df["execute_completed_at"].max()
    duration = (end_time-start_time).total_seconds()
    # format
    start = start_time.strftime("%H:%M:%S")
    end = end_time.strftime("%H:%M:%S")

    return start, end, duration


def extract_timing(row):
    """
    Function to extract values from JSON objects in the "timing" column
    """
    
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



## ---------------


# load data
try:
    data = load_json(filename_json)
    st.markdown("Artifact Loaded")
except:
    print("could not load artifact file: run_results.json")
    sys.exit(1)

# define data frame
df = pd.DataFrame(data["results"])
st.dataframe(df)

# dashboard title
st.title("dbt evaluation")

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
df["compile_duration"] = (df["compile_completed_at"]-df["compile_started_at"]).dt.total_seconds()
df["execute_duration"] = (df["execute_completed_at"]-df["execute_started_at"]).dt.total_seconds()

# Execution metrics
start, end, raw_duration = grab_time_metrics(df)
raw_duration = round(raw_duration, 2)
duration = round(data["elapsed_time"], 2)

print("Start: {} \nEnd: {} \nRaw duration(s): {} \nDuration(s): {}".format(start, end, raw_duration, duration))



# create three columns
kpi1, kpi2, kpi3, kpi4 = st.columns(4)

# fill in those three columns with respective metrics or KPIs
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

kpi3.metric(
    label="Duration",
    value=duration
)


