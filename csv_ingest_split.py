import streamlit as st
import pandas as pd
import pandas_gbq
import json
from google.oauth2 import service_account
import time
from datetime import date
import numpy as np
import os

# Initialize session state for button clicks if not already done
if "ingest_pressed" not in st.session_state:
    st.session_state["ingest_pressed"] = False

# Set up a sidebar for page navigation
st.sidebar.title("Navigation")
page = st.sidebar.radio("Go to", ["Upload CSV to BigQuery", "CSV Splitter"])

# Function to handle BigQuery upload
def upload_to_bigquery(data, table_id, project_id, credentials, if_exists):
    try:
        pandas_gbq.to_gbq(data, table_id, project_id=project_id, if_exists=if_exists, credentials=credentials)
        st.success("Data uploaded successfully to BigQuery")
    except Exception as e:
        st.error(f"An error occurred: {e}")

# Page 1: Upload CSV to BigQuery
if page == "Upload CSV to BigQuery":
    st.title("Upload CSV into GBQ WebApp")

    # Caution and instruction text
    st.markdown(
        """<h1>#Caution!!</h1>
        <p>Number of columns and sequences in CSV file need to match with table_id in GBQ.</p>
        <p>#Instruction</p>
        <ul>
            <li>Upload JSON credential file.</li>
            <li>Upload CSV file you want to ingest.</li>
            <li>Provide the BigQuery table ID.</li>
        </ul>""",
        unsafe_allow_html=True,
    )

    # Upload JSON credential file
    uploaded_file_json = st.sidebar.file_uploader("Upload JSON Credential", type=["json"], key="json_upload")

    # Upload CSV file
    uploaded_file_csv = st.sidebar.file_uploader("Upload CSV File", type=["csv"], key="csv_upload")

    # Input for BigQuery table ID
    table_id = st.sidebar.text_input("Enter BigQuery Table ID", key="table_id")

    # Select function
    if_exists_option = st.sidebar.selectbox("Select Function", ["append"], key="if_exists")

    # Button to trigger ingestion
    if st.sidebar.button("Let's Ingest"):
        st.session_state["ingest_pressed"] = True

    # Process uploaded files and ingest
    if st.session_state["ingest_pressed"]:
        if uploaded_file_json and uploaded_file_csv and table_id:
            try:
                # Load JSON credentials
                credentials = service_account.Credentials.from_service_account_info(
                    json.load(uploaded_file_json)
                )

                # Load CSV data
                data = pd.read_csv(uploaded_file_csv)

                # Display data sample
                st.write("### Data Sample")
                st.write(data.head())

                # Validate and upload data
                st.write("Uploading to BigQuery...")
                upload_to_bigquery(data, table_id, "cdg-mark-cust-prd", credentials, if_exists_option)

            except Exception as e:
                st.error(f"An error occurred during processing: {e}")
        else:
            st.warning("Please upload both JSON and CSV files and provide a valid table ID.")

# Page 2: CSV Splitter
elif page == "CSV Splitter":
    st.title("CSV Splitter")

    uploaded_file = st.file_uploader("Upload your CSV file", type="csv")

    if uploaded_file:
        # Read the uploaded file
        df = pd.read_csv(uploaded_file)
        st.write("Preview of the uploaded file:")
        st.write(df.head())

        # Split by number of rows
        st.subheader("1. Split by Number of Rows")
        rows_per_file = st.number_input("Enter number of rows per file:", min_value=1, value=1000, step=100)
        prefix_rows = st.text_input("Enter file prefix for split by rows:", value="output")

        if st.button("Split by Rows"):
            num_chunks = (len(df) + rows_per_file - 1) // rows_per_file  # Ceiling division
            for i in range(num_chunks):
                start_row = i * rows_per_file
                end_row = start_row + rows_per_file
                chunk = df[start_row:end_row]
                file_name = f"{prefix_rows}_rows_{i}.csv"
                st.download_button(
                    label=f"Download {file_name}",
                    data=chunk.to_csv(index=False),
                    file_name=file_name,
                    mime="text/csv"
                )

        # Split by group name
        st.subheader("2. Split by Group Name")
        group_column = st.selectbox("Select column to split by group name:", df.columns)
        prefix_group = st.text_input("Enter file prefix for split by group name:", value="output")

        if st.button("Split by Group Name"):
            for group_name, group_data in df.groupby(group_column):
                file_name = f"{prefix_group}_{group_name}.csv"
                st.download_button(
                    label=f"Download {file_name}",
                    data=group_data.to_csv(index=False),
                    file_name=file_name,
                    mime="text/csv"
                )
