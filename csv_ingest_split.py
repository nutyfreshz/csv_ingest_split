import subprocess
import streamlit as st
import pandas as pd
import pandas_gbq
import json
from google.oauth2 import service_account
import time
from datetime import date
import numpy as np
import os

# Set up a sidebar for page navigation
st.sidebar.title("Navigation")
page = st.sidebar.radio("Go to", ["Upload CSV to BigQuery", "CSV Splitter"])

# Page 1: Upload CSV to BigQuery
if page == "Upload CSV to BigQuery":
    # Title
    st.title("Upload CSV into GBQ WebApp")

    # Caution
    st.markdown(
        """
        <h1>#Caution!!</h1>
        <p>Number of columns and sequences in CSV file need to matched with table_id in GBQ.</p>
        <p>PS. group_name & commu_type & target columns should be exists in CSV file.</p>
        <p>PSS. commu_type = [SMS,EDM,LINE,T1APP,COL,MART,FB,CALL]</p>
        <p>For input send_date, commu_type = SMS,EDM then send_date_sms & send_date_edm must be filled!! </p>
        """,
        unsafe_allow_html=True
    )

    # Display the image
    st.markdown(
        """
        <h1>#Example of CSV data ingest into BigQuery</h1>
        """,
        unsafe_allow_html=True
    )
    url_images = 'https://i.ibb.co/nCvgDNy/example-table-ingest.png'
    st.image(url_images)

    # Instruction
    st.markdown(
        """
        <h1>#Instruction</h1>
        <p>1. Browse JSON Credential file from moderator in Part 1) section.</p>
        <p>2. Browse CSV file which you want to ingest in Part 2) section.</p>
        <p>3. Type table_id which came from Moderator in Part 3) section.</p>
        """,
        unsafe_allow_html=True
    )

    # Sidebar for Upload JSON Credential
    st.sidebar.header("Part 1) Upload JSON Credential")
    uploaded_file_json = st.sidebar.file_uploader("Upload a JSON file", type=["json"])

    # Sidebar for Upload CSV File
    st.sidebar.header("Part 2) Write data & Upload CSV Data")
    banner_option = st.sidebar.selectbox("Select Banner", ["CDS", "RBS"])
    campaign_name_input = st.sidebar.text_input("Enter Campaign name (e.g., 2024-04_RBS_CRM_SUMMER)")
    subgroup_name_input = st.sidebar.text_input("Enter subgroup name (e.g., offer, commu)")
    start_camp_input = st.sidebar.text_input("Enter start_campaign period (e.g., 2024-04-16)")
    end_camp_input = st.sidebar.text_input("Enter end_campaign period (e.g., 2024-04-26)")
    send_date_sms_input = st.sidebar.text_input("Enter send_date_sms period (e.g., 2024-04-26)")
    send_date_edm_input = st.sidebar.text_input("Enter send_date_edm period (e.g., 2024-04-26)")
    req_option = st.sidebar.selectbox("Select requester", ["Itthikan C.", "Pichaporn K.", "Dudsadee W."])
    owner_option = st.sidebar.selectbox("Select data_owner", ["BI Dashboard", "Kamontip A."])
    uploaded_file = st.sidebar.file_uploader("Upload a CSV file", type=["csv"])

    st.sidebar.header("Part 3) BigQuery Table ID")
    table_id_input = 'CAS_DS_DATABASE.ca_ds_crm_campaign_target_control'
    ingest_button = st.sidebar.button("Let's ingest into GBQ")

    # Rest of the CSV to BigQuery code...
    # (Use the original code you provided in APP_1)

# Page 2: CSV Splitter
elif page == "CSV Splitter":
    # Title
    st.title("CSV Splitter")

    uploaded_file = st.file_uploader("Upload your CSV file", type="csv")

    if uploaded_file:
        # Read the uploaded file
        df = pd.read_csv(uploaded_file)
        st.write("Preview of the uploaded file:")
        st.write(df.head())

        # Split by Number of Rows
        st.subheader("1. Split by Number of Rows")
        rows_per_file = st.number_input("Enter number of rows per file:", min_value=1, value=1000, step=100)
        prefix_rows = st.text_input("Enter file prefix for split by rows:", value="output")

        if st.button("Split by Rows"):
            num_chunks = (len(df) + rows_per_file - 1) // rows_per_file  # Ceiling division
            output_files = []
            for i in range(num_chunks):
                start_row = i * rows_per_file
                end_row = start_row + rows_per_file
                chunk = df[start_row:end_row]
                output_file = f"{prefix_rows}_rows_{i}.csv"
                chunk.to_csv(output_file, index=False)
                output_files.append(output_file)
            st.success("Files created:")
            for file in output_files:
                st.write(file)
                st.download_button(
                    label=f"Download {file}",
                    data=open(file, "rb").read(),
                    file_name=file,
                    mime="text/csv"
                )
                os.remove(file)

        # Split by Group Name
        st.subheader("2. Split by Group Name")
        group_column = st.selectbox("Select column to split by group name:", df.columns)
        prefix_group = st.text_input("Enter file prefix for split by group name:", value="output")

        if st.button("Split by Group Name"):
            output_files = []
            for group_name in df[group_column].unique():
                group_data = df[df[group_column] == group_name]
                output_file = f"{prefix_group}_{group_name}.csv"
                group_data.to_csv(output_file, index=False)
                output_files.append(output_file)
            st.success("Files created:")
            for file in output_files:
                st.write(file)
                st.download_button(
                    label=f"Download {file}",
                    data=open(file, "rb").read(),
                    file_name=file,
                    mime="text/csv"
                )
                os.remove(file)
