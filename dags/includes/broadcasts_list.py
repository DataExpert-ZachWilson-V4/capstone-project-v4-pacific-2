import logging
from io import BytesIO
from os import getenv

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from google.cloud import bigquery
from includes.settings import BROADCASTS_LIST_T_ID, DATASET_ID

CF_TOKEN = getenv("CHATFUEL_TOKEN")
BOT_ID = getenv("CHATFUEL_BOT_ID")
BASE_URL = getenv("CHATFUEL_BASE_URL", "https://dashboard.chatfuel.com")

headers = {
    "authorization": f"Bearer {CF_TOKEN}",
    "content-type": "application/json",
    "cache-control": "no-cache",
}

params = {
    "opName": "GenerateBroadcastDocument",
}

GRAPHQL_URL = f"{BASE_URL}/graphql"


def upload_broadcasts_list():
    query = """mutation GenerateBroadcastDocument($botId: String!) {
        generateBroadcastExportDocument(botId: $botId) {
            generatedId
            downloadUrl
            __typename
            }
        }"""

    json_data = {
        "operationName": "GenerateBroadcastDocument",
        "variables": {
            "botId": BOT_ID,
        },
        "query": query,
    }

    # Send the request to the CF database to receive the url for downloading the broadcasts list
    response = requests.post(
        GRAPHQL_URL, params=params, headers=headers, json=json_data, timeout=60
    )
    result_link = response.json()
    download_url = (
        BASE_URL + result_link["data"]["generateBroadcastExportDocument"]["downloadUrl"]
    )

    # Download the broadcast list, write it into pyarrow csv file
    resp_broadcasts_list = requests.get(download_url, headers=headers)
    bcast_list = resp_broadcasts_list.content
    df = pd.read_csv(BytesIO(bcast_list))

    # Transform column timestamp to datetime and create a new column from it,
    # to ensure the consistency of the timestamps through the project
    df["Timestamp"] = pd.to_datetime(df["Timestamp"])
    df["broadcast_timestamp"] = (
        df["Timestamp"].dt.strftime("%H:%M:%S %m-%d-%Y").to_list()
    )
    df.columns = [str(col).replace(" ", "_").lower() for col in df.columns]

    # Transform result table to pyarrow format
    broadcast_list_table = pa.Table.from_pandas(df, preserve_index=False)

    buffer = BytesIO()
    pq.write_table(broadcast_list_table, buffer)

    # Configure the load job
    bigquery_client = bigquery.Client()
    table_ref = bigquery_client.dataset(DATASET_ID).table(BROADCASTS_LIST_T_ID)

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.PARQUET,
    )

    buffer.seek(0)

    load_job = bigquery_client.load_table_from_file(
        buffer, table_ref, job_config=job_config
    )
    print("Starting job {}".format(load_job.job_id))

    load_job.result()
    print("Job finished.")
