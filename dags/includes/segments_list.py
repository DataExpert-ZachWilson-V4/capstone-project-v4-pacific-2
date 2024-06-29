from datetime import date
from io import BytesIO
from os import getenv

import pyarrow as pa
import pyarrow.parquet as pq
import requests
from google.cloud import bigquery
from includes.settings import DATASET_ID, SEGMENT_TABLE_ID

CF_TOKEN = getenv("CHATFUEL_TOKEN")
BOT_ID = getenv("CHATFUEL_BOT_ID")
BASE_URL = getenv("CHATFUEL_BASE_URL", "https://dashboard.chatfuel.com")

headers = {
    "authorization": f"Bearer {CF_TOKEN}",
    "content-type": "application/json",
    "cache-control": "no-cache",
}
params = {
    "opName": "segments",
}

GRAPHQL_URL = f"{BASE_URL}/graphql"


def upload_segments_list():
    query = """query segments($botId: String!) {
        bot(id: $botId) {
            segments {
                id
                name
            }
        }
    }"""
    # Send the request to CF database to collect the list of all custom user's segments
    json_data = {
        "operationName": "segments",
        "variables": {
            "botId": BOT_ID,
        },
        "query": query,
    }

    response = requests.post(
        GRAPHQL_URL, params=params, headers=headers, json=json_data, timeout=60
    )
    res = response.json()
    segments_list = res["data"]["bot"]["segments"]

    # Write the result of the request to pyarrow table
    df_from_list = pa.Table.from_pylist(segments_list)

    # Rename the columns and add the last_updated_date column
    df = df_from_list.rename_columns(["segment_id", "segment_name"])
    last_updated_date = pa.array([date.today()] * df.num_rows, type=pa.date32())
    df = df.append_column("last_updated_date", last_updated_date)

    buffer = BytesIO()
    pq.write_table(df, buffer)

    # Configure the load job
    bigquery_client = bigquery.Client()
    table_ref = bigquery_client.dataset(DATASET_ID).table(SEGMENT_TABLE_ID)

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
