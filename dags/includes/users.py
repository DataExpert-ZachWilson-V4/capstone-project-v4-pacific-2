import logging
from datetime import datetime
from io import BytesIO, StringIO
from os import getenv

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from google.cloud import bigquery
from includes.settings import CF_USERS_T_ID, DATASET_ID

task_logger = logging.getLogger("airflow.task")
CF_TOKEN = getenv("CHATFUEL_TOKEN")
BOT_ID = getenv("CHATFUEL_BOT_ID")
BASE_URL = getenv("CHATFUEL_BASE_URL", "https://dashboard.chatfuel.com")

headers = {
    "authorization": f"Bearer {CF_TOKEN}",
    "content-type": "application/json",
    "cache-control": "no-cache",
}

GRAPHQL_URL = f"{BASE_URL}/graphql"
BOTS_API = f"{BASE_URL}/api/bots/"


# Send the request to CF db to save the userbase
def upload_users():
    task_logger.warning("TASK 1, start")
    # Define needed attributes to save
    attributes_query_list = [
        {"name": "chatfuel user id", "type": "system"},
        {"name": "first name", "type": "system"},
        {"name": "last name", "type": "system"},
        {"name": "signed up", "type": "system"},
        {"name": "last seen", "type": "system"},
        {"name": "status", "type": "system"},
        {"name": "timezone", "type": "system"},
        {"name": "gender", "type": "system"},
        {"name": "instagram name", "type": "system"},
        {"name": "instagram handle", "type": "system"},
    ]

    params_request_db_link = {
        "opName": "GenerateUsersDocument",
    }

    query = """
        mutation GenerateUsersDocument($botId: String!, $params: ExportParams, $filter: String) {
            generateUsersExportDocument(botId: $botId, params: $params, filter: $filter) {
                generatedId 
                downloadUrl
            }
        }
    """

    json_data_request_database_link = {
        "operationName": "GenerateUsersDocument",
        "variables": {
            "botId": BOT_ID,
            "filter": '{"operation":"or","parameters":[]}',
            "params": {"fields": attributes_query_list},
        },
        "query": query,
    }

    response_db_link = requests.post(
        GRAPHQL_URL,
        params=params_request_db_link,
        headers=headers,
        json=json_data_request_database_link,
        timeout=300,
    )

    # Save db to pandas df, transform columns as needed
    if response_db_link.status_code == 200:
        task_logger.warning("TASK 2, saving the db")
        result_db_link = response_db_link.json()
        link_for_saving_db = (
            BASE_URL
            + result_db_link["data"]["generateUsersExportDocument"]["downloadUrl"]
        )
        save_db = requests.get(link_for_saving_db, headers=headers)
        res = save_db.text

        df = pd.read_csv(StringIO(res))
        df.columns = [str(col).replace(" ", "_").lower() for col in df.columns]
        df["last_updated_date"] = datetime.now()
        df = df.drop(columns=["page_id"])
    else:
        task_logger.warning("TASK 2, failed to save the db")
        print(f"Failed to upload: {response_db_link.status_code}")

    # Transform pandas df to pyarrow df
    df_pq = pa.Table.from_pandas(df)

    task_logger.warning("TASK 3, before GCS")

    buffer = BytesIO()
    pq.write_table(df_pq, buffer)

    # Upload the table to BigQuery
    bigquery_client = bigquery.Client()
    table_ref = bigquery_client.dataset(DATASET_ID).table(CF_USERS_T_ID)

    task_logger.info(f"table_ref: {table_ref}")

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
