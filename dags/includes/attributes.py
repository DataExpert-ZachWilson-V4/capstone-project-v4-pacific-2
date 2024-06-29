import logging
from datetime import datetime
from io import BytesIO, StringIO
from os import getenv

import pandas as pd
import requests
from google.cloud import bigquery
from includes.settings import CF_ATTRIBUTES_T_ID, DATASET_ID

CF_TOKEN = getenv("CHATFUEL_TOKEN")
BOT_ID = getenv("CHATFUEL_BOT_ID")
BASE_URL = getenv("CHATFUEL_BASE_URL", "https://dashboard.chatfuel.com")

headers = {
    "authorization": f"Bearer {CF_TOKEN}",
    "content-type": "application/json",
    "cache-control": "no-cache",
}

GRAPHQL_URL = f"{BASE_URL}/graphql"
CHUNK_SIZE = 300

task_logger = logging.getLogger("airflow.task")


def split_list(data, chunk_size):
    for i in range(0, len(data), chunk_size):
        yield data[i : i + chunk_size]


def upload_attributes():
    # Send the request to CF db to receive the list of all existing attributes
    params_request_attributes = {
        "opName": "AttributesQuery",
    }

    json_request_attributes = {
        "operationName": "AttributesQuery",
        "variables": {
            "botId": BOT_ID,
        },
        "query": """ 
            query AttributesQuery(
                $botId: String!, 
                $platform: Platform = null, 
                $suggestType: VariableSuggestType = template, 
                $allPlatforms: Boolean = false
            ) {
                bot(id: $botId) { 
                    variableSuggest(
                        suggestType: $suggestType, 
                        platform: $platform, 
                        allPlatforms: $allPlatforms
                    ) {
                        name 
                        type
                    }
                }
            }
        """,
    }

    response_attr = requests.post(
        GRAPHQL_URL,
        params=params_request_attributes,
        headers=headers,
        json=json_request_attributes,
        timeout=60,
    )
    result_attr = response_attr.json()
    attr_list = result_attr["data"]["bot"]["variableSuggest"]

    fin_attr_list = [{"name": dct["name"], "type": dct["type"]} for dct in attr_list]

    last_updated_date = datetime.now()

    # Break the attribute list by chunks to avoid the errors from CF db
    chunk_size = CHUNK_SIZE
    chunks = list(split_list(fin_attr_list, chunk_size))

    # Send the request to CF db to collect the userbase by chunks
    params_request_db_link = {
        "opName": "GenerateUsersDocument",
    }

    for chunk_number, chunk in enumerate(chunks, start=1):

        print("chunk", chunk_number, datetime.now())

        chunk.extend([{"name": "chatfuel user id", "type": "system"}])

        json_request_db_link = {
            "operationName": "GenerateUsersDocument",
            "variables": {
                "botId": BOT_ID,
                "filter": '{"operation":"or","parameters":[]}',
                "params": {"fields": chunk},
            },
            "query": """
                mutation GenerateUsersDocument($botId: String!, $params: ExportParams, $filter: String) {
                    generateUsersExportDocument(botId: $botId, params: $params, filter: $filter) {
                        generatedId
                        downloadUrl
                    }
                }
            """,
        }
        response_db_link = requests.post(
            GRAPHQL_URL,
            params=params_request_db_link,
            headers=headers,
            json=json_request_db_link,
            timeout=300,
        )

        # Save the db, read it as pandas df, transform columns,transpose the df and upload it to BigQuery
        if response_db_link.status_code == 200:
            print("Download UsersDocument...", datetime.now())

            result_db_link = response_db_link.json()
            link_for_saving_db = (
                BASE_URL
                + result_db_link["data"]["generateUsersExportDocument"]["downloadUrl"]
            )
            save_db = requests.get(link_for_saving_db, headers=headers)
            res = save_db.text
            df = pd.read_csv(StringIO(res), low_memory=False)
            df.drop(["last seen", "page id"], axis=1, inplace=True)

            df.columns = [str(col).replace(" ", "_").lower() for col in df.columns]
            df = df.dropna(axis=1, how="all")

            duplicate_cols = df.columns[df.columns.duplicated()]
            df.drop(columns=duplicate_cols, inplace=True)

            df = df.melt(
                id_vars="chatfuel_user_id",
                value_vars=list(df.columns),
                var_name="attribute_name",
                value_name="attribute_value",
            ).dropna()
            
            df["last_updated_date"] = last_updated_date
            df["attribute_value"] = df["attribute_value"].astype(str)
            
            upload_to_bq(df)
        else:
            print(
                f"Failed to upload chunk {chunk_number}: {response_db_link.status_code}"
            )


def upload_to_bq(df):

    print("UPLOAD", datetime.now())

    task_logger.warning("TASK 1, before buffer")

    buffer = BytesIO()
    df.to_parquet(buffer, index=False, compression="snappy")
    buffer.seek(0)

    bigquery_client = bigquery.Client()
    table_ref = bigquery_client.dataset(DATASET_ID).table(CF_ATTRIBUTES_T_ID)

    task_logger.info(f"table_ref: {table_ref}")

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
    )

    load_job = bigquery_client.load_table_from_file(
        buffer, table_ref, job_config=job_config
    )
    print("Starting job {}".format(load_job.job_id))
    load_job.result()
    print(f"Loaded {load_job.output_rows} rows into {DATASET_ID}:{CF_ATTRIBUTES_T_ID}.")
