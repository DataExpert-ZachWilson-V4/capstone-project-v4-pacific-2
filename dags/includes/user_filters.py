import json
from datetime import datetime, timedelta, timezone
from io import BytesIO, StringIO
from os import getenv

import pandas as pd
import pyarrow as pa
import pyarrow.csv as pv
import pyarrow.parquet as pq
import requests
from google.cloud import bigquery, storage
from includes.settings import DATASET_ID, RECIPIENTS_T_ID, USER_FILTERS_T_ID


CF_TOKEN = getenv("CHATFUEL_TOKEN")
BOT_ID = getenv("CHATFUEL_BOT_ID")
BASE_URL = getenv("CHATFUEL_BASE_URL", "https://dashboard.chatfuel.com")

headers = {
    "authorization": f"Bearer {CF_TOKEN}",
    "content-type": "application/json",
    "cache-control": "no-cache",
}
params = {
    "opName": "GenerateUsersDocument",
}

GRAPHQL_URL = f"{BASE_URL}/graphql"
BOTS_API = f"{BASE_URL}/api/bots/"

BIGQUERY_CLIENT = bigquery.Client()


# Retrieve the latest broadcast entries from GCS
def get_broadcast_entries():
    client = storage.Client()
    blobs = client.list_blobs("cf_broadcasts_entries_pipeline1")
    broadcast_entry_file = sorted(blobs, key=lambda d: d.updated)[-1]
    return json.loads(broadcast_entry_file.download_as_text())["result"]


# Fetch results of the last 10 sent broadcasts
def get_broadcasts_results():
    r = requests.get(
        f"{BOTS_API}/{BOT_ID}/broadcast_stats?limit=10&from=", headers=headers
    )
    broadcasts_results = r.json()["result"]
    filtered_broadcasts_res = [
        bc
        for bc in broadcasts_results
        if bc["in_progress"] == False and bc["total_users"] > 0
    ]
    return filtered_broadcasts_res


# Fetch audience filter used in a specific broadcast
def get_audience_filter(block_id):
    query = """query BlockQuery($botId: String!, $blockId: ID!) {
      bot(id: $botId) {
        id
        block(id: $blockId) {
          id
          title
          removed
          user_filter {
            operation
            valid
            parameters {
              type
              name
              operation
              values
            }
          }
        }
      }
    }"""

    payload = {
        "operationName": "BlockQuery",
        "variables": {
            "botId": BOT_ID,
            "blockId": block_id,
        },
        "query": query,
    }

    r = requests.post(GRAPHQL_URL, json=payload, headers=headers, timeout=60)
    response = r.json()
    audience_filter_res = response["data"]["bot"]["block"]["user_filter"]
    audience_filter = json.dumps(audience_filter_res)
    return audience_filter


# Check if users have already received the broadcast
def check_user_reception(broadcast_id):
    query = f"""
    SELECT count(*) as count FROM `blended-setup.userbase_staging.int_cf_attributes` 
    WHERE attribute_name='received_broadcast' and attribute_value='{broadcast_id}'"""
    result = BIGQUERY_CLIENT.query(query).to_dataframe().iloc[0]["count"]
    return result > 0


# Send the request to CF database to create a link to save user ids filtered by audience filter
def fetch_user_ids(audience_filter):
    query = """mutation GenerateUsersDocument($botId: String!, $params: ExportParams, $filter: String) {
        generateUsersExportDocument(botId: $botId, params: $params, filter: $filter) {
            generatedId
            downloadUrl
            __typename
        }
    }"""

    json_data = {
        "operationName": "GenerateUsersDocument",
        "variables": {
            "botId": BOT_ID,
            "filter": str(audience_filter),
            "params": {
                "desc": True,
                "sortBy": "updated_date",
                "fields": [
                    {
                        "name": "chatfuel user id",
                        "type": "system",
                    },
                ],
            },
        },
        "query": query,
    }
    response_db_link = requests.post(
        GRAPHQL_URL, params=params, headers=headers, json=json_data, timeout=60
    )
    result_db_link = response_db_link.json()
    link_for_saving_db = (
        BASE_URL + result_db_link["data"]["generateUsersExportDocument"]["downloadUrl"]
    )
    # Download db with user ids
    save_db = requests.get(link_for_saving_db, headers=headers)
    result = save_db.content
    res = BytesIO(result)
    df = pv.read_csv(res).to_pandas()
    df = df.drop(columns="page id")
    return df


# With broadcast entries and broadcasts results find the audience filter per broadcast
# and request the list of user id's that appear in this audience filter
# and return the list of audience filters (will use them later down the pipeline)
# and return the list of user id's, so we can add an event of receiving the broadcast to these users
def user_filters():
    # Call the functions to retrieve broadcasts entries and results
    broadcast_entries = get_broadcast_entries()
    filtered_broadcasts_res = get_broadcasts_results()
    current_time = datetime.now(timezone.utc)

    # Define the empty lists to put the data into
    broadcast_recipients = []
    audience_filter_list = []

    for entry in broadcast_entries:
        broadcast_time = datetime.fromtimestamp(
            entry["broadcast_start_timestamp"], timezone.utc
        )
        time_difference = current_time - broadcast_time

        if entry["enabled"] and timedelta(hours=1) < time_difference < timedelta(
            days=7
        ):
            for result in filtered_broadcasts_res:
                if entry["block_title"] == result[
                    "block_title"
                ] and not check_user_reception(result["broadcast_id"]):
                    audience_filter = get_audience_filter(entry["block_id"])
                    df_audience_filter = pd.read_json(StringIO(audience_filter))
                    df_audience_filter["broadcast_id"] = result["broadcast_id"]
                    df_audience_filter["broadcast_timestamp"] = broadcast_time.strftime(
                        "%Y-%m-%d %H:%M:%S"
                    )
                    audience_filter_list.append(df_audience_filter)

                    # Retrieve and process user ids
                    user_ids = fetch_user_ids(audience_filter)
                    user_ids["broadcast_id"] = result["broadcast_id"]
                    user_ids["broadcast_timestamp"] = broadcast_time.strftime(
                        "%Y-%m-%d %H:%M:%S"
                    )
                    broadcast_recipients.append(user_ids)

    return broadcast_recipients, audience_filter_list


# Formats data and loads it into BigQuery
def format_and_load_data(df_list, dataset_id, bq_table_id):
    if df_list:
        # Concatenate DataFrames from list
        df = pd.concat(df_list, ignore_index=True)
        df.columns = [col.replace(" ", "_").lower() for col in df.columns]
        pq_table = pa.Table.from_pandas(df)
        buffer = BytesIO()
        pq.write_table(pq_table, buffer)

        # Configure the load job
        table_ref = BIGQUERY_CLIENT.dataset(dataset_id).table(bq_table_id)

        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            source_format=bigquery.SourceFormat.PARQUET,
        )

        buffer.seek(0)

        load_job = BIGQUERY_CLIENT.load_table_from_file(
            buffer, table_ref, job_config=job_config
        )
        print("Starting job {}".format(load_job.job_id))

        load_job.result()
        print("Job finished.")
    else:
        print("No data to process.")


def upload_broadcast_recipients_and_audience_filter():
    broadcast_recipients, audience_filter_list = user_filters()
    # Process and upload broadcast recipients and audience filters
    format_and_load_data(broadcast_recipients, DATASET_ID, RECIPIENTS_T_ID)
    format_and_load_data(audience_filter_list, DATASET_ID, USER_FILTERS_T_ID)
