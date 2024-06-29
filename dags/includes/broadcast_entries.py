from datetime import datetime
from os import getenv

import requests
from google.cloud import storage


CF_TOKEN = getenv("CHATFUEL_TOKEN")
BOT_ID = getenv("CHATFUEL_BOT_ID")
BASE_URL = getenv("CHATFUEL_BASE_URL", "https://dashboard.chatfuel.com")

headers = {
    "authorization": f"Bearer {CF_TOKEN}",
    "content-type": "application/json",
    "cache-control": "no-cache",
}

BOTS_API = f"{BASE_URL}/api/bots/"


def upload_broadcast_entries():

    ### Send the request to CF to receive broadcast scheduled entries
    url = f"{BOTS_API}/{BOT_ID}/scheduled_entries"
    r = requests.get(url, headers=headers)
    result = r.text

    ### Uploading the broadcasts entries to the GCS
    storage_client = storage.Client()
    bucket = storage_client.bucket("cf_broadcasts_entries_pipeline1")
    date_today = datetime.now().strftime("%Y-%m-%d")
    destination_blob_name = f"broadcast_entries_{date_today}.csv"
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_string(result)
