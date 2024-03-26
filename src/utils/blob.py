import os
from io import StringIO

import pandas as pd
from azure.storage.blob import ContainerClient
from dotenv import load_dotenv

load_dotenv()

BSGI_CONTAINER_SAS = os.getenv("BSGI_CONTAINER_SAS")
CONTAINER_BASE_URL = "https://imb0chd0dev.blob.core.windows.net/bsgi?"
CONTAINER_URL = CONTAINER_BASE_URL + BSGI_CONTAINER_SAS

container_client = ContainerClient.from_container_url(CONTAINER_URL)


def load_blob_data(blob_name):
    blob_client = container_client.get_blob_client(blob_name)
    data = blob_client.download_blob().readall()
    return data


def load_blob_csv(blob_name) -> pd.DataFrame:
    data = load_blob_data(blob_name)
    return pd.read_csv(StringIO(data.decode("utf-8")))


def upload_blob_data(blob_name, data):
    blob_client = container_client.get_blob_client(blob_name)
    blob_client.upload_blob(data, overwrite=True)


def upload_blob_csv(blob_name, df: pd.DataFrame):
    data = df.to_csv(index=False)
    upload_blob_data(blob_name, data)


def list_blobs(name_starts_with=None):
    return [
        blob.name
        for blob in container_client.list_blobs(
            name_starts_with=name_starts_with
        )
    ]
