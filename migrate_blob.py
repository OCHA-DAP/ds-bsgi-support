import os
from pathlib import Path

from tqdm.auto import tqdm

from src.utils.blob import container_client

BSGI_DATA_DIR = Path(os.getenv("BSGI_DATA_DIR"))
BSGI_BLOB_SYNC_DIR = BSGI_DATA_DIR / "data"


def migrate_blob():
    blob_list = list(container_client.list_blobs())
    print("Migrating blobs:")
    print([blob.name for blob in blob_list])
    for blob in tqdm(blob_list):
        blob_client = container_client.get_blob_client(blob.name)
        download_file_path = BSGI_BLOB_SYNC_DIR / blob.name
        if "." in blob.name:
            os.makedirs(os.path.dirname(download_file_path), exist_ok=True)
            with open(download_file_path, "wb") as download_file:
                download_file.write(blob_client.download_blob().readall())


if __name__ == "__main__":
    migrate_blob()
