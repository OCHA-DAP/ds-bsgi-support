import os
from pathlib import Path

import pandas as pd

from src.utils import blob

BSGI_DATA_DIR = Path(os.getenv("BSGI_DATA_DIR"))
UNGP_RAW_DIR = BSGI_DATA_DIR / "data" / "raw" / "ungp"
UNGP_RAW_BLOB = Path("raw/ungp")


def load_ungp(filename: str):
    filepath = UNGP_RAW_BLOB / filename
    df = blob.load_blob_csv(str(filepath))
    date_cols = ["dt_pos_utc"]
    for col in date_cols:
        df[col] = pd.to_datetime(df[col])
    return df
