---
jupyter:
  jupytext:
    formats: ipynb,md
    text_representation:
      extension: .md
      format_name: markdown
      format_version: '1.3'
      jupytext_version: 1.16.1
  kernelspec:
    display_name: Config template pyspark3.5 ais2.9
    language: python3
    name: pyspark-ais-nosedona-202403
---

# Query AIS

```python3
import datetime
import logging
import os
import subprocess
import sys
import time
from io import StringIO
from pathlib import Path

import pandas as pd
from pyspark.sql.utils import AnalysisException

from getpass import getpass
```

```python3
# install jupyter-black for code formatting - optional
!pip install jupyter-black

import jupyter_black

jupyter_black.load()
```

```python3
# suppress Azure verbose logging
azure_logger = logging.getLogger("azure")
azure_logger.setLevel(logging.WARNING)
```

```python3
# paths for reading and writing files

MT_RAW_DIR = Path("raw/marinetraffic")
UNGP_RAW_DIR = Path("raw/ungp")

# basepath for reading from UNGP
UNGP_BASEPATH = (
    "s3a://ungp-ais-data-historical-backup/exact-earth-data/transformed/prod/"
)
```

```python3
# input BSGI_CONTAINER_SAS to access blob storage
BSGI_CONTAINER_SAS = getpass(prompt="BSGI_CONTAINER_SAS:")
```

```python3
CONTAINER_BASE_URL = "https://imb0chd0dev.blob.core.windows.net/bsgi?"
CONTAINER_URL = CONTAINER_BASE_URL + BSGI_CONTAINER_SAS
```

```python3
# Install Azure Storage Blob SDK

std_out = subprocess.run(
    [sys.executable, "-m", "pip", "install", "azure-storage-blob"],
    capture_output=True,
    text=True,
).stdout
print(std_out)
```

```python3
from azure.storage.blob import ContainerClient
```

```python3
container_client = ContainerClient.from_container_url(CONTAINER_URL)
```

```python3
blob_list = container_client.list_blobs(name_starts_with=f"{MT_RAW_DIR}/")
print("Available MarineTraffic export files in blob:")
for blob in blob_list:
    print(blob.name.removeprefix("raw/marinetraffic/"))
```

```python3
# change MT_FILENAME to determine which MarineTraffic export file to use
MT_FILENAME = (
    "greater-odesa-arrivals-departures-2024-03-01-2024-03-10-more-than-1-dwt.csv"
)
MT_RAW_PATH = MT_RAW_DIR / MT_FILENAME
```

```python3
# load MarineTraffic export CSV
blob_client = container_client.get_blob_client(str(MT_RAW_PATH))
data = blob_client.download_blob().readall()
mt_df = pd.read_csv(StringIO(data.decode("utf-8")), parse_dates=["Ata/atd"])
mt_df["simple_date"] = mt_df["Ata/atd"].dt.date
mt_df = mt_df.rename(columns={"Mmsi": "mmsi"})
mt_df
```

```python3
mt_df.columns
```

```python3
# iterate over dates to query UNGP AIS
# takes about 10s per date once started up

# set number of days before arrival and after departure to get AIS data
ARRIVAL_DAYS = 3
DEPARTURE_DAYS = 30

# set AIS columns to keep
AIS_COLS = [
    "mmsi",
    "imo",
    "message_type",
    "latitude",
    "longitude",
    "dt_pos_utc",
    "heading",
]

# set MarineTraffic columns to merge into AIS df
# this is in addition to the Ata/atd, which is always included
MT_COLS = ["Port At Call"]

mt_all_cols = MT_COLS + ["Ata/atd"]

query_dates = list(mt_df["simple_date"].unique())
query_dates.sort()
query_dates = (
    [min(query_dates) - pd.Timedelta(days=x) for x in range(ARRIVAL_DAYS, 0, -1)]
    + query_dates
    + [
        max(query_dates) + pd.Timedelta(days=x)
        for x in range(1, DEPARTURE_DAYS + 1)
    ]
)


dfs = []
for query_date in query_dates:
    start_time = time.time()
    print(query_date)

    # Determine relevant departure MMSIs to query,
    # based on whether they departed within DEPARTURE_DAYS before the query date.
    departure_dates = [
        query_date - pd.Timedelta(days=x) for x in range(DEPARTURE_DAYS + 1)
    ]
    df_departures = mt_df[
        (mt_df["simple_date"].isin(departure_dates))
        & (mt_df["Port Call Type"] == "DEPARTURE")
    ]
    # also, get the most recent departure
    df_last_departures = df_departures.loc[
        df_departures.groupby("mmsi")["Ata/atd"].idxmax()
    ].rename(columns={x: x + "_departure" for x in mt_all_cols})
    departure_mmsis = [int(x) for x in df_last_departures["mmsi"]]

    # determine relevant arrival MMSIs to query,
    # based on whether they arrived within ARRIVAL_DAYS after the query date
    arrival_dates = [
        query_date + pd.Timedelta(days=x) for x in range(ARRIVAL_DAYS + 1)
    ]
    df_arrivals = mt_df[
        (mt_df["simple_date"].isin(arrival_dates))
        & (mt_df["Port Call Type"] == "ARRIVAL")
    ]
    # also, get the earliest arrival
    df_first_arrivals = df_arrivals.loc[
        df_arrivals.groupby("mmsi")["Ata/atd"].idxmin()
    ].rename(columns={x: x + "_arrival" for x in mt_all_cols})
    arrival_mmsis = [int(x) for x in df_first_arrivals["mmsi"]]

    mmsi_list = arrival_mmsis + departure_mmsis

    date_path = (
        f"{UNGP_BASEPATH}year={query_date.year}/month={query_date.month:02d}/"
        f"day={query_date.day:02d}"
    )
    try:
        sp_in = spark.read.parquet(date_path)
    except AnalysisException as e:
        print(f"Data does not exist for {query_date}")
        continue

    sp_in_f = sp_in.filter(sp_in.mmsi.isin(mmsi_list))[AIS_COLS]
    df_in = sp_in_f.toPandas()

    # add arrival and departure times
    df_in = df_in.merge(
        df_last_departures[[x + "_departure" for x in mt_all_cols] + ["mmsi"]],
        on="mmsi",
        how="left",
    )
    df_in = df_in.merge(
        df_first_arrivals[[x + "_arrival" for x in mt_all_cols] + ["mmsi"]],
        on="mmsi",
        how="left",
    )

    dfs.append(df_in)
    print("duration:", f"{time.time() - start_time:.0f}s")
```

```python3
ais_df = pd.concat(dfs, ignore_index=True)
# merge in Vessel Name
ais_df = ais_df.merge(
    mt_df.groupby("mmsi")["Vessel Name"].first().reset_index()[["mmsi", "Vessel Name"]],
    on="mmsi",
    how="left",
)


# to correctly categorize internal voyages,
# remove departure time for rows where AIS time is before departure time
# and vice-versa for arrival times
def remove_incorrect_arrivals_departures(row):
    if row["dt_pos_utc"] > row["Ata/atd_arrival"]:
        row[[x + "_arrival" for x in mt_all_cols]] = None
    if row["dt_pos_utc"] < row["Ata/atd_departure"]:
        row[[x + "_departure" for x in mt_all_cols]] = None
    return row


ais_df = ais_df.apply(remove_incorrect_arrivals_departures, axis=1)


def determine_voyage_type(row):
    voyage_type = None
    if not pd.isna(row["Ata/atd_departure"]) and not pd.isna(row["Ata/atd_arrival"]):
        voyage_type = "INTERNAL"
    elif not pd.isna(row["Ata/atd_departure"]):
        voyage_type = "DEPARTURE"
    elif not pd.isna(row["Ata/atd_arrival"]):
        voyage_type = "ARRIVAL"
    return voyage_type


ais_df["voyage_type"] = ais_df.apply(determine_voyage_type, axis=1)
```

```python3
ais_df[ais_df["voyage_type"] == "INTERNAL"]
```

```python3
UNGP_FILENAME = f"{MT_RAW_PATH.stem}_ais_a{ARRIVAL_DAYS}_d{DEPARTURE_DAYS}.csv"
UNGP_RAW_PATH = UNGP_RAW_DIR / UNGP_FILENAME

data = ais_df.to_csv(index=False)
blob_client = container_client.get_blob_client(str(UNGP_RAW_PATH))
blob_client.upload_blob(data, overwrite=True)
```
