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
# suppress Azure verbose logging
azure_logger = logging.getLogger('azure')
azure_logger.setLevel(logging.WARNING)
```

```python3
# paths for reading and writing files

MT_RAW_DIR = Path("raw/marinetraffic")
UNGP_RAW_DIR = Path("raw/ungp")

# basepath for reading from UNGP
UNGP_BASEPATH = "s3a://ungp-ais-data-historical-backup/exact-earth-data/transformed/prod/"
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
    text=True
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
MT_FILENAME = "greater-odesa-arrivals-departures-2024-03-01-2024-03-10-more-than-1-dwt.csv"
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
# iterate over dates to query UNGP AIS
# takes about 10s per date once started up

cols = ["mmsi", "imo", "message_type", "latitude", "longitude", "dt_pos_utc", "heading"]

N_EXTRA_DAYS = 30

query_dates = list(mt_df["simple_date"].unique())
query_dates.sort()
query_dates = (
    [
        min(query_dates) - pd.Timedelta(days=x)
        for x in range(N_EXTRA_DAYS, 0, -1)
    ] +
    query_dates +
    [
        max(query_dates) + pd.Timedelta(days=x)
        for x in range(1, N_EXTRA_DAYS + 1)
    ]
)

dfs = []
for query_date in query_dates:
    start_time = time.time()
    print(query_date)

    # Determine relevant departure MMSIs to query,
    # based on whether they departed within N_EXTRA_DAYS before the query date.
    departure_dates = [
        query_date - pd.Timedelta(days=x) for x in range(N_EXTRA_DAYS + 1)
    ]
    df_departures = mt_df[
        (mt_df["simple_date"].isin(departure_dates)) &
        (mt_df["Port Call Type"] == "DEPARTURE")
    ]
    # also, get the most recent departure
    df_last_departures = (
        df_departures
        .groupby("mmsi")["Ata/atd"]
        .max()
        .reset_index()
        .rename(columns={"Ata/atd": "departure_time"})
    )
    # display(df_last_departures)
    departure_mmsis = [int(x) for x in df_last_departures["mmsi"]]

    # determine relevant arrival MMSIs to query,
    # based on whether they arrived within N_EXTRA_DAYS after the query date
    arrival_dates = [
        query_date + pd.Timedelta(days=x) for x in range(N_EXTRA_DAYS + 1)
    ]
    df_arrivals = mt_df[
        (mt_df["simple_date"].isin(arrival_dates)) &
        (mt_df["Port Call Type"] == "ARRIVAL")
    ]
    # also, get the earliest arrival
    df_first_arrivals = (
        df_arrivals
        .groupby("mmsi")["Ata/atd"]
        .min()
        .reset_index()
        .rename(columns={"Ata/atd": "arrival_time"})
    )
    # display(df_first_arrivals)
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

    sp_in_f = sp_in.filter(sp_in.mmsi.isin(mmsi_list))[cols]
    df_in = sp_in_f.toPandas()

    # add arrival and departure dates
    df_in = df_in.merge(df_last_departures, on="mmsi", how="left")
    df_in = df_in.merge(df_first_arrivals, on="mmsi", how="left")

    dfs.append(df_in)
    print("duration:", f"{time.time() - start_time:.0f}s")
```

```python3
ais_df = pd.concat(dfs, ignore_index=True)
```

```python3
# check which ships both arrived and departed
# possibly mis-identified by MarineTraffic
ais_df[~ais_df["departure_time"].isnull() & ~ais_df["arrival_time"].isnull()].groupby("mmsi").first()
```

```python3
UNGP_FILENAME = f"{MT_RAW_PATH.stem}_ais.csv"
UNGP_RAW_PATH = UNGP_RAW_DIR / UNGP_FILENAME

data = ais_df.to_csv(index=False)
blob_client = container_client.get_blob_client(str(UNGP_RAW_PATH))
blob_client.upload_blob(data, overwrite=True)
```
