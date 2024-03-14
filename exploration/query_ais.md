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
    display_name: Config template ais-tt (to be removed 2023-03-15)
    language: python3
    name: ais-tt
---

# Query AIS

```python3
import datetime
import os
import subprocess
import sys
import time
from io import StringIO
from pathlib import Path

import pandas as pd

from getpass import getpass
```

```python3
# paths for reading and writing files
# change MT_FILENAME to determine which MarineTraffic export file to use
MT_FILENAME = "MarineTraffic_Arrivals_departures_Export_2024-03-06.csv"

MT_RAW_DIR = Path("raw/marinetraffic")
UNGP_RAW_DIR = Path("raw/ungp")
MT_RAW_PATH = MT_RAW_DIR / MT_FILENAME
UNGP_FILENAME = f"{MT_RAW_PATH.stem}_ais.csv"
UNGP_RAW_PATH = UNGP_RAW_DIR / UNGP_FILENAME

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
# load MarineTraffic export CSV
blob_client = container_client.get_blob_client(str(MT_RAW_PATH))
data = blob_client.download_blob().readall()
mt_df = pd.read_csv(StringIO(data.decode("utf-8")), parse_dates=["Ata/atd"])
mt_df["simple_date"] = mt_df["Ata/atd"].dt.date
mt_df
```

```python3
# iterate over dates to query UNGP AIS
# takes about 10s per date once started up

cols = ["mmsi", "imo", "message_type", "latitude", "longitude", "dt_pos_utc", "heading"]

query_dates = list(mt_df["simple_date"].unique())
query_dates.sort()
query_dates.extend([max(query_dates) + pd.Timedelta(days=x) for x in range(1,4)])

dfs = []
for query_date in query_dates:
    start_time = time.time()
    print(query_date)
    dates = [query_date - pd.Timedelta(days=x) for x in range(4)]
    dff = mt_df[mt_df["simple_date"].isin(dates)]
    mmsi_list = [int(x) for x in dff["Mmsi"].unique()]
    date_path = (
        f"{UNGP_BASEPATH}year={query_date.year}/month={query_date.month:02d}/"
        f"day={query_date.day:02d}"
    )
    sp_in = spark.read.parquet(date_path)
    sp_in_f = sp_in.filter(sp_in.mmsi.isin(mmsi_list))[cols]
    df_in = sp_in_f.toPandas()
    dfs.append(df_in)
    print("duration:", f"{time.time() - start_time:.0f}s")
```

```python3
ais_df = pd.concat(dfs, ignore_index=True)
```

```python3
ais_df["current_date"] = ais_df["dt_pos_utc"].dt.date
```

```python3
# see how many departures there were
len(mt_df)
```

```python3
# see how many unique ships departed
mt_df["Mmsi"].nunique()
```

```python3
# see the ships that departed at least once
doubled_mmsis = mt_df[mt_df.duplicated(subset="Mmsi")]["Mmsi"].unique()
```

```python3
def get_most_recent_port_departure(ais_row):
    current_date, mmsi = ais_row[["current_date", "mmsi"]]
    dff = mt_df[(mt_df["simple_date"] <= current_date) & (mt_df["Mmsi"] == mmsi)]
    # interestingly, the AIS returns some values that are actually
    # from the previous day (but very close to midnight),
    # so sometimes the dff is empty, since it's looking a day early
    if dff.empty:
        return None
    return dff["simple_date"].max()
```

```python3
# this is definitely not the most efficient way to do this, but it works for now
ais_df["departure_date"] = ais_df.apply(get_most_recent_port_departure, axis=1)
```

```python3
ais_df
```

```python3
data = ais_df.to_csv(index=False)
blob_client = container_client.get_blob_client(str(UNGP_RAW_PATH))
blob_client.upload_blob(data, overwrite=True)
```

```python3

```
