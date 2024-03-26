import os
from pathlib import Path

import geopandas as gpd
import pandas as pd

from src.utils import blob

BSGI_DATA_DIR = Path(os.getenv("BSGI_DATA_DIR"))
UNGP_RAW_DIR = BSGI_DATA_DIR / "data" / "raw" / "ungp"
UNGP_RAW_BLOB = Path("raw/ungp")
UNGP_PROC_BLOB = Path("processed/ungp")


def load_raw_ungp(filename: str):
    filepath = UNGP_RAW_BLOB / filename
    df = blob.load_blob_csv(str(filepath))
    date_cols = ["dt_pos_utc", "departure_time", "arrival_time"]
    for col in date_cols:
        df[col] = pd.to_datetime(df[col])
    return df


def process_ungp(filename: str):
    df = load_raw_ungp(filename)
    df = df.sort_values("dt_pos_utc")
    gdf = gpd.GeoDataFrame(
        data=df, geometry=gpd.points_from_xy(df["longitude"], df["latitude"])
    )
    gdf = gdf.set_crs(4326)
    gdf = gdf.to_crs(3857)

    def validate_ais_points(group):
        group["distance"] = group.geometry.distance(group.geometry.shift())
        group["timespan"] = group["dt_pos_utc"].diff().dt.total_seconds()
        group["speed"] = group["distance"] / group["timespan"]
        group["next_speed"] = group["speed"].shift(-1)
        group["valid"] = ~((group["next_speed"] > 25) & (group["speed"] > 25))
        return group

    gdf = (
        gdf.groupby("mmsi")
        .apply(validate_ais_points, include_groups=False)
        .reset_index()
    )

    gdf.loc[(gdf["latitude"] == 0) & (gdf["longitude"] == 0), "valid"] = False

    save_filename = filename.replace(".csv", "_processed.csv")
    blob.upload_blob_csv(
        str(UNGP_PROC_BLOB / save_filename), gdf.drop(columns=["geometry"])
    )


def load_proc_ungp(filename: str):
    filepath = UNGP_PROC_BLOB / filename
    df = blob.load_blob_csv(str(filepath))
    date_cols = ["dt_pos_utc", "departure_time", "arrival_time"]
    for col in date_cols:
        df[col] = pd.to_datetime(df[col])
    return df
