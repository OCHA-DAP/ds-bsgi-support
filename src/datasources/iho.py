import os
from pathlib import Path

import geopandas as gpd

BSGI_DATA_DIR = Path(os.getenv("BSGI_DATA_DIR"))
IHO_RAW_DIR = BSGI_DATA_DIR / "data" / "raw" / "iho"
IHO_RAW_PATH = IHO_RAW_DIR / "iho" / "iho.shp"
IHO_PROC_DIR = BSGI_DATA_DIR / "data" / "processed" / "iho"
BLACKSEA_IDS = ["30", "31"]


def load_raw_iho():
    return gpd.read_file(IHO_RAW_PATH)


def process_blacksea_buffer():
    gdf = load_raw_iho()
    black_sea = gdf[gdf["id"].isin(BLACKSEA_IDS)]
    black_sea = black_sea.to_crs(3857)
    buffer_distance_km = 50
    buffer = black_sea.dissolve().buffer(buffer_distance_km * 1000)
    buffer = buffer.to_crs(4326)
    filename = f"black_sea_buffer_{buffer_distance_km}km"
    buffer.to_file(IHO_PROC_DIR / filename)


def load_blacksea_buffer():
    filename = "black_sea_buffer_50km"
    return gpd.read_file(IHO_PROC_DIR / filename)
