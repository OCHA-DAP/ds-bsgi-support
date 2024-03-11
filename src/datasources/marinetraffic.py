import os
from pathlib import Path

import pandas as pd

BSGI_DATA_DIR = Path(os.getenv("BSGI_DATA_DIR"))
MT_RAW_DIR = BSGI_DATA_DIR / "data" / "raw" / "marinetraffic"
MT_PROC_DIR = BSGI_DATA_DIR / "data" / "processed" / "marinetraffic"


def mt_export_to_jupyter_csv(filename):
    df = pd.read_csv(MT_RAW_DIR / filename, parse_dates=["Ata/atd"])
    cols = ["Vessel Name", "Ata/atd", "Mmsi", "Imo"]
    core = df[cols].copy()
    core["simple_date"] = core["Ata/atd"].dt.date
    # Create date range with additional 3 days
    core["date_1"] = core["simple_date"]
    core["date_2"] = core["simple_date"] + pd.Timedelta(days=1)
    core["date_3"] = core["simple_date"] + pd.Timedelta(days=2)
    core["date_4"] = core["simple_date"] + pd.Timedelta(days=3)

    # Pivot the data to long format
    voyages = core.melt(
        id_vars=["Vessel Name", "Mmsi", "Imo"],
        value_vars=["date_1", "date_2", "date_3", "date_4"],
        var_name="range",
        value_name="date",
    )

    # Prepare for Spark code
    voyages["date"] = pd.to_datetime(voyages["date"])
    grouped = voyages.groupby("date")["Mmsi"].apply(list)

    # De-duplicate and prepare strings for Python array-style
    grouped = grouped.apply(lambda x: list(set(x)))
    grouped = grouped.apply(lambda x: str(x).replace("'", ""))

    # Generate snippets
    snippets = pd.DataFrame({"mmsi_list": grouped, "date": grouped.index})
    snippets["snip"] = "mmsi_list = " + snippets["mmsi_list"]
    snippets["snip2"] = (
        "start_date = datetime.fromisoformat('"
        + snippets["date"].dt.strftime("%Y-%m-%d")
        + "')"
    )
    snippets["snip3"] = (
        "end_date = datetime.fromisoformat('"
        + snippets["date"].dt.strftime("%Y-%m-%d")
        + "')"
    )
    filestem = filename.removesuffix(".csv")
    save_filename = f"{filestem}_jupytersnippets.csv"
    snippets.to_csv(MT_PROC_DIR / save_filename, index=False)
