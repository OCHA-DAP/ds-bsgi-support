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
    display_name: ds-bsgi-support
    language: python
    name: ds-bsgi-support
---

# Animation of AIS paths

```python
%load_ext jupyter_black
%load_ext autoreload
%autoreload 2
```

```python
import sys
from pathlib import Path

import geopandas as gpd
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

from src.datasources import ungp
from src.utils import blob
```

```python
blob.list_blobs(ungp.UNGP_RAW_BLOB)
```

```python
ungp.process_ungp(
    "greater-odesa-arrivals-departures-2024-03-01-2024-03-10-more-than-1-dwt_ais.csv"
)
```

```python
filename = "greater-odesa-arrivals-departures-2024-03-01-2024-03-10-more-than-1-dwt_ais_processed.csv"
df = ungp.load_proc_ungp(filename)
df = df.sort_values("dt_pos_utc")
df = df[df["valid"]]
```

```python
df
```

```python
sys.getsizeof(df) / 10e6
```

```python
df["mmsi"].unique()
```

```python
df["dt_pos_utc_original"] = df["dt_pos_utc"]


def custom_resampler(group, resample_period):
    resampled = group.resample(resample_period, label="right").last()

    valid_rows = resampled.index - resampled[
        "dt_pos_utc_original"
    ] <= pd.Timedelta(resample_period)

    return resampled[valid_rows]


df_animation = (
    df.set_index("dt_pos_utc")
    .groupby("mmsi")
    .apply(lambda x: custom_resampler(x, "6h"), include_groups=False)
    .reset_index()
)

df_tracks = (
    df.set_index("dt_pos_utc")
    .groupby("mmsi")
    .apply(lambda x: custom_resampler(x, "2h"), include_groups=False)
    .reset_index()
)
```

```python
ARRIVAL_COLOR = "darkorange"
DEPARTURE_COLOR = "blue"

df_animation["color"] = (
    df_animation["departure_time"]
    .isnull()
    .apply(lambda x: ARRIVAL_COLOR if x else DEPARTURE_COLOR)
)

fig = px.scatter_mapbox(
    df_animation,
    lat="latitude",
    lon="longitude",
    animation_frame="dt_pos_utc",  # This column controls the animation steps
    color="color",
    color_discrete_map="identity",
    size_max=5,
    # hover_name="yet_another_column", # Optional: info shown on hover
    # mapbox_style="mapbox_style", # Choose your map style
)
# fig.update_traces(marker_size=5)

j = 0
for mmsi, group in df_tracks.groupby("mmsi"):
    # if j > 5:
    #     break
    arrival = group[~group["arrival_time"].isnull()]
    fig.add_trace(
        go.Scattermapbox(
            lat=arrival["latitude"],
            lon=arrival["longitude"],
            mode="lines",
            line=dict(color=ARRIVAL_COLOR, width=0.2),
            name=mmsi,
        )
    )
    departure = group[~group["departure_time"].isnull()]
    fig.add_trace(
        go.Scattermapbox(
            lat=departure["latitude"],
            lon=departure["longitude"],
            mode="lines",
            line=dict(color=DEPARTURE_COLOR, width=0.2),
            name=mmsi,
        )
    )
    j += 1

fig.update_layout(
    mapbox_style="carto-positron",
    mapbox_zoom=4,
    mapbox_center_lat=40,
    mapbox_center_lon=30,
    margin={"r": 0, "t": 0, "l": 0, "b": 0},
    # height=600,
    showlegend=False,
)

# fig.layout.updatemenus[0].buttons[0].args[1]["frame"]["duration"] = 100
# fig.layout.updatemenus[0].buttons[0].args[1]["transition"]["duration"] = 10

fig.show()
```

```python
plot_name = filename.replace(".csv", "_plot.html")
PLOTS_DIR = Path("../plots")

f = open(PLOTS_DIR / plot_name, "w")
f.close()
with open(PLOTS_DIR / plot_name, "a") as f:
    f.write(
        fig.to_html(full_html=True, include_plotlyjs="cdn", auto_play=False)
    )
f.close()
```

```python

```
