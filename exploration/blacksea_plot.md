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

# Black Sea plot

```python
%load_ext jupyter_black
%load_ext autoreload
%autoreload 2
```

```python
import geopandas as gpd
import matplotlib.pyplot as plt

from src.datasources import iho, ungp
from src.utils import blob
```

```python
seas = iho.load_raw_iho()
blacksea = seas[seas["id"].isin(iho.BLACKSEA_IDS)]
```

```python
df = ungp.load_ungp(
    "MarineTraffic_Arrivals_departures_Export_2024-03-06_ais.csv"
)
```

```python
df
```

```python
buffer = iho.load_blacksea_buffer()
```

```python
buffer.crs
```

```python
gdf = gpd.GeoDataFrame(
    df, geometry=gpd.points_from_xy(df.longitude, df.latitude), crs=4326
)
```

```python
gdf.geometry
```

```python
buffer.geometry[0]
```

```python
gdf["in_blacksea"] = gdf.within(buffer.geometry[0])
```

```python
gdf["in_blacksea"]
```

```python
fig, ax = plt.subplots(figsize=(10, 10))
buffer.boundary.plot(
    ax=ax, color="k", linewidth=0.5, linestyle="dashed", alpha=0.2
)
ax.axis("off")
gdf[gdf["in_blacksea"]].plot(ax=ax, color="blue", markersize=0.2)
gdf[~gdf["in_blacksea"]].plot(ax=ax, color="red", markersize=0.2)
blacksea.plot(ax=ax, color="k", alpha=0.2)
```

```python

```
