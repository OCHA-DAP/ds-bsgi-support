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

# Marine Traffic

```python
%load_ext jupyter_black
%load_ext autoreload
%autoreload 2
```

```python
import pandas as pd

from tqdm.notebook import tqdm

from src.datasources import marinetraffic
```

```python
filename = "MarineTraffic_Arrivals_departures_Export_2024-03-06.csv"
marinetraffic.mt_export_to_jupyter_csv(filename)
```
