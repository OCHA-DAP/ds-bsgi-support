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

# AIS

```python
import os
from pathlib import Path
```

```python
from getpass import getpass
```

```python
GITLAB_TOKEN = getpass(prompt="GITLAB_TOKEN:")
```

```python
os.getcwd()
```

```python
import sys
import subprocess

# Install the ais modue
GITLAB_USER = "read_aistt"  #For use of members of AIS Task Team, read only access
git_package = f"git+https://{GITLAB_USER}:{GITLAB_TOKEN}@code.officialstatistics.org/trade-task-team-phase-1/ais.git"

std_out = subprocess.run(
    [sys.executable, "-m", "pip", "install", git_package],
    capture_output=True,
    text=True
).stdout
print(std_out)
```

```python
from ais import functions as af
```

```python
from pyspark.sql import SparkSession
```

```python
spark
```

```python
spark.conf.set("spark.sql.parquet.enableVectorizedReader", "false")
```
