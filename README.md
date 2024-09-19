# parqflow
A new fast-to-read and highly compressed wind flow format for sharing wind speedups.

## Format description

Parqflow files hold flow timeseries data in Apache's .parquet format
The data itself is stored internally as columns which allows for great compression.
However, users manipulate the data as a table of several columns. Furthermore, metadata allows for appending extra dimensions without changing the format of data (think of several hub heights, several directions, etc...)
Each column of data also belongs to a regular rectangular grid which can be reconstructed from the additional (different) metadata that describes a parqflow set of files (as shown at [Adding eastings and northings](#Adding-eastings-and-northings). Attached to these data one finds the following fields (showing sample values):

* version: '0.0.1'
* stamp: datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S+0000')
* engine: 'StarCCM+'
* levels: ['case', 'instrument', 'sector', 'unit', 'variable']
* dtypes: ['str', 'str', 'float', 'str', 'str']
* epsg: 25048
* dx: 10
* dy: 10
* nx: 873
* ny: 643
* min_x: 1507990
* min_y: 6911090
* max_x: 1513410
* max_y: 6950810

These allow one to reconstruct a gridded representation of the data by following row-major order.

Each column of a parquet file is named as a tuple of (case, instrument, sector, unit, variable)

* case: any of Neutral, Stable, Combined, NeutralBlended, StableBlended, CombinedBlended
  * where Blended cases represent weighted averages of all masts (so any mast name in these .parquet files are meaningless)
* instrument: MeasurementInstrumentName_HubHeight
* sector: the center of direction sector that this column represents. All-dir for the overall, all-directional value
* unit: the physical unit that the column of data represents
* variable: speed, turbulence_intensity, ...

## Imports

```python
from pathlib import Path

import pandas as pd
import numpy as np

# from the same folder as setup.py parqflow can be installed with pip install -e .
import parqflow as pf
```

## Reading with filters

```python
# this example only works with an earlier version of parqflow, see 'levels' above for supported levels

folder = Path(r'C:\minuano\cfd_file_format_proposal_sample_files')

all_possible_filters = {
    'project': ['Project'],
    'inlet': [
        4.0, 11.0, 17.0, 21.0, 26.0, 30.0, 34.0, 39.0, 43.0,
        47.0, 51.0, 56.0, 60.0, 64.0, 69.0, 73.0, 78.0, 84.0,
        90.0,  96.0, 102.0, 110.0, 120.0, 130.0, 143.0, 158.0,
        180.0, 210.0, 240.0, 270.0, 300.0, 330.0, 349.0, 356.0
    ],
    'hub_height': [110, 115, 120, 125],
    'unit': ['m_per_s', 'degrees', 'percent', 'm'],
    'variable': ['horizontal_wind_speed', 'horizontal_wind_direction', 'wind_turbulence_intensity', 'upflow', 'elevation_at_hub_height'],
}

filters = {
    # 'inlet': [0],
    'hub_height': [110, 115],
    'variable': ['horizontal_wind_speed'],
}

pf.filter_dataset(folder, filters)
```

## Reading metadata (requires the pf library)

```python
df = pf.filter_dataset(folder, filters)
df

path = folder / 'SampleProject_000_isoheightSurface_110m.parquet'
metadata = pf.read_metadata(path)
min_x, min_y = metadata['min_x'], metadata['min_y']
max_x, max_y = metadata['max_x'], metadata['max_y']
```

## Filtering points at turbine positions

```python
pois = pd.DataFrame({
    'x': np.random.uniform(min_x, max_x, 10_000_000), // 10 million turbines!
    'y': np.random.uniform(min_y, max_y, 10_000_000),
})
pois

pf.filter_grid_points(pois, df, metadata)
```

## Adding eastings and northings

```python
metadata = pf.read_metadata(path)
min_x, min_y = metadata['min_x'], metadata['min_y']
max_x, max_y = metadata['max_x'], metadata['max_y']
dx, dy = metadata['dx'], metadata['dy']
xx = np.arange(min_x, max_x + dx, dx)
yy = np.arange(min_y, max_y + dy, dy)
tuples = [(x,y) for y in yy for x in xx]
location_df = pd.DataFrame(tuples, columns=['x', 'y'])
location_aware_df = pd.concat([df, location_df], axis=1).set_index(['x', 'y'])
location_aware_df.loc[1508000.0, 6912100.0]
```
