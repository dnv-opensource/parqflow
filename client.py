from pathlib import Path

import pandas as pd
import numpy as np

import parqflow as pf

folder = Path(r'E:\OneDrive - DNV\cfd_file_format_proposal_sample_files')

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


df = pf.filter_dataset(folder, filters={})
df

path = folder / 'SampleProject_000_isoheightSurface_110m.parquet'
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


pois = pd.DataFrame({
    'x': np.random.uniform(min_x, max_x, 10_000_000), # 10 million turbines!
    'y': np.random.uniform(min_y, max_y, 10_000_000),
})
pois

pf.filter_grid_points(pois, df, metadata)
