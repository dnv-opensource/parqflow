from pathlib import Path
import json
import ast

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

idx = pd.IndexSlice


def filter_dataset(folder, filters={}):
    dfs = _filter_dataset(folder, filters)
    return pd.concat(dfs, axis=1)


def _filter_dataset(folder, filters):
    for parquet_path in folder.rglob('*.parquet'):
        cols = list(_get_matching_columns(parquet_path, filters))
        if not cols:
            continue
        yield pd.read_parquet(parquet_path, columns=cols)


def _get_matching_columns(parquet_path, filters):
    metadata = pq.read_metadata(parquet_path)
    columns = metadata.schema.names
    col2tup = {col:ast.literal_eval(col) for col in columns if '__' not in col}
    cfd_metadata = json.loads(metadata.metadata[b'cfd'])
    levels = cfd_metadata['levels']
    dtypes = cfd_metadata['dtypes']
    level2dtype = {l:d for l, d in zip(levels, dtypes)}
    level2index = {l:i for i, l in enumerate(levels) if l in filters and filters[l]}
    for col, tup in col2tup.items():
        for l, i in level2index.items():
            value = tup[i]
            if level2dtype[l] == 'float':
                value = float(value)
            if value not in filters[l]:
                break
        else:
            yield col

def read_metadata(parquet_path):
    metadata = pq.read_metadata(parquet_path)
    metadata = json.loads(metadata.metadata[b'cfd'])
    metadata['dx'] = float(metadata['dx'])
    metadata['dy'] = float(metadata['dy'])
    metadata['nx'] = int(metadata['nx'])
    metadata['ny'] = int(metadata['ny'])
    metadata['min_x'] = float(metadata['min_x'])
    metadata['min_y'] = float(metadata['min_y'])
    metadata['max_x'] = float(metadata['max_x'])
    metadata['max_y'] = float(metadata['max_y'])
    metadata['epsg'] = int(metadata['epsg'])
    return metadata

def filter_grid_points(pois, df, metadata):
    min_x, min_y = metadata['min_x'], metadata['min_y']
    max_x, max_y = metadata['max_x'], metadata['max_y']
    dx, dy = metadata['dx'], metadata['dy']
    nx, ny = metadata['nx'], metadata['ny']
    x = (pois['x'] % min_x) / dx
    y = (pois['y'] % min_y) / dy
    pois = pois.set_index(y.astype(int) * nx + x.astype(int))

    n = len(df.columns[0])-1
    tuples = [('',)*n + (c,) for c in pois]
    pois.columns = pd.MultiIndex.from_tuples(tuples, names=df.columns.names)
    return pois.join(df)

def debug_filter_grid_points(df, metadata, length=10):
    min_x, min_y = metadata['min_x'], metadata['min_y']
    max_x, max_y = metadata['max_x'], metadata['max_y']

    pois = pd.DataFrame({
        'x': np.random.uniform(min_x, max_x, length),
        'y': np.random.uniform(min_y, max_y, length),
    })

    pois = filter_grid_points(pois, df, metadata)

    poi_x = pois.loc[:, idx[:, :, :, :, 'x']].squeeze()
    grid_x = pois.loc[:, idx[:, :, :, :, 'eastings']].squeeze()
    poi_y = pois.loc[:, idx[:, :, :, :, 'x']].squeeze()
    grid_y = pois.loc[:, idx[:, :, :, :, 'northings']].squeeze()

    n = len(df.columns[0])-1
    pois.loc[:, ('',)*n + ('x_diff',)] = poi_x - grid_x
    pois.loc[:, ('',)*n + ('y_diff',)] = poi_y - grid_y

    return pois
