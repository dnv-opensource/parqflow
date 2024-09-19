def attach_metadata(parquet_path):
    df = pd.read_parquet(csv_path)
    df = df.sort_values(['Y (m)', 'X (m)'])
    unique_sorted_xs = df['X (m)'].drop_duplicates().sort_values()
    unique_sorted_ys = df['Y (m)'].drop_duplicates().sort_values()

    # drop x and y columns
    df = df[OLD_VAR_TO_NEW_VAR.keys()]
    # project is really project+stability
    # the client will likely received a combined version of stable and neutral
    project, inlet, hub_height = CFD_CSV_REGEXP.search(str(csv_path)).groups(1)
    epsg = 32619

    # scramble to be able to disclose to client
    # df = df.sample(frac=1).reset_index(drop=True)
    # epsg = 32633

    inlet, hub_height = float(inlet), float(hub_height)
    tuples = [(project, inlet, hub_height, OLD_VAR_TO_UNIT[c], OLD_VAR_TO_NEW_VAR[c]) for c in df]
    levels = ['project', 'inlet', 'hub_height', 'unit', 'variable']
    dtypes = ['str', 'float', 'float', 'str', 'str']
    df.columns = pd.MultiIndex.from_tuples(tuples, names=levels)

    cfd_metadata = {
        'version': '0.0.1',
        'stamp': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S+0000'),
        'engine': 'StarCCM+',
        # allows one to interpret the meaning of the column metadata
        'levels': levels, #['project', 'inlet', 'hub_height', 'unit', 'variable']
        'dtypes': dtypes, #['str', 'float', 'float', 'str', 'str']
        'epsg': str(epsg),
        'dx': str(unique_sorted_xs.head().diff().iloc[1]),
        'dy': str(unique_sorted_ys.head().diff().iloc[1]),
        'nx': str(unique_sorted_xs.count()),
        'ny': str(unique_sorted_ys.count()),
        'min_x': str(unique_sorted_xs.iloc[0]),
        'min_y': str(unique_sorted_ys.iloc[0]),
        'max_x': str(unique_sorted_xs.iloc[-1]),
        'max_y': str(unique_sorted_ys.iloc[-1]),
    }
    # unfortunately we have to have an index column there so that the
    # MultiIndex columns gets properly interpreted by pandas.
    # We loose on storage here but there might be a way out, just gotta
    # explore the b'pandas' metadata
    table = pa.Table.from_pandas(df, preserve_index=True)
    combined_meta = {
        b'cfd': json.dumps(cfd_metadata),
        **table.schema.metadata,
    }
    table = table.replace_schema_metadata(combined_meta)
    parquet_path = csv_path.with_suffix('.parquet')
    pq.write_table(table, parquet_path, compression='snappy')
