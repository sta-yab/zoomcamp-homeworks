#!/usr/bin/env python
# coding: utf-8

import click
import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm

# Yellow taxi schema
yellow_dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

yellow_parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]

# Zones CSV schema
zones_dtype = {
    "LocationID": "Int64",
    "Borough": "string",
    "Zone": "string",
    "service_zone": "string"
}


def ingest_file(file_path, table_name, engine, dtype=None, parse_dates=None, chunksize=100000):

    if file_path.endswith(".parquet"):
        # Parquet cannot be chunked
        df = pd.read_parquet(file_path)
        df.to_sql(name=table_name, con=engine, if_exists='replace', index=False)
        return

    elif file_path.endswith(".csv"):
        df_iter = pd.read_csv(
            file_path,
            dtype=dtype,
            parse_dates=parse_dates,
            iterator=True,
            chunksize=chunksize
        )
    else:
        raise ValueError(f"Unsupported file type: {file_path}")

    first = True
    for df_chunk in tqdm(df_iter):
        if first:
            df_chunk.head(0).to_sql(
                name=table_name,
                con=engine,
                if_exists='replace',
                index=False
            )
            first = False

        df_chunk.to_sql(
            name=table_name,
            con=engine,
            if_exists='append',
            index=False
        )


@click.command()
@click.option('--pg-user', default='root')
@click.option('--pg-pass', default='root')
@click.option('--pg-host', default='localhost')
@click.option('--pg-port', default=5432, type=int)
@click.option('--pg-db', default='ny_taxi')
@click.option('--chunksize', default=100000, type=int)
def run(pg_user, pg_pass, pg_host, pg_port, pg_db, chunksize):

    engine = create_engine(
        f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}'
    )

    ingest_file(
        file_path="homework/green_tripdata_2025-11.parquet",
        table_name="green_taxi_data",
        engine=engine,
        dtype=None,              # parquet does not use dtype
        parse_dates=None,        # parquet already has timestamps
        chunksize=None           # parquet cannot be chunked
    )

    ingest_file(
        file_path="homework/taxi_zone_lookup.csv",
        table_name="zones_lookup",
        engine=engine,
        dtype=zones_dtype,
        parse_dates=None,
        chunksize=chunksize
    )


if __name__ == '__main__':
    run()
