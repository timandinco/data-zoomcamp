#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm

year = 2021
month = 1

pg_user = 'root'
pg_pass = 'root'
pg_host = 'localhost'
pg_port = '5432'
pg_db = 'ny_taxi'

chunksize = 100000

connection_string = f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}'

prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow'
url = f'{prefix}/yellow_tripdata_{year}-{month:02d}.csv.gz'

dtype = {
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

parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]


engine = create_engine(connection_string)


def run():

    df_iter = pd.read_csv(
        url,
        dtype=dtype, 
        parse_dates=parse_dates,
        iterator=True, 
        chunksize=chunksize,
    )

    first_chunk = next(df_iter)

    first_chunk.head(0).to_sql(
        name="yellow_taxi_data",
        con=engine,
        if_exists="replace"
    )

    print("Table created")

    first_chunk.to_sql(
        name="yellow_taxi_data",
        con=engine,
        if_exists="append"
    )

    print("Inserted first chunk:", len(first_chunk))

    for df_chunk in df_iter:
        df_chunk.to_sql(
            name="yellow_taxi_data",
            con=engine,
            if_exists="append"
        )
        print("Inserted chunk:", len(df_chunk))


if __name__ == "__main__":
    run()