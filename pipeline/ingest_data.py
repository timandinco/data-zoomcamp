#!/usr/bin/env python
# coding: utf-8

import click
import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm

# defaults (kept from original)
_default_year = 2021
_default_month = 1

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


@click.command()
@click.option("--year", "-y", default=_default_year, type=int, show_default=True, help="Year of the file to ingest")
@click.option("--month", "-m", default=_default_month, type=int, show_default=True, help="Month of the file to ingest (1-12)")
@click.option("--pg-user", default="root", show_default=True, help="Postgres user")
@click.option("--pg-pass", default="root", hide_input=True, help="Postgres password")
@click.option("--pg-host", default="localhost", show_default=True, help="Postgres host")
@click.option("--pg-port", default="5432", show_default=True, help="Postgres port")
@click.option("--pg-db", default="ny_taxi", show_default=True, help="Postgres database name")
@click.option("--chunksize", default=100000, type=int, show_default=True, help="Number of rows per chunk")
@click.option("--table", default="yellow_taxi_data", show_default=True, help="Target table name in the database")
@click.option("--url", default=None, help="Full URL to the CSV.GZ file (if not provided it will be constructed)")
def run(year, month, pg_user, pg_pass, pg_host, pg_port, pg_db, chunksize, table, url):
    """
    Ingest NYC yellow taxi data into Postgres. Constructs the download URL from year/month
    unless --url is provided.
    """
    if url is None:
        prefix = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow"
        url = f"{prefix}/yellow_tripdata_{year}-{month:02d}.csv.gz"

    connection_string = f"postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}"
    engine = create_engine(connection_string)

    click.echo(f"Ingesting from: {url}")
    click.echo(f"Target DB: {pg_user}@{pg_host}:{pg_port}/{pg_db} -> table: {table}")
    click.echo(f"Chunksize: {chunksize}")

    df_iter = pd.read_csv(
        url,
        dtype=dtype,
        parse_dates=parse_dates,
        iterator=True,
        chunksize=chunksize,
    )

    first_chunk = next(df_iter)

    # create table (no rows)
    first_chunk.head(0).to_sql(
        name=table,
        con=engine,
        if_exists="replace",
        index=False
    )
    click.echo("Table created")

    # insert first chunk
    first_chunk.to_sql(
        name=table,
        con=engine,
        if_exists="append",
        index=False
    )
    click.echo(f"Inserted first chunk: {len(first_chunk)}")

    # insert remaining chunks with progress bar
    for df_chunk in tqdm(df_iter, desc="Ingesting chunks"):
        df_chunk.to_sql(
            name=table,
            con=engine,
            if_exists="append",
            index=False
        )
        click.echo(f"Inserted chunk: {len(df_chunk)}")


if __name__ == "__main__":
    run()