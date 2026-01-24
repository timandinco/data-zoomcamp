#!/usr/bin/env python
# coding: utf-8

import glob
import os
import click
import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm

# defaults 
_default_year = 2025
_default_month = 11

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
# allow multiple --file flags
@click.option("--file", "-f", multiple=True, help="Path to a file or a glob pattern (e.g. './*.parquet' or './data.csv'). Can be used multiple times.")
@click.option("--pg-user", default="root", show_default=True, help="Postgres user")
@click.option("--pg-pass", default="root", hide_input=True, help="Postgres password")
@click.option("--pg-host", default="localhost", show_default=True, help="Postgres host")
@click.option("--pg-port", default="5432", show_default=True, help="Postgres port")
@click.option("--pg-db", default="ny_taxi", show_default=True, help="Postgres database name")
@click.option("--chunksize", default=100000, type=int, show_default=True, help="Number of rows per chunk")
@click.option("--table", default="green_taxi_data", show_default=True, help="Target table name in the database")
@click.option("--url", default=None, help="Full URL to the CSV.GZ file (if not provided it will be constructed)")
def run(file, year, month, pg_user, pg_pass, pg_host, pg_port, pg_db, chunksize, table, url, workdir, debug):
    """
    Ingest NYC green taxi data into Postgres. File can be specified directly or constructed from year/month.
    """
    click.echo(f"Processing file(s): {file}")
    click.echo(f"Current working directory: {os.getcwd()}")

    connection_string = f"postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}"
    engine = create_engine(connection_string)

    files_to_ingest = []
    if file:
        # file is a tuple when multiple=True; allow multiple patterns/paths
        patterns = list(file)
        # expand each pattern using glob (relative to cwd or absolute)
        missing = []
        for pat in patterns:
            matches = sorted(glob.glob(pat))
            click.echo(f"Pattern '{pat}' -> matches: {matches}")
            if matches:
                files_to_ingest.extend(matches)
            else:
                # keep track of patterns that matched nothing
                missing.append(pat)

        if not files_to_ingest:
            # none of the provided patterns matched any files
            # raise a useful error listing the patterns
            raise click.FileError(
                ','.join(patterns), hint=f"No files matched the given path/pattern(s): {missing}")

    for path in files_to_ingest:
        click.echo(f"Ingesting: {path}")

        # Decide read method by extension. For remote URLs ending with .csv(.gz) use CSV path
        lower = path.lower()
        if_exists_mode = "replace" if _should_create_table(engine, table) else "append"

        if lower.endswith(".parquet") or lower.endswith(".parquet.gzip") or lower.endswith(".parquet.gz"):
            # parquet -> read fully, then to_sql
            # How to chunk later?
            df = pd.read_parquet(path)
            # create table if not exists (first file)
            df.to_sql(name=table, con=engine, if_exists=if_exists_mode, index=False)
            click.echo(f"Wrote parquet rows: {len(df)}")
        elif lower.endswith(".csv") or lower.endswith(".csv.gz") or lower.endswith(".gz"):
            # CSV: stream in chunks to avoid memory pressure.
            df_iter = pd.read_csv(path, dtype=dtype, parse_dates=parse_dates, iterator=True, chunksize=chunksize)
            # first chunk: create table schema
            try:
                first_chunk = next(df_iter)
            except StopIteration:
                click.echo("CSV file is empty, skipping.")
                continue

            # write table header (replace on first file only)
            first_chunk.head(0).to_sql(name=table, con=engine, if_exists=if_exists_mode, index=False)
            first_chunk.to_sql(name=table, con=engine, if_exists="append", index=False)
            click.echo(f"Wrote first chunk rows: {len(first_chunk)}")

            for chunk in tqdm(df_iter, desc=f"Ingesting chunks from {os.path.basename(path)}"):
                chunk.to_sql(name=table, con=engine, if_exists="append", index=False)
        else:
            raise click.UsageError(f"Unsupported file extension for '{path}'. Supported: .parquet, .csv, .csv.gz")

def _should_create_table(engine, table_name):
    # helper: returns True if table doesn't exist yet (so we should use replace/create)
    from sqlalchemy import inspect
    inspector = inspect(engine)
    return table_name not in inspector.get_table_names()


if __name__ == "__main__":
    run()