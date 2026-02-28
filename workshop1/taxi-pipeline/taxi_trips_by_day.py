import marimo

__generated_with = "0.8.119"
app = marimo.App()


@app.cell
def __():
    import ibis
    import marimo as mo
    import plotly.express as px
    import pandas as pd
    
    return ibis, mo, px, pd


@app.cell
def __(ibis):
    # Connect to the dlt pipeline via DuckDB
    con = ibis.duckdb.connect('taxi_pipeline.duckdb')
    
    # Access the taxi trips table
    trips = con.table('taxi_trips', database='taxi_pipeline_dataset')
    
    return con, trips


@app.cell
def __(trips, ibis):
    # Extract date from pickup datetime and group by date to count trips
    trips_by_date = (
        trips
        .mutate(pickup_date=trips.trip_pickup_date_time.cast('date'))
        .group_by('pickup_date')
        .aggregate(trip_count=ibis._.trip_pickup_date_time.count())
        .order_by(ibis._.trip_count.desc())
    )
    
    # Execute and convert to pandas
    df = trips_by_date.to_pandas()
    
    return trips_by_date, df


@app.cell
def __(df):
    # Find the day with most trips
    max_trips_day = df.iloc[0]
    
    return max_trips_day


@app.cell
def __(mo, max_trips_day):
    mo.md(f"""
    # Taxi Trips by Day Analysis
    
    **Day with most trips:** {max_trips_day['pickup_date']}  
    **Number of trips:** {max_trips_day['trip_count']}
    """)


@app.cell
def __(df, px):
    # Create visualization
    fig = px.bar(
        df.sort_values('pickup_date'),
        x='pickup_date',
        y='trip_count',
        title='Taxi Trips by Day',
        labels={'pickup_date': 'Date', 'trip_count': 'Number of Trips'},
        height=500
    )
    fig.update_xaxes(title_text='Pickup Date')
    fig.update_yaxes(title_text='Trip Count')
    
    return fig


@app.cell
def __(fig):
    return fig


@app.cell
def __(df):
    # Summary statistics table
    summary = df.reset_index(drop=True)
    summary.columns = ['Pickup Date', 'Trip Count']
    
    return summary


@app.cell
def __(mo, summary):
    return mo.md("""
    ## Daily Trip Summary
    """), mo.ui.table(summary)


if __name__ == "__main__":
    app.run()
