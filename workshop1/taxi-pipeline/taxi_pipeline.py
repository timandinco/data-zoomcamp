"""Pipeline for NYC taxi REST API using dlt."""

import dlt
from dlt.sources.rest_api import rest_api_resources
from dlt.sources.rest_api.typing import RESTAPIConfig


@dlt.source

def taxi_pipeline_rest_api_source():
    """Source that reads paginated taxi trip data from the public API."""
    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api",
        },
        "resources": [
            {
                "name": "taxi_trips",
                "endpoint": {
                    "path": "",
                    "method": "GET",
                    "paginator": {
                        "type": "page_number",
                        "base_page": 1,
                        "page_param": "page",
                        "total_path": None,
                    },
                },
            },
        ],
    }

    yield from rest_api_resources(config)


pipeline = dlt.pipeline(
    pipeline_name="taxi_pipeline",
    destination="duckdb",
    refresh="drop_sources",
    progress="log",
)


if __name__ == "__main__":
    load_info = pipeline.run(taxi_pipeline_rest_api_source())
    print(load_info)  # noqa: T201
