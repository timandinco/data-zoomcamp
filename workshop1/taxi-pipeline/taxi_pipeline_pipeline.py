"""Template for building a `dlt` pipeline to ingest data from a REST API."""

import dlt
from dlt.sources.rest_api import rest_api_resources
from dlt.sources.rest_api.typing import RESTAPIConfig


# the NYC taxi API does not require authentication and is paginated
@dlt.source
def taxi_pipeline_rest_api_source():
    """Define dlt resources from the NYC taxi REST API.

    The service returns up to 1000 records per page and accepts a `page`
    query parameter.  Pagination is handled by the `page_number` paginator
    which will increment the page starting from 1 and stop when an empty
    list is returned.
    """
    config: RESTAPIConfig = {
        "client": {
            # base URL for the zoomcamp taxi data API
            "base_url": "https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api",
            # no auth required for this public dataset
        },
        "resources": [
            {
                "name": "taxi_trips",
                "endpoint": {
                    # the base_url already points to the endpoint
                    "path": "",
                    "method": "GET",
                    # data is returned as a plain JSON array
                    "data_selector": "$",
                    # page-number based pagination, start at 1 and stop on empty
                    "paginator": {
                        "type": "page_number",
                        "base_page": 1,
                        "page_param": "page",
                        # stop_after_empty_page defaults to True so we don't
                        # need to specify it explicitly
                    },
                },
            },
        ],
        # no global defaults needed for this simple source
    }

    yield from rest_api_resources(config)


pipeline = dlt.pipeline(
    pipeline_name='taxi_pipeline',
    destination='duckdb',
    # `refresh="drop_sources"` ensures the data and the state is cleaned
    # on each `pipeline.run()`; remove the argument once you have a
    # working pipeline.
    refresh="drop_sources",
    # show basic progress of resources extracted, normalized files and load-jobs on stdout
    progress="log",
)


if __name__ == "__main__":
    load_info = pipeline.run(taxi_pipeline_rest_api_source())
    print(load_info)  # noqa: T201
