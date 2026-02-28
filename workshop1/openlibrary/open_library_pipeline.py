"""Template for building a `dlt` pipeline to ingest data from a REST API."""

import dlt
from dlt.sources.rest_api import rest_api_resources
from dlt.sources.rest_api.typing import RESTAPIConfig


# if no argument is provided, `access_token` is read from `.dlt/secrets.toml`
@dlt.source
# allow the caller to specify which bibkeys to fetch; defaults to a small sample
# you could use dlt.secrets to store more advanced configs if needed

def open_library_rest_api_source(
    bibkeys: str = "ISBN:0451526538,ISBN:0201558025"
):
    """Define dlt resources from REST API endpoints.

    The Open Library `books` endpoint returns metadata for one or more
    identifiers provided via the `bibkeys` query parameter.  We include a
    default set of ISBNs so the pipeline can run out of the box.  More
    parameters can be added or overridden when the source is invoked.
    """

    config: RESTAPIConfig = {
        "client": {
            # base URL for Open Library REST API
            "base_url": "https://openlibrary.org",
            # no authentication is required for this public endpoint
        },
        "resources": [
            {
                "name": "books",
                "endpoint": {
                    "path": "api/books",
                    "method": "GET",
                    "params": {
                        # the required query parameters for the books API
                        "bibkeys": bibkeys,
                        "format": "json",
                        "jscmd": "data",
                    },
                    # the API returns a plain JSON object keyed by the
                    # provided identifiers; selecting the root (`$`) is
                    # sufficient and avoids parsing errors due to the
                    # colon characters in the keys.
                    "data_selector": "$",
                },
            },
        ],
        # no resource_defaults needed for this simple example
    }

    yield from rest_api_resources(config)


pipeline = dlt.pipeline(
    pipeline_name='open_library_pipeline',
    destination='duckdb',
    # `refresh="drop_sources"` ensures the data and the state is cleaned
    # on each `pipeline.run()`; remove the argument once you have a
    # working pipeline.
    refresh="drop_sources",
    # show basic progress of resources extracted, normalized files and load-jobs on stdout
    progress="log",
)


if __name__ == "__main__":
    load_info = pipeline.run(open_library_rest_api_source())
    print(load_info)  # noqa: T201
