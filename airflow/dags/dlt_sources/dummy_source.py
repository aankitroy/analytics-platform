#/airflow/dags/dlt_sources/dummy_source.py
# A simple dummy dlt source for testing batch ingestion.

import dlt
import logging
from datetime import datetime

@dlt.source
def dummy_data_source(some_param=None):
    """A dummy source that yields a list of dictionaries."""
    
    log = logging.getLogger(__name__) # Get logger inside the source

    # Define a resource that yields data
    @dlt.resource
    def users():
        log.info("Generating dummy user data...")
        # Yield batches of data
        yield [
            {"user_id": 1, "name": "Alice", "city": "New York", "signup_time": datetime.utcnow()},
            {"user_id": 2, "name": "Bob", "city": "Los Angeles", "signup_time": datetime.utcnow()},
            {"user_id": 3, "name": "Charlie", "city": "Chicago", "signup_time": datetime.utcnow()},
        ]
        log.info("Finished generating dummy user data.")

    @dlt.resource
    def page_views():
        log.info("Generating dummy page view data...")
        # Yield more dummy data
        yield [
            {"view_id": 101, "user_id": 1, "page": "/home", "timestamp": datetime.utcnow()},
            {"view_id": 102, "user_id": 2, "page": "/products", "timestamp": datetime.utcnow()},
            {"view_id": 103, "user_id": 1, "page": "/about", "timestamp": datetime.utcnow()},
        ]
        log.info("Finished generating dummy page view data.")

    # Return the resources you want to load
    return users, page_views

# Note: You might need to add import dlt in your ingest.py DAG file
# and potentially configure the source within the task.