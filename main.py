import asyncio
import os
from typing import Dict

import polars as pl
from src.duckdb_pipe.pipe.query_logs import get_events
from duckdb_pipe.pipe.write_db import (
    write_to_duckdb,
    initialize_duckdb,
    initialize_dlt_pipeline,
)

from dotenv import load_dotenv

load_dotenv()
DB_PATH = os.getenv("DB_PATH", "data/mev_commit_testnet.duckdb")
PIPELINE_DIR = os.getenv("PIPELINE_DIR", "data/pipelines")


async def run_pipeline():
    # Step 1: Initialize DuckDB if needed
    initialize_duckdb(DB_PATH)

    # Step 2: Query the events
    print("Fetching events...")
    event_dfs: Dict[str, pl.DataFrame] = await get_events()

    # Step 3: Process each event, initializing the pipeline once per event
    for event_name, df in event_dfs.items():
        print(f"Processing event: {event_name}")

        try:
            # Initialize dlt pipeline for the event, using PIPELINE_DIR
            pipeline = initialize_dlt_pipeline(event_name)

            # Write each DataFrame to DuckDB
            print(f"Writing {event_name} events to DuckDB...")
            write_to_duckdb(df, pipeline, event_name)
        except Exception as e:
            print(f"Error while writing {event_name} to DuckDB: {e}")

    print("Data pipeline completed successfully.")


async def main():
    while True:
        await run_pipeline()
        await asyncio.sleep(30)  # Wait 30 seconds before rerunning the pipeline


if __name__ == "__main__":
    asyncio.run(main())
