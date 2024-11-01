import dlt
import duckdb
import polars as pl
import os

from dotenv import load_dotenv


load_dotenv()
DB_PATH = os.getenv("DB_PATH", "data/mev_commit_testnet.duckdb")
PIPELINE_DIR = os.getenv("PIPELINE_DIR", "data/pipelines")


def initialize_duckdb(db_path: str) -> None:
    """
    Ensure the database file and directory exist. Initialize a new DuckDB database if necessary.

    Parameters:
    - db_path (str): The file path for the DuckDB database.

    Returns:
    - None
    """
    # Create the directory if it doesn't exist
    os.makedirs(os.path.dirname(db_path), exist_ok=True)

    # Connect to the DuckDB database to initialize the file if it doesn't exist
    conn: duckdb.DuckDBPyConnection = duckdb.connect(db_path)
    conn.close()


def initialize_dlt_pipeline(event_name: str) -> dlt.Pipeline:
    """
    Initialize a dlt pipeline for DuckDB with the specified parameters.

    Parameters:
    - event_name (str): The name of the event to create a unique dataset name.

    Returns:
    - pipeline (dlt.Pipeline): The initialized dlt pipeline.
    """

    return dlt.pipeline(
        pipeline_name="mev_commit_testnet",
        destination="duckdb",
        # dataset_name=event_name,
        pipelines_dir=PIPELINE_DIR,
    )


def write_to_duckdb(df: pl.DataFrame, pipeline: dlt.Pipeline, event_name: str):
    """
    Load the Polars DataFrame into a DuckDB table using dlt.

    Parameters:
    - df (pl.DataFrame): The Polars DataFrame to be loaded.
    - pipeline (dlt.Pipeline): The dlt pipeline object to load data into DuckDB.
    - event_name (str): The event name for logging purposes.
    """
    # Skip if DataFrame is empty
    if df.is_empty():
        print(f"No data to write for {event_name}, skipping table creation.")
        return

    # Preprocess column names to avoid normalization collisions
    df = preprocess_column_names(df)

    # Convert DataFrame to Arrow format for dlt
    data = df.to_arrow()

    # Load data into the DuckDB table with dlt
    pipeline.run(data, table_name=event_name)
    print(f"Data for {event_name} loaded successfully.")


def preprocess_column_names(df: pl.DataFrame) -> pl.DataFrame:
    # Map columns to unique names to avoid normalization collisions
    column_mapping = {
        "blockNumber": "l1_block_number",
    }

    # Only rename columns that exist in the DataFrame
    column_mapping = {
        col: new_col for col, new_col in column_mapping.items() if col in df.columns
    }

    # Rename columns in DataFrame
    return df.rename(column_mapping)
