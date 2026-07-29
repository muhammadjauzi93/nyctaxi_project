from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp

# Utility functions for adding metadata columns to Spark DataFrames.

def add_ingestion_date(df: DataFrame) -> DataFrame:
    """Add ingestion metadata to the input DataFrame.

    This function appends a 'processed_timestamp' column and populates it
    with the current timestamp at the time of processing.

    Args:
        df: Input Spark DataFrame.

    Returns:
        Spark DataFrame with the added 'processed_timestamp' column.
    """
    # Add a column 'processed_timestamp' with the current timestamp to the DataFrame
    return df.withColumn("processed_timestamp", current_timestamp())