import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year
from datetime import datetime

# Initialize an isolated local Spark session for testing
@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .master("local[1]") \
        .appName("pytest-pyspark-local") \
        .getOrCreate()

def test_data_cleaning_logic(spark):
    # Sample dataset containing various scenarios (clean and dirty data)
    mock_data = [
        (1, 2.5, 15.0, datetime(2024, 1, 15, 10, 0)),   # Valid record
        (0, 2.5, 15.0, datetime(2024, 1, 15, 10, 0)),   # Invalid: No passengers
        (1, -1.0, 15.0, datetime(2024, 1, 15, 10, 0)),  # Invalid: Negative distance
        (1, 2.5, 15.0, datetime(2023, 12, 31, 23, 59)), # Invalid: Outlier year
        (2, 5.0, -10.0, datetime(2024, 2, 10, 14, 0))   # Invalid: Negative amount
    ]
    columns = ["passenger_count", "trip_distance", "total_amount", "tpep_pickup_datetime"]
    df = spark.createDataFrame(mock_data, columns)

    # Apply the exact filtering logic used in transform.py
    df_filtered = df.filter(
        (col("passenger_count") > 0) & 
        (col("trip_distance") > 0.0) & 
        (col("total_amount") > 0.0) &
        (year(col("tpep_pickup_datetime")) == 2024)
    )

    # Assert that only 1 out of 5 rows (the valid one) remains
    assert df_filtered.count() == 1, "Data cleaning logic failed!"