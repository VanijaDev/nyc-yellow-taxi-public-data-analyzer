from pathlib import Path
from typing import Any

import pandas as pd
import pytest

FIXTURE_PATH = Path(__file__).parent / "fixtures/sample_trips.parquet"


@pytest.fixture
def raw_df() -> pd.DataFrame:
    """Fixture that loads the raw DataFrame from a Parquet file."""
    return pd.read_parquet(FIXTURE_PATH)


@pytest.fixture
def valid_row() -> dict[str, Any]:
    return {
        "VendorID": 1,
        "tpep_pickup_datetime": pd.Timestamp("2024-01-01 08:00"),
        "tpep_dropoff_datetime": pd.Timestamp("2024-01-01 08:30"),
        "passenger_count": 2.0,
        "trip_distance": 5.0,
        "RatecodeID": 1.0,
        "store_and_fwd_flag": "N",
        "PULocationID": 100,
        "DOLocationID": 200,
        "payment_type": 1,
        "fare_amount": 15.0,
        "extra": 1.0,
        "mta_tax": 0.5,
        "tip_amount": 3.0,
        "tolls_amount": 0.0,
        "improvement_surcharge": 0.3,
        "total_amount": 19.8,
        "congestion_surcharge": 2.5,
        "Airport_fee": None,
        "cbd_congestion_fee": None,
    }
