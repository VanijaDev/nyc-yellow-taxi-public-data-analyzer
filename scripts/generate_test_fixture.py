from pathlib import Path
from typing import Any

import pandas as pd

OUTPUT_PATH = Path("tests/transform/fixtures/sample_trips.parquet")

VALID_ROW: dict[str, Any] = {
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

rows: list[dict[str, Any]] = [
    # Row 0 - valid
    {**VALID_ROW},
    # Row 1 - invalid, pickup after dropoff
    {
        **VALID_ROW,
        "tpep_pickup_datetime": pd.Timestamp("2024-01-01 09:00"),
        "tpep_dropoff_datetime": pd.Timestamp("2024-01-01 08:30"),
    },
    # Row 2 — invalid: trip_distance negative
    {**VALID_ROW, "trip_distance": -1.0},
    # Row 3 — invalid: trip_distance exceeds max (> 200)
    {**VALID_ROW, "trip_distance": 201.0},
    # Row 4 — invalid: fare_amount negative
    {**VALID_ROW, "fare_amount": -5.0},
    # Row 5 — invalid: payment_type disputed (4)
    {**VALID_ROW, "payment_type": 4},
    # Row 6 — invalid: PULocationID out of range
    {**VALID_ROW, "PULocationID": 0},
    # Row 7 — invalid: DOLocationID out of range
    {**VALID_ROW, "DOLocationID": 266},
    # Row 8 — invalid: passenger_count exceeds max
    {**VALID_ROW, "passenger_count": 11.0},
    # Row 9 — valid: null passenger_count is allowed
    {**VALID_ROW, "passenger_count": None},
    # Row 10 — valid: zero trip_distance (revenue_per_mile should be NaN)
    {**VALID_ROW, "trip_distance": 0.0},
    # Row 11 — invalid: high speed (will be filtered in _enrich_fields)
    {
        **VALID_ROW,
        "trip_distance": 200.0,
        "tpep_pickup_datetime": pd.Timestamp("2024-01-01 08:00"),
        "tpep_dropoff_datetime": pd.Timestamp("2024-01-01 08:01"),
    },
]

OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
df = pd.DataFrame(rows)
df.to_parquet(OUTPUT_PATH, index=False)
print(f"Fixture written to {OUTPUT_PATH} with {len(df)} rows.")
