# import io

# import numpy as np
import pandas as pd

# --- Constants ---
VALID_PAYMENT_TYPES = {1, 2, 3}  # Credit card, Cash, No Charge
VALID_RATE_CODE_IDS = set(
    range(1, 7)
)  # 1= Standard rate 2=JFK 3=Newark 4=Nassau or Westchester 5=Negotiated fare 6=Group ride
VALID_LOCATION_ID_RANGE = (1, 265)
MAX_TRIP_DISTANCE_MILES = 200.0
MAX_FARE_AMOUNT = 500.0
MAX_AVG_SPEED_MPH = 150.0
MAX_PASSENGER_COUNT = 10
NULLABLE_FEE_COLUMNS = {
    "congestion_surcharge",  # added Feb 2019 — NULL in older records
    "Airport_fee",  # added Jul 2022 — NULL in older records
    "cbd_congestion_fee",  # added Jan 2025 — NULL in older records
}
MODERN_LOCATION_COLUMNS = {"PULocationID", "DOLocationID"}
LEGACY_LOCATION_COLUMNS = {"Pickup_longitude", "Pickup_latitude"}


class SchemaNotSupportedError(Exception):
    """Raised when the Parquet file uses a schema this pipeline doesn't support"""

    pass


def validate_schema_format(df: pd.DataFrame) -> None:
    if MODERN_LOCATION_COLUMNS.issubset(df.columns):
        return

    if LEGACY_LOCATION_COLUMNS.issubset(df.columns):
        raise SchemaNotSupportedError(
            "Pre-2016 lat/lng schema is not supported. "
            "File contains Pickup_longitude/Pickup_latitude instead of zone IDs."
        )

    raise SchemaNotSupportedError(
        f"Unrecognized schema. Expected columns {MODERN_LOCATION_COLUMNS}, "
        f"got {set(df.columns)}"
    )


def validate_and_clean(df: pd.DataFrame) -> pd.DataFrame:
    lo, hi = VALID_LOCATION_ID_RANGE

    valid = (
        df["tpep_pickup_datetime"].notna()
        & df["tpep_dropoff_datetime"].notna()
        & (df["tpep_pickup_datetime"] < df["tpep_dropoff_datetime"])
        & df["trip_distance"].notna()
        & df["trip_distance"].between(0, MAX_TRIP_DISTANCE_MILES)
        & df["fare_amount"].notna()
        & df["fare_amount"].between(0, MAX_FARE_AMOUNT)
        & df["total_amount"].notna()
        & df["total_amount"].between(0, MAX_FARE_AMOUNT)
        & df["payment_type"].isin(VALID_PAYMENT_TYPES)
        & df["RatecodeID"].isin(VALID_RATE_CODE_IDS)
        & df["PULocationID"].notna()
        & df["PLULOcationID"].between(lo, hi)
        & df["DOLocationID"].notna()
        & df["DOLocationID"].between(lo, hi)
        & (
            df["passenger_count"].isna()
            | (df["passenger_count"].between(0, MAX_PASSENGER_COUNT))
        )
    )

    cleaned = df[valid].reset_index(drop=True)
    dropped = len(df) - len(cleaned)
    print(
        f"validate_and_clean: dropped {dropped} of {len(df)} records ({dropped / len(df):.1%})"  # noqa: E501
    )

    return cleaned
