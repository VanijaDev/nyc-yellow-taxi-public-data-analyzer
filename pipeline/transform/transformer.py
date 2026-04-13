import pandas as pd
import structlog

logger = structlog.get_logger()

# --- Constants ---
VALID_PAYMENT_TYPES = {1, 2, 3}  # Credit card, Cash, No Charge
VALID_RATE_CODE_IDS = set(
    range(1, 7)
)  # 1= Standard rate 2=JFK 3=Newark 4=Nassau or Westchester 5=Negotiated fare 6=Group ride  # noqa: E501
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


def _validate_schema_format(df: pd.DataFrame) -> None:
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


def _validate_and_clean(df: pd.DataFrame) -> pd.DataFrame:
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
        & df["PULocationID"].between(lo, hi)
        & df["DOLocationID"].notna()
        & df["DOLocationID"].between(lo, hi)
        & (
            df["passenger_count"].isna()
            | (df["passenger_count"].between(0, MAX_PASSENGER_COUNT))
        )
    )

    cleaned = df[valid].reset_index(drop=True)
    dropped = len(df) - len(cleaned)
    logger.info("validate_and_clean", dropped=f"{dropped:,}", total=f"{len(df):,}")

    return cleaned


def _enrich_fields(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df["trip_duration_min"] = (
        df["tpep_dropoff_datetime"] - df["tpep_pickup_datetime"]
    ).dt.total_seconds() / 60

    df["revenue_per_mile"] = df["total_amount"] / df["trip_distance"].replace(
        0, float("nan")
    )

    df["avg_speed_mph"] = df["trip_distance"] / (
        df["trip_duration_min"].replace(0, float("nan")) / 60
    )

    df = df[df["avg_speed_mph"].isna() | (df["avg_speed_mph"] <= MAX_AVG_SPEED_MPH)]

    return df.reset_index(drop=True)


FACT_TRIPS_COLUMNS = {
    "PULocationID": "pickup_location_id",
    "DOLocationID": "dropoff_location_id",
    "tpep_pickup_datetime": "pickup_datetime",
    "tpep_dropoff_datetime": "dropoff_datetime",
    "passenger_count": "passenger_count",
    "trip_distance": "trip_distance",
    "fare_amount": "fare_amount",
    "tip_amount": "tip_amount",
    "total_amount": "total_amount",
    "payment_type": "payment_type",
    "RatecodeID": "rate_code_id",
    "VendorID": "vendor_id",
    "trip_duration_min": "trip_duration_min",
    "revenue_per_mile": "revenue_per_mile",
    "avg_speed_mph": "avg_speed_mph",
    "congestion_surcharge": "congestion_surcharge",
    "Airport_fee": "airport_fee",
    "cbd_congestion_fee": "cbd_congestion_fee",
}


def _build_fact_trips(df: pd.DataFrame) -> pd.DataFrame:
    return df.reindex(columns=list(FACT_TRIPS_COLUMNS.keys())).rename(
        columns=FACT_TRIPS_COLUMNS
    )


def transform(df: pd.DataFrame) -> pd.DataFrame:
    _validate_schema_format(df)
    cleaned = _validate_and_clean(df)
    enriched = _enrich_fields(cleaned)
    fact_trips = _build_fact_trips(enriched)

    return fact_trips
