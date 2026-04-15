import math
from typing import Any

import pandas as pd
import pytest

from pipeline.transform.transformer import (
    FACT_TRIPS_COLUMNS,
    MAX_FARE_AMOUNT,
    MAX_PASSENGER_COUNT,
    MAX_TRIP_DISTANCE_MILES,
    SchemaNotSupportedError,
    _build_fact_trips,  # pyright: ignore[reportPrivateUsage]
    _enrich_fields,  # pyright: ignore[reportPrivateUsage]
    _validate_and_clean,  # pyright: ignore[reportPrivateUsage]
    _validate_schema_format,  # pyright: ignore[reportPrivateUsage]
    transform,
)


class TestValidateSchemaFormat:
    def test_modern_schema_passes(self) -> None:
        df = pd.DataFrame(columns=["PULocationID", "DOLocationID"])
        _validate_schema_format(df)

    def test_legacy_schema_raises(self) -> None:
        df = pd.DataFrame(
            columns=[
                "Pickup_longitude",
                "Pickup_latitude",
            ]
        )
        with pytest.raises(SchemaNotSupportedError, match="Pre-2016"):
            _validate_schema_format(df)

    def test_unsupported_schema_raises(self) -> None:
        df = pd.DataFrame(columns=["col_a", "col_b"])
        with pytest.raises(SchemaNotSupportedError, match="Unrecognized schema"):
            _validate_schema_format(df)


class TestValidateAndClean:
    def test_valid_row_survives(self, valid_row: dict[str, Any]) -> None:
        df = pd.DataFrame([valid_row])
        result = _validate_and_clean(df)
        assert len(result) == 1

    def test_null_pickup_datetime_dropped(self, valid_row: dict[str, Any]) -> None:
        valid_row["tpep_pickup_datetime"] = None
        df = pd.DataFrame([valid_row])
        result = _validate_and_clean(df)
        assert len(result) == 0

    def test_null_dropoff_datetime_dropped(self, valid_row: dict[str, Any]) -> None:
        valid_row["tpep_dropoff_datetime"] = None
        df = pd.DataFrame([valid_row])
        result = _validate_and_clean(df)
        assert len(result) == 0

    def test_pickup_after_dropoff_dropped(self, valid_row: dict[str, Any]) -> None:
        valid_row["tpep_pickup_datetime"] = pd.Timestamp("2021-01-01 10:00:00")
        valid_row["tpep_dropoff_datetime"] = pd.Timestamp("2021-01-01 9:00:00")
        df = pd.DataFrame([valid_row])
        result = _validate_and_clean(df)
        assert len(result) == 0

    def test_trip_distance_dropped(self, valid_row: dict[str, Any]) -> None:
        valid_row["trip_distance"] = None
        df = pd.DataFrame([valid_row])
        result = _validate_and_clean(df)
        assert len(result) == 0

    def test_trip_distance_negative_dropped(self, valid_row: dict[str, Any]) -> None:
        valid_row["trip_distance"] = -1.0
        df = pd.DataFrame([valid_row])
        result = _validate_and_clean(df)
        assert len(result) == 0

    def test_trip_distance_exceeds_max_dropped(self, valid_row: dict[str, Any]) -> None:
        valid_row["trip_distance"] = MAX_TRIP_DISTANCE_MILES + 1
        df = pd.DataFrame([valid_row])
        result = _validate_and_clean(df)
        assert len(result) == 0

    def test_fare_amount_dropped(self, valid_row: dict[str, Any]) -> None:
        valid_row["fare_amount"] = None
        df = pd.DataFrame([valid_row])
        result = _validate_and_clean(df)
        assert len(result) == 0

    def test_fare_amount_negative_dropped(self, valid_row: dict[str, Any]) -> None:
        valid_row["fare_amount"] = -1.0
        df = pd.DataFrame([valid_row])
        result = _validate_and_clean(df)
        assert len(result) == 0

    def test_fare_amount_exceeds_max_dropped(self, valid_row: dict[str, Any]) -> None:
        valid_row["fare_amount"] = MAX_FARE_AMOUNT + 1
        df = pd.DataFrame([valid_row])
        result = _validate_and_clean(df)
        assert len(result) == 0

    def test_total_amount_dropped(self, valid_row: dict[str, Any]) -> None:
        valid_row["total_amount"] = None
        df = pd.DataFrame([valid_row])
        result = _validate_and_clean(df)
        assert len(result) == 0

    def test_total_amount_negative_dropped(self, valid_row: dict[str, Any]) -> None:
        valid_row["total_amount"] = -1.0
        df = pd.DataFrame([valid_row])
        result = _validate_and_clean(df)
        assert len(result) == 0

    def test_total_amount_exceeds_max_dropped(self, valid_row: dict[str, Any]) -> None:
        valid_row["total_amount"] = MAX_FARE_AMOUNT + 1
        df = pd.DataFrame([valid_row])
        result = _validate_and_clean(df)
        assert len(result) == 0

    def test_payment_type_dropped(self, valid_row: dict[str, Any]) -> None:
        valid_row["payment_type"] = 100
        df = pd.DataFrame([valid_row])
        result = _validate_and_clean(df)
        assert len(result) == 0

    def test_rate_code_too_low_dropped(self, valid_row: dict[str, Any]) -> None:
        valid_row["RatecodeID"] = 0
        df = pd.DataFrame([valid_row])
        result = _validate_and_clean(df)
        assert len(result) == 0

    def test_rate_code_too_high_dropped(self, valid_row: dict[str, Any]) -> None:
        valid_row["RatecodeID"] = 100
        df = pd.DataFrame([valid_row])
        result = _validate_and_clean(df)
        assert len(result) == 0

    def test_pu_location_id_dropped(self, valid_row: dict[str, Any]) -> None:
        valid_row["PULocationID"] = None
        df = pd.DataFrame([valid_row])
        result = _validate_and_clean(df)
        assert len(result) == 0

    def test_pu_location_id_out_of_range_dropped(
        self, valid_row: dict[str, Any]
    ) -> None:
        valid_row["PULocationID"] = 0
        df = pd.DataFrame([valid_row])
        result = _validate_and_clean(df)
        assert len(result) == 0

    def test_do_location_id_dropped(self, valid_row: dict[str, Any]) -> None:
        valid_row["DOLocationID"] = None
        df = pd.DataFrame([valid_row])
        result = _validate_and_clean(df)
        assert len(result) == 0

    def test_do_location_id_out_of_range_dropped(
        self, valid_row: dict[str, Any]
    ) -> None:
        valid_row["DOLocationID"] = 0
        df = pd.DataFrame([valid_row])
        result = _validate_and_clean(df)
        assert len(result) == 0

    def test_passenger_count_negative_dropped(self, valid_row: dict[str, Any]) -> None:
        valid_row["passenger_count"] = -1
        df = pd.DataFrame([valid_row])
        result = _validate_and_clean(df)
        assert len(result) == 0

    def test_passenger_count_exceeds_max_dropped(
        self, valid_row: dict[str, Any]
    ) -> None:
        valid_row["passenger_count"] = MAX_PASSENGER_COUNT + 1
        df = pd.DataFrame([valid_row])
        result = _validate_and_clean(df)
        assert len(result) == 0

    def test_passenger_count_null_survives(self, valid_row: dict[str, Any]) -> None:
        valid_row["passenger_count"] = None
        df = pd.DataFrame([valid_row])
        result = _validate_and_clean(df)
        assert len(result) == 1


class TestEnrichFields:
    def test_trip_duration_minutes_computed(self, valid_row: dict[str, Any]) -> None:
        valid_row["tpep_pickup_datetime"] = pd.Timestamp("2021-01-01 10:00:00")
        valid_row["tpep_dropoff_datetime"] = pd.Timestamp("2021-01-01 10:30:00")
        df = pd.DataFrame([valid_row])
        result = _enrich_fields(df)
        assert result["trip_duration_min"].iloc[0] == 30.0

    def test_revenue_per_mile_computed(self, valid_row: dict[str, Any]) -> None:
        valid_row["total_amount"] = 24.0
        valid_row["trip_distance"] = 10.0
        df = pd.DataFrame([valid_row])
        result = _enrich_fields(df)
        assert result["revenue_per_mile"].iloc[0] == 2.4

    def test_revenue_per_mile_is_nan_when_distance_zero(
        self, valid_row: dict[str, Any]
    ) -> None:
        valid_row["trip_distance"] = 0.0
        df = pd.DataFrame([valid_row])
        result = _enrich_fields(df)
        assert math.isnan(result["revenue_per_mile"].iloc[0])

    def test_avg_speed_mph_computed(self, valid_row: dict[str, Any]) -> None:
        valid_row["trip_distance"] = 30.0
        valid_row["tpep_pickup_datetime"] = pd.Timestamp("2021-01-01 10:00:00")
        valid_row["tpep_dropoff_datetime"] = pd.Timestamp("2021-01-01 11:30:00")
        df = pd.DataFrame([valid_row])
        result = _enrich_fields(df)
        assert result["avg_speed_mph"].iloc[0] == 20.0

    def test_avg_speed_exceeds_max_dropped(self, valid_row: dict[str, Any]) -> None:
        valid_row["trip_distance"] = 100.0
        valid_row["tpep_pickup_datetime"] = pd.Timestamp("2021-01-01 10:00:00")
        valid_row["tpep_dropoff_datetime"] = pd.Timestamp("2021-01-01 10:05:00")
        df = pd.DataFrame([valid_row])
        result = _enrich_fields(df)
        assert len(result) == 0


class TestBuildFactTrips:
    def test_columns_renamed(self, valid_row: dict[str, Any]) -> None:
        df = pd.DataFrame([valid_row])
        enriched = _enrich_fields(df)
        result = _build_fact_trips(enriched)
        assert list(result.columns) == list(FACT_TRIPS_COLUMNS.values())

    def test_correct_number_of_columns(self, valid_row: dict[str, Any]) -> None:
        df = pd.DataFrame([valid_row])
        enriched = _enrich_fields(df)
        result = _build_fact_trips(enriched)
        assert len(result.columns) == len(FACT_TRIPS_COLUMNS)

    def test_missing_nullable_fee_column_filled_with_nan(
        self, valid_row: dict[str, Any]
    ) -> None:
        del valid_row["cbd_congestion_fee"]
        df = pd.DataFrame([valid_row])
        enriched = _enrich_fields(df)
        result = _build_fact_trips(enriched)
        assert "cbd_congestion_fee" in result.columns
        assert math.isnan(result["cbd_congestion_fee"].iloc[0])


class TestTransform:
    def test_return_dataframe(self, valid_row: dict[str, Any]) -> None:
        df = pd.DataFrame([valid_row])
        result = transform(df)
        assert isinstance(result, pd.DataFrame)

    def test_columns_renamed(self, valid_row: dict[str, Any]) -> None:
        df = pd.DataFrame([valid_row])
        result = transform(df)
        assert list(result.columns) == list(FACT_TRIPS_COLUMNS.values())

    def test_invalid_rows_dropped(self, raw_df: pd.DataFrame) -> None:
        result = transform(raw_df)
        # fixture has 12 rows, 9 invalid → expect 3 valid
        assert len(result) == 3

    def test_raises_on_legacy_schema(self) -> None:
        df = pd.DataFrame(columns=["Pickup_longitude", "Pickup_latitude"])
        with pytest.raises(SchemaNotSupportedError, match="Pre-2016 "):
            transform(df)
