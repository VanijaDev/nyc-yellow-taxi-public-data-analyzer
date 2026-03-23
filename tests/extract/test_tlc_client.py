import datetime
import pytest
from pipeline.extract.tlc_client import _build_url # pyright: ignore[reportPrivateUsage]

class TestBuildUrl:
  def test_build_url_returns_correct_url(self):
    expected_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-12.parquet"
    assert _build_url(2025, 12) == expected_url

  def test_build_url_pads_month_with_leading_zero(self):
    expected_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet"
    assert _build_url(2025, 1) == expected_url

  def test_build_url_fails_on_future_year(self):
    current_year = datetime.date.today().year
    year = current_year + 1
    with pytest.raises(ValueError, match = f"Year cannot be in the future, today is {current_year} got {year}."):
      _build_url(year, 5)

  def test_build_url_fails_on_invalid_month(self):
    with pytest.raises(ValueError, match="Month must be between 1 and 12, got 0."):
      _build_url(2023, 0)
    
    with pytest.raises(ValueError, match="Month must be between 1 and 12, got 13."):
      _build_url(2023, 13)
