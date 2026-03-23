import datetime
import pytest
from unittest.mock import patch, MagicMock
from pipeline.extract.tlc_client import _build_url, download_monthly_data, MonthNotAvailableError # pyright: ignore[reportPrivateUsage]


class TestBuildUrl:
  def test_build_url_returns_correct_url(self):
    expected_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-12.parquet"
    assert _build_url(2025, 12) == expected_url

  def test_build_url_pads_month_with_leading_zero(self):
    expected_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet"
    assert _build_url(2025, 1) == expected_url

  def test_build_url_raises_on_future_year(self):
    current_year = datetime.date.today().year
    year = current_year + 1
    with pytest.raises(ValueError, match = f"Year cannot be in the future, today is {current_year} got {year}."):
      _build_url(year, 5)

  def test_build_url_raises_on_invalid_month(self):
    with pytest.raises(ValueError, match="Month must be between 1 and 12, got 0."):
      _build_url(2023, 0)
    
    with pytest.raises(ValueError, match="Month must be between 1 and 12, got 13."):
      _build_url(2023, 13)

class TestDownloadMonthlyData:
  @patch("httpx.stream")
  def test_returns_bytes_on_success(self, mock_stream: MagicMock):
    mock_response = MagicMock()
    mock_stream.return_value.__enter__.return_value = mock_response # pyright: ignore[reportUnknownMemberType]
    mock_response.status_code = 200
    mock_response.read.return_value = b"test data"

    assert download_monthly_data(2025, 5) == b"test data"

  @patch("httpx.stream")
  def test_raises_month_not_available_error_on_403(self, mock_stream:MagicMock):
    mock_response = MagicMock()
    mock_stream.return_value.__enter__.return_value = mock_response # pyright: ignore[reportUnknownMemberType]
    mock_response.status_code = 403

    with pytest.raises(MonthNotAvailableError, match="Data for 2025-05 not found."):
      download_monthly_data(2025, 5)

  def test_raises_month_not_available_error_on_404(self):
    pass

  def test_raises_http_status_error_on_500(self):
    pass

  def test_raises_on_network_error(self):
    pass