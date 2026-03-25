import datetime

import httpx

TLC_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
TLC_REQUEST_TIMEOUT = httpx.Timeout(30.0, connect=10.0, read=30.0)


class MonthNotAvailableError(Exception):
    """Custom exception raised when a month is not found in the dataset."""

    def __init__(self, year: int, month: int):
        self.year = year
        self.month = month
        super().__init__(f"Data for {year}-{month:02d} not found.")


def _build_url(year: int, month: int) -> str:
    """Build the URL for the TLC trip data file for a given year and month."""
    current_year = datetime.date.today().year
    if year > current_year:
        raise ValueError(
            f"Year cannot be in the future, today is {current_year} got {year}."
        )

    if not 1 <= month <= 12:
        raise ValueError(f"Month must be between 1 and 12, got {month}.")

    return f"{TLC_BASE_URL}/yellow_tripdata_{year}-{month:02d}.parquet"


def download_monthly_data(year: int, month: int) -> bytes:
    """Download the TLC trip data for a specific year and month.

    Args:
        year (int): The year of the data to download.
        month (int): The month of the data to download.

    Returns:
        bytes: The content of the downloaded file.

    Raises:
        MonthNotAvailableError: If the data for the specified month is not available.
        HTTPStatusError: If an HTTP error occurs during the request.
    """

    url = _build_url(year, month)

    with httpx.stream("GET", url, timeout=TLC_REQUEST_TIMEOUT) as response:
        if response.status_code in (403, 404):
            raise MonthNotAvailableError(year, month)
        response.raise_for_status()

        return response.read()


if __name__ == "__main__":
    # Example usage
    try:
        data = download_monthly_data(2024, 12)
        print(f"Downloaded {len(data)} bytes of data.")
    except MonthNotAvailableError as e:
        print(f"No data: {e}")
    except httpx.HTTPStatusError as e:
        print(f"HTTP error occurred: {e}")
