import io

import pandas as pd

from pipeline.extract.tlc_client import download_monthly_data
from pipeline.transform.transformer import transform

print("Downloading Jan 2024 data...")
data = download_monthly_data(2024, 1)
print(f"Downloaded {len(data):,} bytes")

df = pd.read_parquet(io.BytesIO(data))
print(f"\nRaw shape: {df.shape}")
print(f"\nColumn types:\n{df.dtypes}")

print("\n Running transformer...")
result = transform(df)

print(f"\nResult shape: {result.shape}")
print(f"\nColumns:\n{result.columns.tolist()}")
print(f"\nSample rows:\n{result.head(3)}")

print("\nDerived fields summary:")
print(result[["trip_duration_min", "avg_speed_mph", "revenue_per_mile"]].describe())
