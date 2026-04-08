from pipeline.extract.tlc_client import download_monthly_data
from pipeline.land.s3_client import upload_to_s3

year, month = 2024, 1

print(f"Downloading {year}-{month:02d}...")
data = download_monthly_data(year, month)
print(f"Downloaded {len(data):,} bytes.")

print("Landing to S3...")
landed_url = upload_to_s3(year, month, data)
print(f"Data landed at: {landed_url}")
