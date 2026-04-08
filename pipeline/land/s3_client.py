import boto3
from botocore.exceptions import ClientError
from mypy_boto3_s3 import S3Client

from pipeline.utils.config import (
    AWS_ACCESS_KEY_ID,
    AWS_REGION,
    AWS_SECRET_ACCESS_KEY,
    S3_BUCKET_RAW,
)


def upload_to_s3(year: int, month: int, data: bytes) -> str:
    key = f"{year}/{month:02d}/yellow_tripdata_{year}-{month:02d}.parquet"
    s3_client = _boto3_client()

    try:
        s3_client.put_object(Bucket=S3_BUCKET_RAW, Key=key, Body=data)
        return f"s3://{S3_BUCKET_RAW}/{key}"
    except ClientError as e:
        raise RuntimeError(f"Failed to upload to S3: {e}")


def _boto3_client() -> S3Client:
    return boto3.client(  # pyright: ignore[reportUnknownMemberType]
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION,
    )
