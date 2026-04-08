from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError

from pipeline.land.s3_client import upload_to_s3


class TestUploadToS3:
    @patch("pipeline.land.s3_client.boto3.client")
    def test_upload_constructs_correct_s3_key(self, mock_boto3_client: MagicMock):
        mock_s3 = MagicMock()
        mock_boto3_client.return_value = mock_s3

        upload_to_s3(2025, 5, b"text data")
        mock_s3.put_object.assert_called_once_with(
            Bucket=mock_s3.put_object.call_args.kwargs["Bucket"],
            Key="2025/05/yellow_tripdata_2025-05.parquet",
            Body=b"text data",
        )

    @patch("pipeline.land.s3_client.boto3.client")
    def test_upload_calls_put_object_with_correct_args(
        self, mock_boto3_client: MagicMock
    ):
        mock_s3 = MagicMock()
        mock_boto3_client.return_value = mock_s3

        upload_to_s3(2025, 5, b"text data")
        mock_s3.put_object.assert_called_once_with(
            Bucket="test-bucket",
            Key="2025/05/yellow_tripdata_2025-05.parquet",
            Body=b"text data",
        )

    @patch("pipeline.land.s3_client.boto3.client")
    def test_upload_returns_correct_s3_url(self, mock_s3_client: MagicMock):
        mock_s3 = MagicMock()
        mock_s3_client.return_value = mock_s3

        result = upload_to_s3(2025, 5, b"text data")
        assert result == "s3://test-bucket/2025/05/yellow_tripdata_2025-05.parquet"

    @patch("pipeline.land.s3_client.boto3.client")
    def test_upload_raises_runtime_error_on_client_error(
        self, mock_s3_client: MagicMock
    ):
        mock_s3 = MagicMock()
        mock_s3_client.return_value = mock_s3
        mock_s3.put_object.side_effect = ClientError(
            {"Error": {"Code": "500", "Message": "The bucket does not exist"}},
            "PutObject",
        )

        with pytest.raises(RuntimeError, match="Failed to upload to S3"):
            upload_to_s3(2025, 5, b"text data")
