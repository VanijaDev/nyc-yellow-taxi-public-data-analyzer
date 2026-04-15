import os

from dotenv import load_dotenv

load_dotenv()


def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if value is None:
        raise ValueError(
            f"Missing required environment variable: {name}. Set it in .env."
        )
    return value


# AWS
AWS_ACCESS_KEY_ID: str = _require_env("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY: str = _require_env("AWS_SECRET_ACCESS_KEY")
AWS_REGION: str = _require_env("AWS_REGION")
S3_BUCKET_RAW: str = _require_env("S3_BUCKET_RAW")

# Snowflake
SNOWFLAKE_ACCOUNT: str = _require_env("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER: str = _require_env("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD: str = _require_env("SNOWFLAKE_PASSWORD")
SNOWFLAKE_DATABASE: str = _require_env("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA: str = _require_env("SNOWFLAKE_SCHEMA")
SNOWFLAKE_WAREHOUSE: str = _require_env("SNOWFLAKE_WAREHOUSE")
