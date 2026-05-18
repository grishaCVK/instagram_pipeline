import os

from dotenv import load_dotenv

load_dotenv()


def get_bool_env(name: str, default: bool = False) -> bool:
    value = os.getenv(name)

    if value is None:
        return default

    return value.lower() in ("1", "true", "yes", "y", "on")


META_ACCESS_TOKEN = os.getenv("META_ACCESS_TOKEN")
GRAPH_API_VERSION = os.getenv("GRAPH_API_VERSION", "v25.0")
AD_ACCOUNT_ID = os.getenv("AD_ACCOUNT_ID")
INSTAGRAM_ACCOUNT_ID = os.getenv("INSTAGRAM_ACCOUNT_ID")

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DB", "instagram_ads")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "instagram_embeddings")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")

BACKFILL_MODE = get_bool_env("BACKFILL_MODE", False)
BACKFILL_START_DATE = os.getenv("BACKFILL_START_DATE") or None
BACKFILL_END_DATE = os.getenv("BACKFILL_END_DATE") or None
BACKFILL_BATCH_DAYS = int(
    os.getenv("BACKFILL_BATCH_DAYS", "3")
)


def validate_config():
    required = {
        "META_ACCESS_TOKEN": META_ACCESS_TOKEN,
        "GRAPH_API_VERSION": GRAPH_API_VERSION,
        "AD_ACCOUNT_ID": AD_ACCOUNT_ID,
        "CLICKHOUSE_HOST": CLICKHOUSE_HOST,
        "POSTGRES_HOST": POSTGRES_HOST,
    }

    missing = [name for name, value in required.items() if not value]

    if missing:
        raise ValueError(
            f"Missing required env variables: {', '.join(missing)}"
        )

    if BACKFILL_MODE and not BACKFILL_START_DATE:
        raise ValueError(
            "BACKFILL_START_DATE is required when BACKFILL_MODE=true"
        )

    if BACKFILL_BATCH_DAYS <= 0:
        raise ValueError("BACKFILL_BATCH_DAYS must be greater than 0")
