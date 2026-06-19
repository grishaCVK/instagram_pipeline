"""
main.py

Оркестратор ETL пайплайна Instagram Ads.

Step 1 (fetch_raw):    Meta Graph API → instagram_raw
Step 2 (raw_to_staging): instagram_raw → instagram_staging
Step 3 (staging_to_core): instagram_staging → instagram_core
"""

from datetime import date, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import clickhouse_db
import config
import etl_logger as instagram_etl_logger
import graph_api
import instagram_core_loader
import instagram_staging_loader
import embeddings


ALMATY_TZ = ZoneInfo("Asia/Almaty")


# ------------------------------------------------------------
# Raw data saver
# ------------------------------------------------------------

def save_raw(
    source: str,
    response_data: dict[str, Any],
    date_since: str,
    date_until: str,
    extra_params: dict[str, Any] | None = None,
) -> None:
    base_params: dict[str, Any] = {
        "date_since": date_since,
        "date_until": date_until,
        "level": "ad",
        "time_increment": 1,
        "limit": 100,
    }
    if extra_params:
        base_params.update(extra_params)
    clickhouse_db.insert_raw_data(
        source=source,
        api_type="ads_insights",
        endpoint=f"/{config.AD_ACCOUNT_ID}/insights",
        object_id=config.AD_ACCOUNT_ID,
        response_data=response_data,
        request_params=base_params,
    )


# ------------------------------------------------------------
# Date batching
# ------------------------------------------------------------

def iter_date_batches(
    start_date: str,
    end_date: str,
    batch_days: int,
) -> list[tuple[str, str]]:
    start = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date)
    batches: list[tuple[str, str]] = []
    current = start
    while current <= end:
        batch_end = min(
            current + timedelta(days=batch_days - 1),
            end,
        )
        batches.append(
            (current.isoformat(), batch_end.isoformat())
        )
        current = batch_end + timedelta(days=1)
    return batches


def get_yesterday() -> str:
    return (date.today() - timedelta(days=1)).isoformat()


# ------------------------------------------------------------
# Step 1: API → raw
# ------------------------------------------------------------

def run_api_to_raw(
    *,
    run_id: str,
    date_since: str,
    date_until: str,
) -> int:
    with instagram_etl_logger.etl_step(
        run_id=run_id,
        step_name="fetch_raw",
        step_order=1,
        target_database=config.CLICKHOUSE_RAW_DB,
    ) as step:
        hourly_resp = graph_api.get_ads_insights(
            date_since=date_since,
            date_until=date_until,
        )
        daily_resp = graph_api.get_ads_insights_daily(
            date_since=date_since,
            date_until=date_until,
        )
        geo_resp = graph_api.get_ads_insights_geo_daily(
            date_since=date_since,
            date_until=date_until,
        )
        device_resp = (
            graph_api.get_ads_insights_device_daily(
                date_since=date_since,
                date_until=date_until,
            )
        )
        gender_resp = (
            graph_api.get_ads_insights_gender_daily(
                date_since=date_since,
                date_until=date_until,
            )
        )

        save_raw(
            "ads_insights_hourly",
            hourly_resp,
            date_since,
            date_until,
            {"breakdowns": graph_api.HOURLY_BREAKDOWN_FIELD},
        )
        save_raw(
            "ads_insights_daily",
            daily_resp,
            date_since,
            date_until,
        )
        save_raw(
            "ads_insights_geo",
            geo_resp,
            date_since,
            date_until,
            {"breakdowns": "country,region"},
        )
        save_raw(
            "ads_insights_device",
            device_resp,
            date_since,
            date_until,
            {
                "breakdowns": (
                    "device_platform,impression_device"
                )
            },
        )
        save_raw(
            "ads_insights_gender",
            gender_resp,
            date_since,
            date_until,
            {"breakdowns": "age,gender"},
        )

        raw_count = (
            len(hourly_resp.get("data", []))
            + len(daily_resp.get("data", []))
            + len(geo_resp.get("data", []))
            + len(device_resp.get("data", []))
            + len(gender_resp.get("data", []))
        )
        step["input_rows"] = raw_count
        step["output_rows"] = raw_count

    return raw_count


# ------------------------------------------------------------
# Step 4: staging → image embeddings (pgvector)
# ------------------------------------------------------------

def run_embeddings(
    *,
    run_id: str,
    date_since: str,
    date_until: str,
) -> int:
    with instagram_etl_logger.etl_step(
        run_id=run_id,
        step_name="embeddings",
        step_order=4,
        target_database=config.POSTGRES_DB,
    ) as step:
        ads = clickhouse_db.read_staging_unique_ads(
            date_since=date_since,
            date_until=date_until,
        )
        step["input_rows"] = len(ads)
        print(f"[EMB] unique ads for embeddings: {len(ads)}")

        embeddings.process_ads_insights_image_embeddings(ads)

        step["output_rows"] = len(ads)
    return len(ads)


# ------------------------------------------------------------
# Pipeline for one period
# ------------------------------------------------------------

def run_pipeline_for_period(
    date_since: str,
    date_until: str,
    run_id: str,
) -> tuple[int, int]:
    print(f"Start period: {date_since} -> {date_until}")

    total_raw = run_api_to_raw(
        run_id=run_id,
        date_since=date_since,
        date_until=date_until,
    )
    total_staging = instagram_staging_loader.run_raw_to_staging(
        run_id=run_id,
        date_since=date_since,
        date_until=date_until,
    )

    print(f"Finished period: {date_since} -> {date_until}")
    return total_raw, total_staging


# ------------------------------------------------------------
# Entry point
# ------------------------------------------------------------

def main() -> None:
    config.validate_config()

    if config.BACKFILL_MODE:
        date_since = config.BACKFILL_START_DATE
        date_until = (
            config.BACKFILL_END_DATE
            or datetime.now(ALMATY_TZ).date().isoformat()
        )
        batch_days = config.BACKFILL_BATCH_DAYS
        run_type = "backfill"
    else:
        date_since = get_yesterday()
        date_until = get_yesterday()
        batch_days = 1
        run_type = "daily"

    batches = iter_date_batches(
        start_date=date_since,
        end_date=date_until,
        batch_days=batch_days,
    )

    print(
        f"Instagram {run_type} started: "
        f"{date_since} -> {date_until}, "
        f"batches={len(batches)}"
    )

    total_raw = 0
    total_staging = 0

    run_id = instagram_etl_logger.create_run(
        run_type=run_type,
        date_since=date_since,
        date_until=date_until,
    )
    started_at = datetime.now(ALMATY_TZ)

    try:
        for batch_since, batch_until in batches:
            raw, staging = run_pipeline_for_period(
                date_since=batch_since,
                date_until=batch_until,
                run_id=run_id,
            )
            total_raw += raw
            total_staging += staging

        total_core = instagram_core_loader.run_staging_to_core(
            run_id=run_id,
            date_since=date_since,
            date_until=date_until,
        )

        # Embeddings — best-effort: сбой здесь не должен
        # ронять уже загруженный core. Шаг логируется как
        # failed, но сам run остаётся success.
        try:
            run_embeddings(
                run_id=run_id,
                date_since=date_since,
                date_until=date_until,
            )
        except Exception as emb_error:
            print(
                f"[WARN] embeddings step failed, "
                f"run continues: {emb_error}"
            )

        instagram_etl_logger.finish_run(
            run_id=run_id,
            started_at=started_at,
            status="success",
            total_raw_rows=total_raw,
            total_staging_rows=total_staging,
            total_core_rows=total_core,
            actual_min_date=date_since,
            actual_max_date=date_until,
        )

    except Exception as e:
        import traceback
        instagram_etl_logger.finish_run(
            run_id=run_id,
            started_at=started_at,
            status="failed",
            total_raw_rows=total_raw,
            total_staging_rows=total_staging,
            error_message=str(e),
            error_trace=traceback.format_exc(),
        )
        raise

    print(f"Instagram {run_type} finished")


if __name__ == "__main__":
    main()
