import json
import uuid
from datetime import datetime
from typing import Any

import clickhouse_connect

import config


def get_client():
    return clickhouse_connect.get_client(
        host=config.CLICKHOUSE_HOST,
        port=config.CLICKHOUSE_PORT,
        username=config.CLICKHOUSE_USER,
        password=config.CLICKHOUSE_PASSWORD,
        database=config.CLICKHOUSE_DB,
    )


def insert_raw_data(
    source: str,
    api_type: str,
    endpoint: str,
    object_id: str | None,
    response_data: dict,
    request_params: dict,
) -> None:
    client = get_client()

    row = [
        str(uuid.uuid4()),
        source,
        api_type,
        endpoint,
        object_id,
        json.dumps(response_data, ensure_ascii=False),
        datetime.now(),
        json.dumps(request_params, ensure_ascii=False),
    ]

    client.insert(
        "raw_data",
        [row],
        column_names=[
            "raw_id",
            "source",
            "api_type",
            "endpoint",
            "object_id",
            "response_json",
            "fetched_at",
            "request_params",
        ],
    )


def get_latest_raw_data(source: str) -> dict[str, Any] | None:
    client = get_client()

    query = """
        SELECT
            raw_id,
            source,
            api_type,
            endpoint,
            object_id,
            response_json,
            fetched_at,
            request_params
        FROM raw_data
        WHERE source = {source:String}
        ORDER BY fetched_at DESC
        LIMIT 1
    """

    result = client.query(
        query,
        parameters={
            "source": source,
        },
    )

    if not result.result_rows:
        return None

    row = result.result_rows[0]

    return {
        "raw_id": row[0],
        "source": row[1],
        "api_type": row[2],
        "endpoint": row[3],
        "object_id": row[4],
        "response_data": json.loads(row[5]),
        "fetched_at": row[6],
        "request_params": json.loads(row[7]),
    }


def get_latest_ads_insights_data() -> list[dict[str, Any]]:
    latest_raw = get_latest_raw_data(source="ads_insights")

    if latest_raw is None:
        return []

    response_data = latest_raw["response_data"]

    return response_data.get("data", [])


PAID_TABLE_COLUMNS = {
    "paid_ads_awareness_hourly_ad_level": [
        "date_start",
        "date_stop",
        "campaign_id",
        "campaign_name",
        "campaign_status",
        "campaign_effective_status",
        "campaign_start_time",
        "campaign_stop_time",
        "adset_id",
        "adset_name",
        "ad_id",
        "ad_name",
        "destination_url",
        "media_type",
        "media_product_type",
        "children_count",
        "children_json",
        "conversion_location",
        "is_incremental_attribution_enabled",
        "attribution_setting",
        "target_locations_json",
        "age_range",
        "gender",
        "languages_json",
        "placements_json",
        "objective",
        "result_name",
        "result_value",
        "cost_per_result",
        "spend",
        "impressions",
        "reach",
        "frequency",
        "cpm",
        "clicks",
        "inline_link_clicks",
        "inline_link_click_ctr",
        "ctr",
        "cpc",
        "messaging_conversation_started",
        "cost_per_messaging_conversation_started",
        "video_play_actions",
        "video_p25_watched_actions",
        "video_p50_watched_actions",
        "video_p75_watched_actions",
        "video_p100_watched_actions",
        "video_avg_time_watched_actions",
        "daily_budget",
        "lifetime_budget",
        "budget_remaining",
        "loaded_at",
    ],
    "paid_ads_traffic_hourly_ad_level": [
        "date_start",
        "date_stop",
        "campaign_id",
        "campaign_name",
        "campaign_status",
        "campaign_effective_status",
        "campaign_start_time",
        "campaign_stop_time",
        "adset_id",
        "adset_name",
        "ad_id",
        "ad_name",
        "destination_url",
        "media_type",
        "media_product_type",
        "children_count",
        "children_json",
        "conversion_location",
        "is_incremental_attribution_enabled",
        "attribution_setting",
        "target_locations_json",
        "age_range",
        "gender",
        "languages_json",
        "placements_json",
        "objective",
        "result_name",
        "result_value",
        "cost_per_result",
        "spend",
        "impressions",
        "reach",
        "landing_page_view",
        "cost_per_landing_page_view",
        "frequency",
        "cpm",
        "clicks",
        "inline_link_clicks",
        "inline_link_click_ctr",
        "ctr",
        "cpc",
        "messaging_conversation_started",
        "cost_per_messaging_conversation_started",
        "video_play_actions",
        "video_p25_watched_actions",
        "video_p50_watched_actions",
        "video_p75_watched_actions",
        "video_p100_watched_actions",
        "video_avg_time_watched_actions",
        "daily_budget",
        "lifetime_budget",
        "budget_remaining",
        "loaded_at",
    ],
    "paid_ads_engagement_hourly_ad_level": [
        "date_start",
        "date_stop",
        "campaign_id",
        "campaign_name",
        "campaign_status",
        "campaign_effective_status",
        "campaign_start_time",
        "campaign_stop_time",
        "adset_id",
        "adset_name",
        "ad_id",
        "ad_name",
        "destination_url",
        "media_type",
        "media_product_type",
        "children_count",
        "children_json",
        "conversion_location",
        "is_incremental_attribution_enabled",
        "attribution_setting",
        "target_locations_json",
        "age_range",
        "gender",
        "languages_json",
        "placements_json",
        "objective",
        "result_name",
        "result_value",
        "cost_per_result",
        "spend",
        "impressions",
        "reach",
        "messaging_conversation_started",
        "cost_per_messaging_conversation_started",
        "frequency",
        "cpm",
        "clicks",
        "inline_link_clicks",
        "inline_link_click_ctr",
        "ctr",
        "cpc",
        "video_play_actions",
        "video_p25_watched_actions",
        "video_p50_watched_actions",
        "video_p75_watched_actions",
        "video_p100_watched_actions",
        "video_avg_time_watched_actions",
        "daily_budget",
        "lifetime_budget",
        "budget_remaining",
        "comments_count",
        "likes_count",
        "saved",
        "shares",
        "post_engagement",
        "cost_per_post_engagement",
        "loaded_at",
    ],
    "paid_ads_leads_hourly_ad_level": [
        "date_start",
        "date_stop",
        "campaign_id",
        "campaign_name",
        "campaign_status",
        "campaign_effective_status",
        "campaign_start_time",
        "campaign_stop_time",
        "adset_id",
        "adset_name",
        "ad_id",
        "ad_name",
        "destination_url",
        "media_type",
        "media_product_type",
        "children_count",
        "children_json",
        "conversion_location",
        "is_incremental_attribution_enabled",
        "attribution_setting",
        "target_locations_json",
        "age_range",
        "gender",
        "languages_json",
        "placements_json",
        "objective",
        "result_name",
        "result_value",
        "cost_per_result",
        "spend",
        "impressions",
        "reach",
        "messaging_conversation_started",
        "cost_per_messaging_conversation_started",
        "frequency",
        "cpm",
        "clicks",
        "inline_link_clicks",
        "inline_link_click_ctr",
        "ctr",
        "cpc",
        "video_play_actions",
        "video_p25_watched_actions",
        "video_p50_watched_actions",
        "video_p75_watched_actions",
        "video_p100_watched_actions",
        "video_avg_time_watched_actions",
        "daily_budget",
        "lifetime_budget",
        "budget_remaining",
        "profile_visits",
        "leads",
        "cost_per_lead",
        "loaded_at",
    ],
    "paid_ads_app_promotion_hourly_ad_level": [
        "date_start",
        "date_stop",
        "campaign_id",
        "campaign_name",
        "campaign_status",
        "campaign_effective_status",
        "campaign_start_time",
        "campaign_stop_time",
        "adset_id",
        "adset_name",
        "ad_id",
        "ad_name",
        "destination_url",
        "media_type",
        "media_product_type",
        "children_count",
        "children_json",
        "conversion_location",
        "is_incremental_attribution_enabled",
        "attribution_setting",
        "target_locations_json",
        "age_range",
        "gender",
        "languages_json",
        "placements_json",
        "objective",
        "result_name",
        "result_value",
        "cost_per_result",
        "spend",
        "impressions",
        "reach",
        "messaging_conversation_started",
        "cost_per_messaging_conversation_started",
        "frequency",
        "cpm",
        "clicks",
        "inline_link_clicks",
        "inline_link_click_ctr",
        "ctr",
        "cpc",
        "video_play_actions",
        "video_p25_watched_actions",
        "video_p50_watched_actions",
        "video_p75_watched_actions",
        "video_p100_watched_actions",
        "video_avg_time_watched_actions",
        "daily_budget",
        "lifetime_budget",
        "budget_remaining",
        "mobile_app_install",
        "cost_per_mobile_app_install",
        "mobile_app_registration",
        "mobile_app_purchase",
        "loaded_at",
    ],
    "paid_ads_sales_hourly_ad_level": [
        "date_start",
        "date_stop",
        "campaign_id",
        "campaign_name",
        "campaign_status",
        "campaign_effective_status",
        "campaign_start_time",
        "campaign_stop_time",
        "adset_id",
        "adset_name",
        "ad_id",
        "ad_name",
        "destination_url",
        "media_type",
        "media_product_type",
        "children_count",
        "children_json",
        "conversion_location",
        "is_incremental_attribution_enabled",
        "attribution_setting",
        "target_locations_json",
        "age_range",
        "gender",
        "languages_json",
        "placements_json",
        "objective",
        "result_name",
        "result_value",
        "cost_per_result",
        "spend",
        "impressions",
        "reach",
        "messaging_conversation_started",
        "cost_per_messaging_conversation_started",
        "frequency",
        "cpm",
        "clicks",
        "inline_link_clicks",
        "inline_link_click_ctr",
        "ctr",
        "cpc",
        "video_play_actions",
        "video_p25_watched_actions",
        "video_p50_watched_actions",
        "video_p75_watched_actions",
        "video_p100_watched_actions",
        "video_avg_time_watched_actions",
        "daily_budget",
        "lifetime_budget",
        "budget_remaining",
        "purchase",
        "cost_per_purchase",
        "add_to_cart",
        "cost_per_add_to_cart",
        "initiate_checkout",
        "cost_per_initiate_checkout",
        "view_content",
        "cost_per_view_content",
        "loaded_at",
    ],
}

PAID_ADS_GEO_DAILY_TABLE = "paid_ads_geo_daily_level"

PAID_ADS_GEO_DAILY_COLUMNS = [
    "date_start",
    "date_stop",

    "campaign_id",
    "campaign_name",
    "adset_id",
    "adset_name",
    "ad_id",
    "ad_name",
    "objective",

    "country",
    "region",

    "spend",
    "impressions",
    "reach",
    "frequency",
    "cpm",

    "clicks",
    "inline_link_clicks",
    "ctr",

    "loaded_at",
]


PAID_ADS_DEVICE_DAILY_TABLE = "paid_ads_device_daily_level"

PAID_ADS_DEVICE_DAILY_COLUMNS = [
    "date_start",
    "date_stop",

    "campaign_id",
    "campaign_name",
    "adset_id",
    "adset_name",
    "ad_id",
    "ad_name",
    "objective",

    "device_platform",
    "impression_device",

    "device_type",
    "os_type",

    "spend",
    "impressions",
    "reach",
    "frequency",
    "cpm",

    "clicks",
    "inline_link_clicks",
    "ctr",

    "loaded_at",
]


def delete_paid_ads_for_period(
    table_name: str,
    date_since: str,
    date_until: str,
) -> None:
    """
    Удаляет старые строки за период из paid_ads_* таблицы.

    Для hourly-данных удаляем:
    date_since 00:00:00 включительно
    до следующего дня после date_until 00:00:00 не включительно.

    Например:
    date_since = 2026-05-09
    date_until = 2026-05-09

    Удалится:
    2026-05-09 00:00:00
    ...
    2026-05-09 23:00:00
    """
    if table_name not in PAID_TABLE_COLUMNS:
        raise ValueError(f"Unknown paid ads table: {table_name}")

    client = get_client()

    query = f"""
        ALTER TABLE instagram_ads.{table_name}
        DELETE WHERE date_start >= toDateTime(
            {{date_since:String}},
            'Asia/Almaty'
        )
        AND date_start < addDays(
            toDateTime(
                {{date_until:String}},
                'Asia/Almaty'
            ),
            1
        )
    """

    client.command(
        query,
        parameters={
            "date_since": date_since,
            "date_until": date_until,
        },
        settings={"mutations_sync": 1},
    )


def insert_paid_ads_rows(
    table_name: str,
    rows: list[list],
) -> None:
    if table_name not in PAID_TABLE_COLUMNS:
        raise ValueError(f"Unknown paid ads table: {table_name}")

    if not rows:
        return

    client = get_client()

    client.insert(
        table_name,
        rows,
        column_names=PAID_TABLE_COLUMNS[table_name],
    )


def delete_paid_ads_geo_daily_for_period(
    date_since: str,
    date_until: str,
) -> None:
    client = get_client()

    query = f"""
        ALTER TABLE instagram_ads.{PAID_ADS_GEO_DAILY_TABLE}
        DELETE WHERE date_start >= toDate({{date_since:String}})
        AND date_start <= toDate({{date_until:String}})
    """

    client.command(
        query,
        parameters={
            "date_since": date_since,
            "date_until": date_until,
        },
        settings={"mutations_sync": 1},
    )


def insert_paid_ads_geo_daily_rows(
    rows: list[list],
) -> None:
    if not rows:
        return

    client = get_client()

    client.insert(
        PAID_ADS_GEO_DAILY_TABLE,
        rows,
        column_names=PAID_ADS_GEO_DAILY_COLUMNS,
    )


def delete_paid_ads_device_daily_for_period(
    date_since: str,
    date_until: str,
) -> None:
    client = get_client()

    query = f"""
        ALTER TABLE instagram_ads.{PAID_ADS_DEVICE_DAILY_TABLE}
        DELETE WHERE date_start >= toDate({{date_since:String}})
        AND date_start <= toDate({{date_until:String}})
    """

    client.command(
        query,
        parameters={
            "date_since": date_since,
            "date_until": date_until,
        },
        settings={"mutations_sync": 1},
    )


def insert_paid_ads_device_daily_rows(
    rows: list[list],
) -> None:
    if not rows:
        return

    client = get_client()

    client.insert(
        PAID_ADS_DEVICE_DAILY_TABLE,
        rows,
        column_names=PAID_ADS_DEVICE_DAILY_COLUMNS,
    )
