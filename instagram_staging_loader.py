"""
instagram_staging_loader.py

Шаг 2 ETL: instagram_raw → instagram_staging.
Читает сырые данные из raw_data, трансформирует
и записывает в staging таблицы.
"""

import json
from datetime import date, datetime, time, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import clickhouse_db
import config
import etl_logger as instagram_etl_logger
import graph_api


ALMATY_TZ = ZoneInfo("Asia/Almaty")

STAGING_DB = config.CLICKHOUSE_STAGING_DB


# ------------------------------------------------------------
# Type conversion helpers
# ------------------------------------------------------------

def to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def to_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def to_json_or_none(value: Any) -> str | None:
    if value in (None, [], {}):
        return None
    return json.dumps(value, ensure_ascii=False)


# ------------------------------------------------------------
# Action / cost extraction
# ------------------------------------------------------------

def extract_action_value(
    actions: list[dict[str, Any]] | None,
    action_names: list[str],
) -> int | None:
    if not actions:
        return None
    total = 0
    found = False
    for action in actions:
        action_type = action.get("action_type", "")
        if any(n in action_type for n in action_names):
            value = to_int(action.get("value"))
            if value is not None:
                total += value
                found = True
    return total if found else None


def extract_cost_value(
    costs: list[dict[str, Any]] | None,
    action_names: list[str],
) -> float | None:
    if not costs:
        return None
    for cost in costs:
        action_type = cost.get("action_type", "")
        if any(n in action_type for n in action_names):
            return to_float(cost.get("value"))
    return None


def extract_metric_sum(
    row: dict[str, Any],
    field_name: str,
) -> float | None:
    value = row.get(field_name)
    if isinstance(value, list):
        total: float = 0.0
        found = False
        for item in value:
            item_value = to_float(item.get("value"))
            if item_value is not None:
                total += item_value
                found = True
        return total if found else None
    return to_float(value)


# ------------------------------------------------------------
# Result info
# ------------------------------------------------------------

def get_result_name(
    result_type: str | None,
    adset_info: dict[str, Any],
) -> str | None:
    if result_type is None:
        return None
    destination_type = str(
        adset_info.get("destination_type") or ""
    ).upper()
    optimization_goal = str(
        adset_info.get("optimization_goal") or ""
    ).upper()
    is_profile = (
        "INSTAGRAM_PROFILE" in destination_type
        or optimization_goal == "VISIT_INSTAGRAM_PROFILE"
    )
    if result_type == "link_click" and is_profile:
        return "profile_visits"
    if result_type == "link_click":
        return "link_clicks"
    if "profile_visit" in result_type:
        return "profile_visits"
    if "landing_page_view" in result_type:
        return "landing_page_views"
    if "lead" in result_type:
        return "leads"
    if "purchase" in result_type:
        return "purchases"
    if "post_engagement" in result_type:
        return "post_engagement"
    if "messaging_conversation_started" in result_type:
        return "messaging_conversation_started"
    if "mobile_app_install" in result_type:
        return "mobile_app_installs"
    return result_type


def get_result_info(
    actions: list[dict[str, Any]] | None,
    costs: list[dict[str, Any]] | None,
    adset_info: dict[str, Any],
) -> dict[str, Any]:
    if not actions:
        return {
            "result_name": None,
            "result_value": None,
            "cost_per_result": None,
        }
    priority = [
        "profile_visit",
        "link_click",
        "landing_page_view",
        "lead",
        "messaging_conversation_started",
        "post_engagement",
        "purchase",
        "mobile_app_install",
    ]
    for p in priority:
        for action in actions:
            atype = action.get("action_type", "")
            if p in atype:
                return {
                    "result_name": get_result_name(
                        atype, adset_info
                    ),
                    "result_value": to_int(
                        action.get("value")
                    ),
                    "cost_per_result": extract_cost_value(
                        costs, [atype]
                    ),
                }
    first = actions[0]
    ftype = first.get("action_type", "")
    return {
        "result_name": get_result_name(ftype, adset_info),
        "result_value": to_int(first.get("value")),
        "cost_per_result": extract_cost_value(
            costs, [ftype]
        ),
    }


# ------------------------------------------------------------
# Device / OS normalization
# ------------------------------------------------------------

def normalize_device_type(
    device_platform: str | None,
    impression_device: str | None,
) -> str:
    platform = str(device_platform or "").lower()
    device = str(impression_device or "").lower()
    if "desktop" in platform or "desktop" in device:
        return "pc"
    if "tablet" in device or "ipad" in device:
        return "tablet"
    if (
        "mobile" in platform
        or "phone" in device
        or "iphone" in device
        or "smartphone" in device
        or "android" in device
    ):
        return "phone"
    return "unknown"


def normalize_os_type(
    impression_device: str | None,
) -> str:
    device = str(impression_device or "").lower()
    if "android" in device:
        return "android"
    if "iphone" in device or "ipad" in device \
            or "ios" in device:
        return "ios"
    if "windows" in device:
        return "windows"
    if "mac" in device:
        return "macos"
    if "desktop" in device:
        return "desktop_unknown"
    return "unknown"


# ------------------------------------------------------------
# Hourly date range
# ------------------------------------------------------------

def get_hourly_datetime_range(
    row: dict[str, Any],
) -> tuple[datetime, datetime]:
    row_date = date.fromisoformat(row["date_start"])
    hour_range = row.get(graph_api.HOURLY_BREAKDOWN_FIELD)

    if not hour_range:
        start_dt = datetime.combine(
            row_date, time.min, tzinfo=ALMATY_TZ
        )
        return start_dt, start_dt + timedelta(days=1)

    start_hour = int(
        hour_range.split(" - ")[0].split(":")[0]
    )
    start_dt = datetime.combine(
        row_date,
        time(hour=start_hour),
        tzinfo=ALMATY_TZ,
    )
    return start_dt, start_dt + timedelta(hours=1)


# ------------------------------------------------------------
# Common enriched fields (shared by hourly and daily)
# ------------------------------------------------------------

def _build_common_fields(
    row: dict[str, Any],
    media_info: dict[str, dict[str, Any]],
    adset_info: dict[str, dict[str, Any]],
    campaign_info: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    actions = row.get("actions")
    costs = row.get("cost_per_action_type")
    ad_id = row.get("ad_id", "")
    adset_id = row.get("adset_id", "")
    campaign_id = row.get("campaign_id", "")

    media = media_info.get(ad_id, {})
    adset = adset_info.get(adset_id, {})
    campaign = campaign_info.get(campaign_id, {})

    result = get_result_info(actions, costs, adset)

    daily_budget = (
        campaign.get("daily_budget")
        or adset.get("daily_budget")
    )
    lifetime_budget = (
        campaign.get("lifetime_budget")
        or adset.get("lifetime_budget")
    )
    budget_remaining = (
        campaign.get("budget_remaining")
        or adset.get("budget_remaining")
    )

    return {
        "campaign_id": row.get("campaign_id", ""),
        "campaign_name": row.get("campaign_name", ""),
        "campaign_status": campaign.get("status"),
        "campaign_effective_status": campaign.get(
            "effective_status"
        ),
        "campaign_start_time": campaign.get("start_time"),
        "campaign_stop_time": campaign.get("stop_time"),
        "adset_id": row.get("adset_id", ""),
        "adset_name": row.get("adset_name", ""),
        "ad_id": ad_id,
        "ad_name": row.get("ad_name", ""),
        "destination_url": media.get("destination_url"),
        "media_type": media.get("media_type"),
        "media_product_type": media.get(
            "media_product_type"
        ),
        "children_count": media.get("children_count"),
        "children_json": media.get("children_json"),
        "conversion_location": adset.get(
            "conversion_location"
        ),
        "is_incremental_attribution_enabled": adset.get(
            "is_incremental_attribution_enabled"
        ),
        "attribution_setting": adset.get(
            "attribution_setting"
        ),
        "target_locations_json": adset.get(
            "target_locations_json"
        ),
        "age_range": adset.get("age_range"),
        "gender": adset.get("gender"),
        "languages_json": adset.get("languages_json"),
        "placements_json": adset.get("placements_json"),
        "objective": row.get("objective", ""),
        "result_name": result["result_name"],
        "result_value": result["result_value"],
        "cost_per_result": result["cost_per_result"],
        "spend": to_float(row.get("spend")),
        "impressions": to_int(row.get("impressions")),
        "messaging_conversation_started": (
            extract_action_value(
                actions,
                ["messaging_conversation_started"],
            )
        ),
        "cost_per_messaging_conversation_started": (
            extract_cost_value(
                costs,
                ["messaging_conversation_started"],
            )
        ),
        "cpm": to_float(row.get("cpm")),
        "clicks": to_int(row.get("clicks")),
        "inline_link_clicks": to_int(
            row.get("inline_link_clicks")
        ),
        "inline_link_click_ctr": to_float(
            row.get("inline_link_click_ctr")
        ),
        "ctr": to_float(row.get("ctr")),
        "cpc": to_float(row.get("cpc")),
        "video_play_actions": to_int(
            extract_metric_sum(row, "video_play_actions")
        ),
        "video_p25_watched_actions": to_int(
            extract_metric_sum(
                row, "video_p25_watched_actions"
            )
        ),
        "video_p50_watched_actions": to_int(
            extract_metric_sum(
                row, "video_p50_watched_actions"
            )
        ),
        "video_p75_watched_actions": to_int(
            extract_metric_sum(
                row, "video_p75_watched_actions"
            )
        ),
        "video_p100_watched_actions": to_int(
            extract_metric_sum(
                row, "video_p100_watched_actions"
            )
        ),
        "video_avg_time_watched_actions": (
            extract_metric_sum(
                row, "video_avg_time_watched_actions"
            )
        ),
        "daily_budget": daily_budget,
        "lifetime_budget": lifetime_budget,
        "budget_remaining": budget_remaining,
        "landing_page_view": extract_action_value(
            actions, ["landing_page_view"]
        ),
        "cost_per_landing_page_view": extract_cost_value(
            costs, ["landing_page_view"]
        ),
        "comments_count": extract_action_value(
            actions, ["comment"]
        ),
        "likes_count": extract_action_value(
            actions, ["like", "post_reaction"]
        ),
        "saved": extract_action_value(
            actions, ["post_save", "save"]
        ),
        "shares": extract_action_value(
            actions, ["post_share", "share"]
        ),
        "post_engagement": extract_action_value(
            actions, ["post_engagement"]
        ),
        "cost_per_post_engagement": extract_cost_value(
            costs, ["post_engagement"]
        ),
        "profile_visits": extract_action_value(
            actions, ["profile_visit"]
        ),
        "leads": extract_action_value(actions, ["lead"]),
        "cost_per_lead": extract_cost_value(
            costs, ["lead"]
        ),
        "mobile_app_install": extract_action_value(
            actions, ["mobile_app_install"]
        ),
        "cost_per_mobile_app_install": extract_cost_value(
            costs, ["mobile_app_install"]
        ),
        "mobile_app_registration": extract_action_value(
            actions,
            [
                "mobile_app_registration",
                "complete_registration",
            ],
        ),
        "mobile_app_purchase": extract_action_value(
            actions, ["mobile_app_purchase"]
        ),
        "purchase": extract_action_value(
            actions, ["purchase"]
        ),
        "cost_per_purchase": extract_cost_value(
            costs, ["purchase"]
        ),
        "add_to_cart": extract_action_value(
            actions, ["add_to_cart"]
        ),
        "cost_per_add_to_cart": extract_cost_value(
            costs, ["add_to_cart"]
        ),
        "initiate_checkout": extract_action_value(
            actions, ["initiate_checkout"]
        ),
        "cost_per_initiate_checkout": extract_cost_value(
            costs, ["initiate_checkout"]
        ),
        "view_content": extract_action_value(
            actions, ["view_content"]
        ),
        "cost_per_view_content": extract_cost_value(
            costs, ["view_content"]
        ),
        "loaded_at": datetime.now(ALMATY_TZ),
    }


# ------------------------------------------------------------
# Row builders
# ------------------------------------------------------------

def build_hourly_row(
    table: str,
    row: dict[str, Any],
    media_info: dict[str, dict[str, Any]],
    adset_info: dict[str, dict[str, Any]],
    campaign_info: dict[str, dict[str, Any]],
) -> list[Any]:
    data = _build_common_fields(
        row, media_info, adset_info, campaign_info
    )
    date_start, date_stop = get_hourly_datetime_range(row)
    data["date_start"] = date_start
    data["date_stop"] = date_stop
    cols = clickhouse_db.HOURLY_COLUMNS[table]
    return [data.get(col) for col in cols]


def build_daily_row(
    table: str,
    row: dict[str, Any],
    media_info: dict[str, dict[str, Any]],
    adset_info: dict[str, dict[str, Any]],
    campaign_info: dict[str, dict[str, Any]],
) -> list[Any]:
    data = _build_common_fields(
        row, media_info, adset_info, campaign_info
    )
    data["date_start"] = date.fromisoformat(
        row["date_start"]
    )
    data["date_stop"] = date.fromisoformat(row["date_stop"])
    data["reach"] = to_int(row.get("reach"))
    data["frequency"] = to_float(row.get("frequency"))
    cols = clickhouse_db.DAILY_COLUMNS[table]
    return [data.get(col) for col in cols]


def build_geo_row(row: dict[str, Any]) -> list[Any]:
    data = {
        "date_start": date.fromisoformat(row["date_start"]),
        "date_stop": date.fromisoformat(row["date_stop"]),
        "campaign_id": row.get("campaign_id", ""),
        "campaign_name": row.get("campaign_name"),
        "adset_id": row.get("adset_id", ""),
        "adset_name": row.get("adset_name"),
        "ad_id": row.get("ad_id", ""),
        "ad_name": row.get("ad_name"),
        "objective": row.get("objective"),
        "country": row.get("country"),
        "region": row.get("region"),
        "spend": to_float(row.get("spend")),
        "impressions": to_int(row.get("impressions")) or 0,
        "reach": to_int(row.get("reach")),
        "frequency": to_float(row.get("frequency")),
        "cpm": to_float(row.get("cpm")),
        "clicks": to_int(row.get("clicks")) or 0,
        "inline_link_clicks": to_int(
            row.get("inline_link_clicks")
        ),
        "ctr": to_float(row.get("ctr")),
        "loaded_at": datetime.now(ALMATY_TZ),
    }
    return [
        data.get(col)
        for col in clickhouse_db.GEO_COLUMNS
    ]


def build_device_row(row: dict[str, Any]) -> list[Any]:
    dp = row.get("device_platform")
    imp_dev = row.get("impression_device")
    data = {
        "date_start": date.fromisoformat(row["date_start"]),
        "date_stop": date.fromisoformat(row["date_stop"]),
        "campaign_id": row.get("campaign_id", ""),
        "campaign_name": row.get("campaign_name"),
        "adset_id": row.get("adset_id", ""),
        "adset_name": row.get("adset_name"),
        "ad_id": row.get("ad_id", ""),
        "ad_name": row.get("ad_name"),
        "objective": row.get("objective"),
        "device_platform": dp,
        "impression_device": imp_dev,
        "device_type": normalize_device_type(dp, imp_dev),
        "os_type": normalize_os_type(imp_dev),
        "spend": to_float(row.get("spend")),
        "impressions": to_int(row.get("impressions")) or 0,
        "reach": to_int(row.get("reach")),
        "frequency": to_float(row.get("frequency")),
        "cpm": to_float(row.get("cpm")),
        "clicks": to_int(row.get("clicks")) or 0,
        "inline_link_clicks": to_int(
            row.get("inline_link_clicks")
        ),
        "ctr": to_float(row.get("ctr")),
        "loaded_at": datetime.now(ALMATY_TZ),
    }
    return [
        data.get(col)
        for col in clickhouse_db.DEVICE_COLUMNS
    ]


def build_gender_row(row: dict[str, Any]) -> list[Any]:
    data = {
        "date_start": date.fromisoformat(row["date_start"]),
        "date_stop": date.fromisoformat(row["date_stop"]),
        "campaign_id": row.get("campaign_id", ""),
        "campaign_name": row.get("campaign_name"),
        "adset_id": row.get("adset_id", ""),
        "adset_name": row.get("adset_name"),
        "ad_id": row.get("ad_id", ""),
        "ad_name": row.get("ad_name"),
        "objective": row.get("objective"),
        "age_range": row.get("age"),
        "gender_type": row.get("gender"),
        "spend": to_float(row.get("spend")),
        "impressions": to_int(row.get("impressions")),
        "reach": to_int(row.get("reach")),
        "frequency": to_float(row.get("frequency")),
        "cpm": to_float(row.get("cpm")),
        "clicks": to_int(row.get("clicks")),
        "inline_link_clicks": to_int(
            row.get("inline_link_clicks")
        ),
        "ctr": to_float(row.get("ctr")),
        "loaded_at": datetime.now(ALMATY_TZ),
    }
    return [
        data.get(col)
        for col in clickhouse_db.GENDER_COLUMNS
    ]


# ------------------------------------------------------------
# Formatters: rows → grouped by staging table
# ------------------------------------------------------------

def format_hourly_rows(
    rows: list[dict[str, Any]],
    media_info: dict[str, dict[str, Any]],
    adset_info: dict[str, dict[str, Any]],
    campaign_info: dict[str, dict[str, Any]],
) -> dict[str, list[list[Any]]]:
    grouped: dict[str, list[list[Any]]] = {
        t: [] for t in clickhouse_db.HOURLY_TABLES.values()
    }
    for row in rows:
        objective = row.get("objective", "")
        table = clickhouse_db.get_hourly_table(objective)
        if table is None:
            print(
                f"[hourly] Unknown objective skipped: "
                f"{objective}"
            )
            continue
        grouped[table].append(
            build_hourly_row(
                table, row,
                media_info, adset_info, campaign_info,
            )
        )
    return grouped


def format_daily_rows(
    rows: list[dict[str, Any]],
    media_info: dict[str, dict[str, Any]],
    adset_info: dict[str, dict[str, Any]],
    campaign_info: dict[str, dict[str, Any]],
) -> dict[str, list[list[Any]]]:
    grouped: dict[str, list[list[Any]]] = {
        t: [] for t in clickhouse_db.DAILY_TABLES.values()
    }
    for row in rows:
        objective = row.get("objective", "")
        table = clickhouse_db.get_daily_table(objective)
        if table is None:
            print(
                f"[daily] Unknown objective skipped: "
                f"{objective}"
            )
            continue
        grouped[table].append(
            build_daily_row(
                table, row,
                media_info, adset_info, campaign_info,
            )
        )
    return grouped


# ------------------------------------------------------------
# Enrichment fetcher (media / adset / campaign)
# ------------------------------------------------------------

def fetch_enrichment(
    ad_ids: list[str],
    adset_ids: list[str],
    campaign_ids: list[str],
) -> tuple[
    dict[str, dict[str, Any]],
    dict[str, dict[str, Any]],
    dict[str, dict[str, Any]],
]:
    media = graph_api.get_media_info_for_ads(ad_ids)
    adset = graph_api.get_adset_details_for_adsets(
        adset_ids
    )
    campaign = (
        graph_api.get_campaign_details_for_campaigns(
            campaign_ids
        )
    )
    return media, adset, campaign


def _merge_enrichment(
    base_media: dict,
    base_adset: dict,
    base_campaign: dict,
    extra_ad_ids: list[str],
    extra_adset_ids: list[str],
    extra_campaign_ids: list[str],
) -> None:
    if extra_ad_ids:
        base_media.update(
            graph_api.get_media_info_for_ads(extra_ad_ids)
        )
    if extra_adset_ids:
        base_adset.update(
            graph_api.get_adset_details_for_adsets(
                extra_adset_ids
            )
        )
    if extra_campaign_ids:
        base_campaign.update(
            graph_api.get_campaign_details_for_campaigns(
                extra_campaign_ids
            )
        )


# ------------------------------------------------------------
# Entry point: raw → staging
# ------------------------------------------------------------

def run_raw_to_staging(
    *,
    run_id: str,
    date_since: str,
    date_until: str,
) -> int:
    total = 0

    with instagram_etl_logger.etl_step(
        run_id=run_id,
        step_name="raw_to_staging",
        step_order=2,
        target_database=STAGING_DB,
    ) as step:
        # Read all 5 sources from raw
        hourly_rows = clickhouse_db.read_raw(
            "ads_insights_hourly", date_since, date_until
        )
        daily_rows = clickhouse_db.read_raw(
            "ads_insights_daily", date_since, date_until
        )
        geo_rows = clickhouse_db.read_raw(
            "ads_insights_geo", date_since, date_until
        )
        device_rows = clickhouse_db.read_raw(
            "ads_insights_device", date_since, date_until
        )
        gender_rows = clickhouse_db.read_raw(
            "ads_insights_gender", date_since, date_until
        )

        raw_count = (
            len(hourly_rows)
            + len(daily_rows)
            + len(geo_rows)
            + len(device_rows)
            + len(gender_rows)
        )
        step["input_rows"] = raw_count

        # Collect unique IDs from hourly rows
        hourly_ad_ids = list({
            r["ad_id"]
            for r in hourly_rows
            if r.get("ad_id")
        })
        hourly_adset_ids = list({
            r["adset_id"]
            for r in hourly_rows
            if r.get("adset_id")
        })
        hourly_campaign_ids = list({
            r["campaign_id"]
            for r in hourly_rows
            if r.get("campaign_id")
        })

        media_info, adset_info, campaign_info = (
            fetch_enrichment(
                hourly_ad_ids,
                hourly_adset_ids,
                hourly_campaign_ids,
            )
        )

        # Merge extra IDs from daily rows
        _merge_enrichment(
            media_info,
            adset_info,
            campaign_info,
            extra_ad_ids=list(
                {r["ad_id"] for r in daily_rows
                 if r.get("ad_id")}
                - set(hourly_ad_ids)
            ),
            extra_adset_ids=list(
                {r["adset_id"] for r in daily_rows
                 if r.get("adset_id")}
                - set(hourly_adset_ids)
            ),
            extra_campaign_ids=list(
                {r["campaign_id"] for r in daily_rows
                 if r.get("campaign_id")}
                - set(hourly_campaign_ids)
            ),
        )

        # Hourly staging
        hourly_grouped = format_hourly_rows(
            hourly_rows, media_info, adset_info,
            campaign_info,
        )
        total += clickhouse_db.write_hourly_staging(
            hourly_grouped, date_since, date_until
        )

        # Daily staging
        daily_grouped = format_daily_rows(
            daily_rows, media_info, adset_info,
            campaign_info,
        )
        total += clickhouse_db.write_daily_staging(
            daily_grouped, date_since, date_until
        )

        # Geo staging
        total += clickhouse_db.write_geo_staging(
            [build_geo_row(r) for r in geo_rows],
            date_since,
            date_until,
        )

        # Device staging
        total += clickhouse_db.write_device_staging(
            [build_device_row(r) for r in device_rows],
            date_since,
            date_until,
        )

        # Gender staging
        total += clickhouse_db.write_gender_staging(
            [build_gender_row(r) for r in gender_rows],
            date_since,
            date_until,
        )

        step["output_rows"] = total

    return total
