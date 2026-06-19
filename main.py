import json
from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo
from typing import Any

import clickhouse_db
import config
import graph_api
import embeddings
import etl_logger as instagram_etl_logger
import instagram_core_loader


ALMATY_TZ = ZoneInfo("Asia/Almaty")


def build_daily_ad_row(
    table_name: str,
    row: dict,
    media_info_by_ad_id: dict,
    adset_info_by_adset_id: dict,
    campaign_info_by_campaign_id: dict,
) -> list:
    """
    Собирает строку для daily ad-level staging таблицы.
    Аналог build_table_row, но для daily:
    - date_start/date_stop как Date (не DateTime)
    - есть reach и frequency из API
    - нет hourly breakdown
    """
    from datetime import date as date_type
    actions = row.get("actions")
    costs = row.get("cost_per_action_type")
    ad_id = row.get("ad_id", "")
    adset_id = row.get("adset_id", "")
    campaign_id = row.get("campaign_id", "")
    media_info = media_info_by_ad_id.get(ad_id, {})
    adset_info = adset_info_by_adset_id.get(adset_id, {})
    campaign_info = campaign_info_by_campaign_id.get(
        campaign_id, {}
    )
 
    result_info = get_result_info(actions, costs, adset_info)
 
    video_play = to_int(
        extract_metric_sum(row, "video_play_actions")
    )
    video_p25 = to_int(
        extract_metric_sum(row, "video_p25_watched_actions")
    )
    video_p50 = to_int(
        extract_metric_sum(row, "video_p50_watched_actions")
    )
    video_p75 = to_int(
        extract_metric_sum(row, "video_p75_watched_actions")
    )
    video_p100 = to_int(
        extract_metric_sum(row, "video_p100_watched_actions")
    )
    video_avg = extract_metric_sum(
        row, "video_avg_time_watched_actions"
    )
 
    daily_budget = (
        campaign_info.get("daily_budget")
        or adset_info.get("daily_budget")
    )
    lifetime_budget = (
        campaign_info.get("lifetime_budget")
        or adset_info.get("lifetime_budget")
    )
    budget_remaining = (
        campaign_info.get("budget_remaining")
        or adset_info.get("budget_remaining")
    )
 
    data = {
        "date_start": date_type.fromisoformat(
            row["date_start"]
        ),
        "date_stop": date_type.fromisoformat(
            row["date_stop"]
        ),
        "campaign_id": row.get("campaign_id", ""),
        "campaign_name": row.get("campaign_name", ""),
        "campaign_status": campaign_info.get("status"),
        "campaign_effective_status": campaign_info.get(
            "effective_status"
        ),
        "campaign_start_time": campaign_info.get(
            "start_time"
        ),
        "campaign_stop_time": campaign_info.get("stop_time"),
        "adset_id": row.get("adset_id", ""),
        "adset_name": row.get("adset_name", ""),
        "ad_id": row.get("ad_id", ""),
        "ad_name": row.get("ad_name", ""),
        "destination_url": media_info.get("destination_url"),
        "media_type": media_info.get("media_type"),
        "media_product_type": media_info.get(
            "media_product_type"
        ),
        "children_count": media_info.get("children_count"),
        "children_json": media_info.get("children_json"),
        "conversion_location": adset_info.get(
            "conversion_location"
        ),
        "is_incremental_attribution_enabled": adset_info.get(
            "is_incremental_attribution_enabled"
        ),
        "attribution_setting": adset_info.get(
            "attribution_setting"
        ),
        "target_locations_json": adset_info.get(
            "target_locations_json"
        ),
        "age_range": adset_info.get("age_range"),
        "gender": adset_info.get("gender"),
        "languages_json": adset_info.get("languages_json"),
        "placements_json": adset_info.get("placements_json"),
        "objective": row.get("objective", ""),
        "result_name": result_info["result_name"],
        "result_value": result_info["result_value"],
        "cost_per_result": result_info["cost_per_result"],
        "spend": to_float(row.get("spend")),
        "impressions": to_int(row.get("impressions")),
        "reach": to_int(row.get("reach")),
        "frequency": to_float(row.get("frequency")),
        "messaging_conversation_started": extract_action_value(
            actions, ["messaging_conversation_started"]
        ),
        "cost_per_messaging_conversation_started": (
            extract_cost_value(
                costs, ["messaging_conversation_started"]
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
        "video_play_actions": video_play,
        "video_p25_watched_actions": video_p25,
        "video_p50_watched_actions": video_p50,
        "video_p75_watched_actions": video_p75,
        "video_p100_watched_actions": video_p100,
        "video_avg_time_watched_actions": video_avg,
        "daily_budget": daily_budget,
        "lifetime_budget": lifetime_budget,
        "budget_remaining": budget_remaining,
        # objective-specific (заполняются ниже)
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
            ["mobile_app_registration", "complete_registration"],
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
 
    columns = clickhouse_db.DAILY_AD_TABLE_COLUMNS[table_name]
    return [data.get(col) for col in columns]
 
 
def format_daily_ads_rows(
    ads_insights_rows: list[dict],
    media_info_by_ad_id: dict,
    adset_info_by_adset_id: dict,
    campaign_info_by_campaign_id: dict,
) -> dict[str, list[list]]:
    """
    Группирует daily строки по objective → staging таблица.
    """
    grouped: dict[str, list[list]] = {
        table: []
        for table in clickhouse_db.DAILY_AD_STAGING_TABLES.values()
    }
 
    for row in ads_insights_rows:
        objective = row.get("objective", "")
        table_name = clickhouse_db.get_target_daily_table(
            objective
        )
 
        if table_name is None:
            print(
                f"[daily] Unknown objective skipped: "
                f"{objective}"
            )
            continue
 
        formatted = build_daily_ad_row(
            table_name,
            row,
            media_info_by_ad_id,
            adset_info_by_adset_id,
            campaign_info_by_campaign_id,
        )
        grouped[table_name].append(formatted)
 
    return grouped
 
 
def build_gender_daily_row(row: dict) -> list:
    """Собирает строку для gender daily staging."""
    from datetime import date as date_type
    data = {
        "date_start": date_type.fromisoformat(
            row["date_start"]
        ),
        "date_stop": date_type.fromisoformat(
            row["date_stop"]
        ),
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
        for col in clickhouse_db.GENDER_DAILY_COLUMNS
    ]
 
 
def write_daily_ads_tables(
    grouped_rows: dict[str, list[list]],
    date_since: str,
    date_until: str,
) -> int:
    """Записывает daily ad-level строки в staging."""
    total = 0
    for table_name, rows in grouped_rows.items():
        clickhouse_db.delete_daily_ads_for_period(
            table_name=table_name,
            date_since=date_since,
            date_until=date_until,
        )
        clickhouse_db.insert_daily_ads_rows(
            table_name=table_name,
            rows=rows,
        )
        print(f"{table_name}: inserted {len(rows)} rows")
        total += len(rows)
    return total
 
 
def write_gender_daily_table(
    rows: list[list],
    date_since: str,
    date_until: str,
) -> int:
    """Записывает gender daily строки в staging."""
    clickhouse_db.delete_gender_daily_for_period(
        date_since=date_since,
        date_until=date_until,
    )
    clickhouse_db.insert_gender_daily_rows(rows)
    print(
        f"{clickhouse_db.GENDER_DAILY_STAGING_TABLE}: "
        f"inserted {len(rows)} rows"
    )
    return len(rows)


def get_hourly_datetime_range(
    row: dict[str, Any],
) -> tuple[datetime, datetime]:
    """
    Превращает дату Meta + hourly breakdown в дату-время Алматы.

    Пример:
    date_start = 2026-04-30
    hourly_stats_aggregated_by_advertiser_time_zone = 00:00:00 - 00:59:59

    Результат:
    date_start = 2026-04-30 00:00:00
    date_stop = 2026-04-30 01:00:00
    """
    row_date = date.fromisoformat(row["date_start"])

    hour_range = row.get(graph_api.HOURLY_BREAKDOWN_FIELD)

    if not hour_range:
        start_datetime = datetime.combine(
            row_date,
            time.min,
            tzinfo=ALMATY_TZ,
        )

        return start_datetime, start_datetime + timedelta(days=1)

    start_hour_text = hour_range.split(" - ")[0].split(":")[0]
    start_hour = int(start_hour_text)

    start_datetime = datetime.combine(
        row_date,
        time(hour=start_hour),
        tzinfo=ALMATY_TZ,
    )
    stop_datetime = start_datetime + timedelta(hours=1)

    return start_datetime, stop_datetime


def get_yesterday() -> str:
    """
    Возвращает вчерашнюю дату в формате YYYY-MM-DD.

    Эта функция для cron-режима,
    когда pipeline будет каждый день брать данные за прошлый день.
    """
    return (date.today() - timedelta(days=1)).isoformat()


def make_reach_key(
    row: dict[str, Any],
) -> tuple[str, str, str, str]:
    """
    Создаёт ключ для сопоставления daily reach
    с hourly-строкой.

    Ключ:
    date_start + campaign_id + adset_id + ad_id
    """
    return (
        str(row.get("date_start", "")),
        str(row.get("campaign_id", "")),
        str(row.get("adset_id", "")),
        str(row.get("ad_id", "")),
    )


def to_json_or_none(value: Any) -> str | None:
    if value in (None, [], {}):
        return None

    return json.dumps(value, ensure_ascii=False)


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

    if "iphone" in device or "ipad" in device or "ios" in device:
        return "ios"

    if "windows" in device:
        return "windows"

    if "mac" in device:
        return "macos"

    if "desktop" in device:
        return "desktop_unknown"

    return "unknown"


def build_daily_reach_by_key(
    daily_reach_rows: list[dict[str, Any]],
) -> dict[tuple[str, str, str, str], int | None]:
    """
    Создаёт словарь daily reach.

    Потом этот reach будет подставляться
    в каждую hourly-строку этого же дня.
    """
    daily_reach_by_key: dict[tuple[str, str, str, str], int | None] = {}

    for row in daily_reach_rows:
        key = make_reach_key(row)
        daily_reach_by_key[key] = to_int(row.get("reach"))

    return daily_reach_by_key


def build_daily_frequency_by_key(
    daily_reach_rows: list[dict[str, Any]],
) -> dict[tuple[str, str, str, str], float | None]:
    """
    Создаёт словарь daily frequency.

    Потом эта frequency будет подставляться
    в каждую hourly-строку этого же дня.

    Если Meta не вернула frequency, пробуем посчитать:
    impressions / reach.
    """
    daily_frequency_by_key: dict[
        tuple[str, str, str, str],
        float | None,
    ] = {}

    for row in daily_reach_rows:
        key = make_reach_key(row)

        frequency = (row.get("frequency"))

        daily_frequency_by_key[key] = frequency

    return daily_frequency_by_key


def to_float(value: Any) -> float | None:
    """
    Безопасно преобразует значение в float.

    Если значение пустое или его нельзя преобразовать,
    возвращает None.
    """
    if value is None:
        return None

    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def to_int(value: Any) -> int | None:
    """
    Безопасно преобразует значение в int.

    Meta часто отдаёт числа строками, например "123".
    Функция превращает такие значения в int.
    """
    if value is None:
        return None

    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def extract_action_value(
    actions: list[dict[str, Any]] | None,
    action_names: list[str],
) -> int | None:
    """
    Достаёт значение нужного действия из поля actions.

    Например:
    - landing_page_view
    - purchase
    - lead
    - post_engagement

    Если action_type совпадает с одним из action_names,
    значение суммируется.
    """
    if not actions:
        return None

    total = 0
    found = False

    for action in actions:
        action_type = action.get("action_type", "")

        if any(name in action_type for name in action_names):
            value = to_int(action.get("value"))

            if value is not None:
                total += value
                found = True

    return total if found else None


def extract_cost_value(
    costs: list[dict[str, Any]] | None,
    action_names: list[str],
) -> float | None:
    """
    Достаёт стоимость нужного действия из cost_per_action_type.

    Например:
    - cost_per_purchase
    - cost_per_lead
    - cost_per_landing_page_view
    """
    if not costs:
        return None

    for cost in costs:
        action_type = cost.get("action_type", "")

        if any(name in action_type for name in action_names):
            return to_float(cost.get("value"))

    return None


def get_result_name(
    result_type: str | None,
    adset_info: dict[str, Any],
) -> str | None:
    """
    Определяет наше короткое название результата.

    Нужно отличать:
    - link_clicks
    - profile_visits

    В Meta API оба варианта могут прийти как link_click,
    поэтому дополнительно смотрим настройки adset:
    destination_type и optimization_goal.
    """
    if result_type is None:
        return None

    destination_type = str(
        adset_info.get("destination_type") or ""
    ).upper()

    optimization_goal = str(
        adset_info.get("optimization_goal") or ""
    ).upper()

    is_profile_visit = (
        "INSTAGRAM_PROFILE" in destination_type
        or optimization_goal == "VISIT_INSTAGRAM_PROFILE"
    )

    if result_type == "link_click" and is_profile_visit:
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
    """
    Определяет результат рекламы для колонок:
    - result_name
    - result_value
    - cost_per_result

    result_name будет:
    - profile_visits
    - link_clicks
    - landing_page_views
    - leads
    - purchases
    и т.д.
    """
    if not actions:
        return {
            "result_name": None,
            "result_value": None,
            "cost_per_result": None,
        }

    priority_actions = [
        "profile_visit",
        "link_click",
        "landing_page_view",
        "lead",
        "messaging_conversation_started",
        "post_engagement",
        "purchase",
        "mobile_app_install",
    ]

    for priority_action in priority_actions:
        for action in actions:
            action_type = action.get("action_type", "")

            if priority_action in action_type:
                return {
                    "result_name": get_result_name(
                        action_type,
                        adset_info,
                    ),
                    "result_value": to_int(action.get("value")),
                    "cost_per_result": extract_cost_value(
                        costs,
                        [action_type],
                    ),
                }

    first_action = actions[0]
    first_action_type = first_action.get("action_type", "")

    return {
        "result_name": get_result_name(
            first_action_type,
            adset_info,
        ),
        "result_value": to_int(first_action.get("value")),
        "cost_per_result": extract_cost_value(
            costs,
            [first_action_type],
        ),
    }


def extract_metric_sum(
    row: dict[str, Any],
    field_name: str,
) -> float | None:
    """
    Достаёт и суммирует метрику, которая приходит массивом.

    Это нужно для video-полей, например:
    - video_play_actions
    - video_p25_watched_actions
    - video_avg_time_watched_actions
    """
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


def get_target_table(objective: str) -> str | None:
    """
    Определяет, в какую paid_ads_* таблицу положить строку.

    Выбор делается по objective рекламной кампании.
    """
    objective = objective.upper()

    awareness_objectives = [
        "OUTCOME_AWARENESS",
        "AWARENESS",
        "REACH",
        "BRAND_AWARENESS",
    ]

    traffic_objectives = [
        "OUTCOME_TRAFFIC",
        "TRAFFIC",
        "LINK_CLICKS",
    ]

    engagement_objectives = [
        "OUTCOME_ENGAGEMENT",
        "ENGAGEMENT",
        "POST_ENGAGEMENT",
        "VIDEO_VIEWS",
    ]

    leads_objectives = [
        "OUTCOME_LEADS",
        "LEADS",
        "LEAD_GENERATION",
    ]

    app_promotion_objectives = [
        "OUTCOME_APP_PROMOTION",
        "APP_PROMOTION",
        "APP_INSTALLS",
    ]

    sales_objectives = [
        "OUTCOME_SALES",
        "SALES",
        "CONVERSIONS",
        "PRODUCT_CATALOG_SALES",
    ]

    if objective in awareness_objectives:
        return "paid_ads_awareness_hourly_ad_level"

    if objective in traffic_objectives:
        return "paid_ads_traffic_hourly_ad_level"

    if objective in engagement_objectives:
        return "paid_ads_engagement_hourly_ad_level"

    if objective in leads_objectives:
        return "paid_ads_leads_hourly_ad_level"

    if objective in app_promotion_objectives:
        return "paid_ads_app_promotion_hourly_ad_level"

    if objective in sales_objectives:
        return "paid_ads_sales_hourly_ad_level"

    return None


def build_common_row(
    row: dict[str, Any],
    media_info_by_ad_id: dict[str, dict[str, Any]],
    adset_info_by_adset_id: dict[str, dict[str, Any]],
    campaign_info_by_campaign_id: dict[str, dict[str, Any]],
    daily_reach_by_key: dict[tuple[str, str, str, str], int | None],
    daily_frequency_by_key: dict[tuple[str, str, str, str], float | None],
) -> dict[str, Any]:
    """
    Собирает общие поля, которые есть во всех paid_ads_* таблицах.

    Например:
    - date_start
    - campaign_id
    - spend
    - impressions
    - clicks
    - video metrics
    """
    actions = row.get("actions")
    costs = row.get("cost_per_action_type")
    ad_id = row.get("ad_id", "")
    adset_id = row.get("adset_id", "")
    campaign_id = row.get("campaign_id", "")
    media_info = media_info_by_ad_id.get(ad_id, {})
    adset_info = adset_info_by_adset_id.get(adset_id, {})
    campaign_info = campaign_info_by_campaign_id.get(campaign_id, {})
    daily_reach = daily_reach_by_key.get(make_reach_key(row))
    daily_frequency = daily_frequency_by_key.get(make_reach_key(row))

    result_info = get_result_info(
        actions,
        costs,
        adset_info,
    )

    video_play_actions = to_int(
        extract_metric_sum(row, "video_play_actions")
    )
    video_p25 = to_int(
        extract_metric_sum(row, "video_p25_watched_actions")
    )
    video_p50 = to_int(
        extract_metric_sum(row, "video_p50_watched_actions")
    )
    video_p75 = to_int(
        extract_metric_sum(row, "video_p75_watched_actions")
    )
    video_p100 = to_int(
        extract_metric_sum(row, "video_p100_watched_actions")
    )
    video_avg_time = extract_metric_sum(
        row,
        "video_avg_time_watched_actions",
    )

    daily_budget = (
        campaign_info.get("daily_budget")
        if campaign_info.get("daily_budget") is not None
        else adset_info.get("daily_budget")
    )

    lifetime_budget = (
        campaign_info.get("lifetime_budget")
        if campaign_info.get("lifetime_budget") is not None
        else adset_info.get("lifetime_budget")
    )

    budget_remaining = (
        campaign_info.get("budget_remaining")
        if campaign_info.get("budget_remaining") is not None
        else adset_info.get("budget_remaining")
    )

    hourly_date_start, hourly_date_stop = get_hourly_datetime_range(row)

    return {
        "date_start": hourly_date_start,
        "date_stop": hourly_date_stop,
        "campaign_id": row.get("campaign_id", ""),
        "campaign_name": row.get("campaign_name", ""),
        "campaign_status": campaign_info.get("status"),
        "campaign_effective_status": campaign_info.get("effective_status"),
        "campaign_start_time": campaign_info.get("start_time"),
        "campaign_stop_time": campaign_info.get("stop_time"),
        "adset_id": row.get("adset_id", ""),
        "adset_name": row.get("adset_name", ""),
        "ad_id": row.get("ad_id", ""),
        "ad_name": row.get("ad_name", ""),
        "destination_url": media_info.get("destination_url"),
        "media_type": media_info.get("media_type"),
        "media_product_type": media_info.get("media_product_type"),
        "children_count": media_info.get("children_count"),
        "children_json": media_info.get("children_json"),
        "conversion_location": adset_info.get("conversion_location"),
        "is_incremental_attribution_enabled": adset_info.get(
            "is_incremental_attribution_enabled"
        ),
        "attribution_setting": adset_info.get("attribution_setting"),
        "target_locations_json": adset_info.get("target_locations_json"),
        "age_range": adset_info.get("age_range"),
        "gender": adset_info.get("gender"),
        "languages_json": adset_info.get("languages_json"),
        "placements_json": adset_info.get("placements_json"),
        "objective": row.get("objective", ""),
        "result_name": result_info["result_name"],
        "result_value": result_info["result_value"],
        "cost_per_result": result_info["cost_per_result"],
        "spend": to_float(row.get("spend")),
        "impressions": to_int(row.get("impressions")),
        "reach": (
            daily_reach
            if daily_reach is not None
            else to_int(row.get("reach"))
        ),
        "messaging_conversation_started": extract_action_value(
            actions,
            ["messaging_conversation_started"],
        ),
        "cost_per_messaging_conversation_started": extract_cost_value(
            costs,
            ["messaging_conversation_started"],
        ),
        "frequency": (
            daily_frequency
            if daily_frequency is not None
            else to_float(row.get("frequency"))
        ),
        "cpm": to_float(row.get("cpm")),
        "clicks": to_int(row.get("clicks")),
        "inline_link_clicks": to_int(row.get("inline_link_clicks")),
        "inline_link_click_ctr": to_float(
            row.get("inline_link_click_ctr")
        ),
        "ctr": to_float(row.get("ctr")),
        "cpc": to_float(row.get("cpc")),
        "video_play_actions": video_play_actions,
        "video_p25_watched_actions": video_p25,
        "video_p50_watched_actions": video_p50,
        "video_p75_watched_actions": video_p75,
        "video_p100_watched_actions": video_p100,
        "video_avg_time_watched_actions": video_avg_time,
        "daily_budget": daily_budget,
        "lifetime_budget": lifetime_budget,
        "budget_remaining": budget_remaining,
        "loaded_at": datetime.now(),
    }


def build_table_row(
    table_name: str,
    row: dict[str, Any],
    media_info_by_ad_id: dict[str, dict[str, Any]],
    adset_info_by_adset_id: dict[str, dict[str, Any]],
    campaign_info_by_campaign_id: dict[str, dict[str, Any]],
    daily_reach_by_key: dict[tuple[str, str, str, str], int | None],
    daily_frequency_by_key: dict[tuple[str, str, str, str], float | None],
) -> list[Any]:
    """
    Собирает одну строку для конкретной paid_ads_* таблицы.

    Сначала берёт общие поля, потом добавляет поля,
    которые нужны конкретным типам кампаний.
    """
    actions = row.get("actions")
    costs = row.get("cost_per_action_type")

    data = build_common_row(
        row,
        media_info_by_ad_id,
        adset_info_by_adset_id,
        campaign_info_by_campaign_id,
        daily_reach_by_key,
        daily_frequency_by_key,
    )

    data.update(
        {
            "landing_page_view": extract_action_value(
                actions,
                ["landing_page_view"],
            ),
            "cost_per_landing_page_view": extract_cost_value(
                costs,
                ["landing_page_view"],
            ),
            "comments_count": extract_action_value(
                actions,
                ["comment"],
            ),
            "likes_count": extract_action_value(
                actions,
                ["like", "post_reaction"],
            ),
            "saved": extract_action_value(
                actions,
                ["post_save", "save"],
            ),
            "shares": extract_action_value(
                actions,
                ["post_share", "share"],
            ),
            "post_engagement": extract_action_value(
                actions,
                ["post_engagement"],
            ),
            "cost_per_post_engagement": extract_cost_value(
                costs,
                ["post_engagement"],
            ),
            "profile_visits": extract_action_value(
                actions,
                ["profile_visit"],
            ),
            "leads": extract_action_value(
                actions,
                ["lead"],
            ),
            "cost_per_lead": extract_cost_value(
                costs,
                ["lead"],
            ),
            "mobile_app_install": extract_action_value(
                actions,
                ["mobile_app_install"],
            ),
            "cost_per_mobile_app_install": extract_cost_value(
                costs,
                ["mobile_app_install"],
            ),
            "mobile_app_registration": extract_action_value(
                actions,
                ["mobile_app_registration", "complete_registration"],
            ),
            "mobile_app_purchase": extract_action_value(
                actions,
                ["mobile_app_purchase"],
            ),
            "purchase": extract_action_value(
                actions,
                ["purchase"],
            ),
            "cost_per_purchase": extract_cost_value(
                costs,
                ["purchase"],
            ),
            "add_to_cart": extract_action_value(
                actions,
                ["add_to_cart"],
            ),
            "cost_per_add_to_cart": extract_cost_value(
                costs,
                ["add_to_cart"],
            ),
            "initiate_checkout": extract_action_value(
                actions,
                ["initiate_checkout"],
            ),
            "cost_per_initiate_checkout": extract_cost_value(
                costs,
                ["initiate_checkout"],
            ),
            "view_content": extract_action_value(
                actions,
                ["view_content"],
            ),
            "cost_per_view_content": extract_cost_value(
                costs,
                ["view_content"],
            ),
        }
    )

    columns = clickhouse_db.PAID_TABLE_COLUMNS[table_name]

    return [data.get(column) for column in columns]


def build_geo_daily_row(
    row: dict[str, Any],
) -> list[Any]:
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
        "inline_link_clicks": to_int(row.get("inline_link_clicks")),
        "ctr": to_float(row.get("ctr")),

        "loaded_at": datetime.now(ALMATY_TZ),
    }

    return [
        data.get(column)
        for column in clickhouse_db.PAID_ADS_GEO_DAILY_STAGING_COLUMNS
    ]


def build_device_daily_row(
    row: dict[str, Any],
) -> list[Any]:
    device_platform = row.get("device_platform")
    impression_device = row.get("impression_device")

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

        "device_platform": device_platform,
        "impression_device": impression_device,

        "device_type": normalize_device_type(
            device_platform=device_platform,
            impression_device=impression_device,
        ),
        "os_type": normalize_os_type(
            impression_device=impression_device,
        ),

        "spend": to_float(row.get("spend")),
        "impressions": to_int(row.get("impressions")) or 0,
        "reach": to_int(row.get("reach")),
        "frequency": to_float(row.get("frequency")),
        "cpm": to_float(row.get("cpm")),

        "clicks": to_int(row.get("clicks")) or 0,
        "inline_link_clicks": to_int(row.get("inline_link_clicks")),
        "ctr": to_float(row.get("ctr")),

        "loaded_at": datetime.now(ALMATY_TZ),
    }

    return [
        data.get(column)
        for column in clickhouse_db.PAID_ADS_DEVICE_DAILY_STAGING_COLUMNS
    ]


def format_geo_daily_rows(
    rows: list[dict[str, Any]],
) -> list[list[Any]]:
    return [
        build_geo_daily_row(row)
        for row in rows
    ]


def format_device_daily_rows(
    rows: list[dict[str, Any]],
) -> list[list[Any]]:
    return [
        build_device_daily_row(row)
        for row in rows
    ]


def format_ads_insights_rows(
    ads_insights_rows: list[dict[str, Any]],
    media_info_by_ad_id: dict[str, dict[str, Any]],
    adset_info_by_adset_id: dict[str, dict[str, Any]],
    campaign_info_by_campaign_id: dict[str, dict[str, Any]],
    daily_reach_by_key: dict[tuple[str, str, str, str], int | None],
    daily_frequency_by_key: dict[tuple[str, str, str, str], float | None],
) -> dict[str, list[list[Any]]]:
    """
    Форматирует строки Ads Insights и группирует их по paid_ads_* таблицам.

    На выходе получается словарь:
    {
        "paid_ads_traffic": [...],
        "paid_ads_awareness": [...],
        ...
    }
    """
    grouped_rows: dict[str, list[list[Any]]] = {
        table_name: []
        for table_name in clickhouse_db.PAID_TABLE_COLUMNS
    }

    for row in ads_insights_rows:
        objective = row.get("objective", "")
        table_name = get_target_table(objective)

        if table_name is None:
            print(f"Unknown objective skipped: {objective}")
            continue

        formatted_row = build_table_row(
            table_name,
            row,
            media_info_by_ad_id,
            adset_info_by_adset_id,
            campaign_info_by_campaign_id,
            daily_reach_by_key,
            daily_frequency_by_key,
        )
        grouped_rows[table_name].append(formatted_row)

    return grouped_rows


def save_raw_ads_insights(
    response_data: dict[str, Any],
    date_since: str,
    date_until: str,
) -> None:
    """
    Сохраняет сырой ответ Ads Insights в ClickHouse raw_data.

    raw_data не очищается и не перезаписывается.
    Каждый запуск добавляет новую raw-запись.
    """
    clickhouse_db.insert_raw_data(
        source="ads_insights",
        api_type="ads_insights",
        endpoint=f"/{config.AD_ACCOUNT_ID}/insights",
        object_id=config.AD_ACCOUNT_ID,
        response_data=response_data,
        request_params={
            "date_since": date_since,
            "date_until": date_until,
            "level": "ad",
            "time_increment": 1,
            "breakdowns": graph_api.HOURLY_BREAKDOWN_FIELD,
            "limit": 100,
        },
    )


def save_raw_daily_reach_response(
    response_data: dict[str, Any],
    date_since: str,
    date_until: str,
) -> None:
    """
    Сохраняет сырой ответ daily reach запроса в raw_data.

    Этот запрос нужен, чтобы подставлять дневной reach
    в почасовые строки.
    """
    clickhouse_db.insert_raw_data(
        source="ads_insights_daily_reach",
        api_type="ads_insights",
        endpoint=f"/{config.AD_ACCOUNT_ID}/insights",
        object_id=config.AD_ACCOUNT_ID,
        response_data=response_data,
        request_params={
            "date_since": date_since,
            "date_until": date_until,
            "level": "ad",
            "time_increment": 1,
            "limit": 100,
            "purpose": "daily_reach_for_hourly_rows",
        },
    )


def save_raw_geo_daily_response(
    response_data: dict[str, Any],
    date_since: str,
    date_until: str,
) -> None:
    clickhouse_db.insert_raw_data(
        source="ads_insights_geo_daily",
        api_type="ads_insights",
        endpoint=f"/{config.AD_ACCOUNT_ID}/insights",
        object_id=config.AD_ACCOUNT_ID,
        response_data=response_data,
        request_params={
            "date_since": date_since,
            "date_until": date_until,
            "level": "ad",
            "time_increment": 1,
            "breakdowns": "country,region",
            "limit": 100,
        },
    )


def save_raw_device_daily_response(
    response_data: dict[str, Any],
    date_since: str,
    date_until: str,
) -> None:
    clickhouse_db.insert_raw_data(
        source="ads_insights_device_daily",
        api_type="ads_insights",
        endpoint=f"/{config.AD_ACCOUNT_ID}/insights",
        object_id=config.AD_ACCOUNT_ID,
        response_data=response_data,
        request_params={
            "date_since": date_since,
            "date_until": date_until,
            "level": "ad",
            "time_increment": 1,
            "breakdowns": "device_platform,impression_device",
            "limit": 100,
        },
    )


def write_paid_ads_tables(
    grouped_rows: dict[str, list[list[Any]]],
    date_since: str,
    date_until: str,
) -> int:
    total = 0
    for old_table, rows in grouped_rows.items():
        staging_table = old_table + "_staging"

        if not rows:
            print(f"{staging_table}: inserted 0 rows")
            continue

        old_cols = clickhouse_db.PAID_TABLE_COLUMNS[old_table]
        new_cols = clickhouse_db.HOURLY_STAGING_TABLE_COLUMNS[
            staging_table
        ]
        # Индексы колонок которые нужно оставить
        keep_idx = [
            i for i, c in enumerate(old_cols)
            if c in new_cols
        ]
        # Фильтруем строки — берём только нужные колонки
        filtered_rows = [
            [row[i] for i in keep_idx]
            for row in rows
        ]

        clickhouse_db.delete_hourly_staging_for_period(
            table_name=staging_table,
            date_since=date_since,
            date_until=date_until,
        )
        clickhouse_db.insert_hourly_staging_rows(
            table_name=staging_table,
            rows=filtered_rows,
        )
        print(f"{staging_table}: inserted {len(rows)} rows")
        total += len(rows)
    return total

def write_geo_daily_table(
    rows: list[list[Any]],
    date_since: str,
    date_until: str,
) -> None:
    clickhouse_db.delete_geo_daily_staging_for_period(
        date_since=date_since,
        date_until=date_until,
    )

    clickhouse_db.insert_geo_daily_staging_rows(rows)

    print(
        f"{clickhouse_db.GEO_DAILY_STAGING_TABLE}: "
        f"inserted {len(rows)} rows"
    )


def write_device_daily_table(
    rows: list[list[Any]],
    date_since: str,
    date_until: str,
) -> None:
    clickhouse_db.delete_device_daily_staging_for_period(
        date_since=date_since,
        date_until=date_until,
    )

    clickhouse_db.insert_device_daily_staging_rows(rows)

    print(
        f"{clickhouse_db.DEVICE_DAILY_STAGING_TABLE}: "
        f"inserted {len(rows)} rows"
    )


def iter_date_batches(
    start_date: str,
    end_date: str,
    batch_days: int,
) -> list[tuple[str, str]]:
    """
    Делит большой период на батчи.

    Например:
    2025-12-01 -> 2026-05-18
    batch_days = 3

    Получим:
    2025-12-01 -> 2025-12-03
    2025-12-04 -> 2025-12-06
    ...
    """
    start = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date)

    batches: list[tuple[str, str]] = []
    current_start = start

    while current_start <= end:
        current_end = min(
            current_start + timedelta(days=batch_days - 1),
            end,
        )

        batches.append(
            (
                current_start.isoformat(),
                current_end.isoformat(),
            )
        )

        current_start = current_end + timedelta(days=1)

    return batches


def run_pipeline_for_period(
    date_since: str,
    date_until: str,
    run_id: str,
) -> tuple[int, int]:
    """
    Запускает pipeline за один период.
    Возвращает (total_raw_rows, total_staging_rows).
    """
    print(f"Start period: {date_since} -> {date_until}")
 
    total_raw = 0
    total_staging = 0
 
    # ----------------------------------------------------------
    # Шаг 1: fetch_raw
    # ----------------------------------------------------------
    with instagram_etl_logger.etl_step(
        run_id=run_id,
        step_name="fetch_raw",
        step_order=1,
    ) as step:
 
        # Hourly insights (существующий запрос)
        ads_insights_response = graph_api.get_ads_insights(
            date_since=date_since,
            date_until=date_until,
        )
 
        # Daily insights (новый запрос — reach/frequency)
        daily_response = graph_api.get_ads_insights_daily(
            date_since=date_since,
            date_until=date_until,
        )
 
        # Geo daily
        geo_daily_response = (
            graph_api.get_ads_insights_geo_daily(
                date_since=date_since,
                date_until=date_until,
            )
        )
 
        # Device daily
        device_daily_response = (
            graph_api.get_ads_insights_device_daily(
                date_since=date_since,
                date_until=date_until,
            )
        )
 
        # Gender daily (новый запрос)
        gender_daily_response = (
            graph_api.get_ads_insights_gender_daily(
                date_since=date_since,
                date_until=date_until,
            )
        )
 
        raw_count = (
            ads_insights_response.get("rows_count", 0)
            + daily_response.get("rows_count", 0)
            + geo_daily_response.get("rows_count", 0)
            + device_daily_response.get("rows_count", 0)
            + gender_daily_response.get("rows_count", 0)
        )
 
        # Подсчёт через len(data) если rows_count нет
        hourly_rows = ads_insights_response.get("data", [])
        daily_rows = daily_response.get("data", [])
        geo_rows = geo_daily_response.get("data", [])
        device_rows = device_daily_response.get("data", [])
        gender_rows = gender_daily_response.get("data", [])
 
        raw_count = (
            len(hourly_rows) + len(daily_rows)
            + len(geo_rows) + len(device_rows)
            + len(gender_rows)
        )
 
        step["input_rows"] = raw_count
        step["output_rows"] = raw_count
        total_raw = raw_count
 
    # ----------------------------------------------------------
    # Шаг 2: raw_to_staging
    # ----------------------------------------------------------
    with instagram_etl_logger.etl_step(
        run_id=run_id,
        step_name="raw_to_staging",
        step_order=2,
    ) as step:
 
        # Сохранить raw
        save_raw_ads_insights(
            response_data=ads_insights_response,
            date_since=date_since,
            date_until=date_until,
        )
        save_raw_geo_daily_response(
            response_data=geo_daily_response,
            date_since=date_since,
            date_until=date_until,
        )
        save_raw_device_daily_response(
            response_data=device_daily_response,
            date_since=date_since,
            date_until=date_until,
        )
 
        # Hourly → existing paid_ads_* таблицы (без изменений)
        ad_ids = [r["ad_id"] for r in hourly_rows if r.get("ad_id")]
        adset_ids = [
            r["adset_id"] for r in hourly_rows
            if r.get("adset_id")
        ]
        campaign_ids = [
            r["campaign_id"] for r in hourly_rows
            if r.get("campaign_id")
        ]
 
        media_info = graph_api.get_media_info_for_ads(ad_ids)
        adset_info = graph_api.get_adset_details_for_adsets(
            adset_ids
        )
        campaign_info = (
            graph_api.get_campaign_details_for_campaigns(
                campaign_ids
            )
        )
 
        # Hourly staged (без изменений — в old paid_ads_* таблицы)
        daily_reach_by_key = build_daily_reach_by_key([])
        daily_frequency_by_key = build_daily_frequency_by_key([])
 
        hourly_grouped = format_ads_insights_rows(
            hourly_rows,
            media_info,
            adset_info,
            campaign_info,
            daily_reach_by_key,
            daily_frequency_by_key,
        )
        write_paid_ads_tables(
            grouped_rows=hourly_grouped,
            date_since=date_since,
            date_until=date_until,
        )
 
        # Daily → новые *_daily_ad_level_staging таблицы
        # Daily строки используют те же ad_ids что и hourly
        daily_ad_ids = [
            r["ad_id"] for r in daily_rows if r.get("ad_id")
        ]
        daily_adset_ids = [
            r["adset_id"] for r in daily_rows
            if r.get("adset_id")
        ]
        daily_campaign_ids = [
            r["campaign_id"] for r in daily_rows
            if r.get("campaign_id")
        ]
 
        # Переиспользуем уже полученные данные где возможно
        new_ad_ids = set(daily_ad_ids) - set(ad_ids)
        new_adset_ids = set(daily_adset_ids) - set(adset_ids)
        new_campaign_ids = (
            set(daily_campaign_ids) - set(campaign_ids)
        )
 
        if new_ad_ids:
            extra_media = graph_api.get_media_info_for_ads(
                list(new_ad_ids)
            )
            media_info.update(extra_media)
 
        if new_adset_ids:
            extra_adset = (
                graph_api.get_adset_details_for_adsets(
                    list(new_adset_ids)
                )
            )
            adset_info.update(extra_adset)
 
        if new_campaign_ids:
            extra_campaign = (
                graph_api.get_campaign_details_for_campaigns(
                    list(new_campaign_ids)
                )
            )
            campaign_info.update(extra_campaign)
 
        daily_grouped = format_daily_ads_rows(
            daily_rows,
            media_info,
            adset_info,
            campaign_info,
        )
        staging_daily = write_daily_ads_tables(
            grouped_rows=daily_grouped,
            date_since=date_since,
            date_until=date_until,
        )
 
        # Geo → staging
        formatted_geo = format_geo_daily_rows(geo_rows)
        write_geo_daily_table(
            rows=formatted_geo,
            date_since=date_since,
            date_until=date_until,
        )
 
        # Device → staging
        formatted_device = format_device_daily_rows(
            device_rows
        )
        write_device_daily_table(
            rows=formatted_device,
            date_since=date_since,
            date_until=date_until,
        )
 
        # Gender → staging (новое)
        formatted_gender = [
            build_gender_daily_row(r) for r in gender_rows
        ]
        staging_gender = write_gender_daily_table(
            rows=formatted_gender,
            date_since=date_since,
            date_until=date_until,
        )
 
        staging_count = (
            write_paid_ads_tables(  # теперь возвращает int
                grouped_rows=hourly_grouped,
                date_since=date_since,
                date_until=date_until,
            )
            + staging_daily
            + len(formatted_geo)
            + len(formatted_device)
            + staging_gender
        )
 
        step["input_rows"] = raw_count
        step["output_rows"] = staging_count
        total_staging = staging_count
 
    print(f"Finished period: {date_since} -> {date_until}")
    return total_raw, total_staging


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
 
    all_unique_ad_rows: dict[str, dict] = {}
 
    try:
        for batch_since, batch_until in batches:
            raw, staging = run_pipeline_for_period(
                date_since=batch_since,
                date_until=batch_until,
                run_id=run_id,
            )
            total_raw += raw
            total_staging += staging
 
        # Core загрузка
        total_core = instagram_core_loader.run_staging_to_core(
            run_id=run_id,
            date_since=date_since,
            date_until=date_until,
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
 
        # Embeddings (если включены)
        embeddings.process_ads_insights_image_embeddings(
            list(all_unique_ad_rows.values())
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
