from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo
from typing import Any

import clickhouse_db
import config
import graph_api
import embeddings


ALMATY_TZ = ZoneInfo("Asia/Almaty")


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
        return "paid_ads_awareness"

    if objective in traffic_objectives:
        return "paid_ads_traffic"

    if objective in engagement_objectives:
        return "paid_ads_engagement"

    if objective in leads_objectives:
        return "paid_ads_leads"

    if objective in app_promotion_objectives:
        return "paid_ads_app_promotion"

    if objective in sales_objectives:
        return "paid_ads_sales"

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


def write_paid_ads_tables(
    grouped_rows: dict[str, list[list[Any]]],
    date_since: str,
    date_until: str,
) -> None:
    """
    Записывает отформатированные данные в paid_ads_* таблицы.

    Перед вставкой удаляет старые строки за этот же период,
    чтобы не создавать дубли в форматированных таблицах.
    """
    for table_name, rows in grouped_rows.items():
        clickhouse_db.delete_paid_ads_for_period(
            table_name=table_name,
            date_since=date_since,
            date_until=date_until,
        )

        clickhouse_db.insert_paid_ads_rows(
            table_name=table_name,
            rows=rows,
        )

        print(f"{table_name}: inserted {len(rows)} rows")


def main() -> None:
    """
    Главная функция pipeline.

    Делает полный цикл:
    1. Проверяет config.
    2. Забирает Ads Insights из Meta API.
    3. Сохраняет сырой JSON в raw_data.
    4. Форматирует данные.
    5. Раскладывает данные по paid_ads_* таблицам.
    """
    config.validate_config()

    date_since = "2026-04-30"
    date_until = "2026-05-09"

    ads_insights_response = graph_api.get_ads_insights(
        date_since=date_since,
        date_until=date_until,
    )

    daily_reach_response = graph_api.get_ads_insights_daily_reach(
        date_since=date_since,
        date_until=date_until,
    )

    save_raw_ads_insights(
        response_data=ads_insights_response,
        date_since=date_since,
        date_until=date_until,
    )

    save_raw_daily_reach_response(
        response_data=daily_reach_response,
        date_since=date_since,
        date_until=date_until,
    )

    print(f"Ads insights raw data saved from {date_since} to {date_until}")

    ads_insights_rows = ads_insights_response.get("data", [])
    daily_reach_rows = daily_reach_response.get("data", [])
    daily_reach_by_key = build_daily_reach_by_key(daily_reach_rows)
    daily_frequency_by_key = build_daily_frequency_by_key(
        daily_reach_rows
    )
    ad_ids = [
        row["ad_id"]
        for row in ads_insights_rows
        if row.get("ad_id")
    ]

    adset_ids = [
        row["adset_id"]
        for row in ads_insights_rows
        if row.get("adset_id")
    ]

    campaign_ids = [
        row["campaign_id"]
        for row in ads_insights_rows
        if row.get("campaign_id")
    ]

    media_info_by_ad_id = (
        graph_api.get_media_info_for_ads(
            ad_ids
        )
    )
    adset_info_by_adset_id = (
        graph_api.get_adset_details_for_adsets(
            adset_ids
        )
    )
    campaign_info_by_campaign_id = (
        graph_api.get_campaign_details_for_campaigns(
            campaign_ids
        )
    )

    grouped_rows = format_ads_insights_rows(
        ads_insights_rows,
        media_info_by_ad_id,
        adset_info_by_adset_id,
        campaign_info_by_campaign_id,
        daily_reach_by_key,
        daily_frequency_by_key,
    )

    write_paid_ads_tables(
        grouped_rows=grouped_rows,
        date_since=date_since,
        date_until=date_until,
    )

    embeddings.process_ads_insights_image_embeddings(ads_insights_rows)


if __name__ == "__main__":
    main()
