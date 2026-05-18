import json

from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

import requests

import config


BASE_URL = f"https://graph.facebook.com/{config.GRAPH_API_VERSION}"

HOURLY_BREAKDOWN_FIELD = (
    "hourly_stats_aggregated_by_advertiser_time_zone"
)


class GraphAPIError(Exception):
    pass


def format_meta_datetime_to_almaty(value: str | None) -> str | None:
    """
    Преобразует дату Meta из UTC/offset формата в Алматы.

    Пример:
    2026-04-29T19:00:00+0000
    ->
    2026-04-30 00:00:00
    """
    if not value:
        return None

    try:
        parsed_datetime = datetime.strptime(
            value,
            "%Y-%m-%dT%H:%M:%S%z",
        )
    except ValueError:
        return value

    almaty_datetime = parsed_datetime.astimezone(
        ZoneInfo("Asia/Almaty")
    )

    return almaty_datetime.strftime("%Y-%m-%d %H:%M:%S")


def graph_get(endpoint: str, params: dict | None = None) -> dict:
    """
    Универсальная функция для одного GET-запроса к Meta Graph API.
    """

    if params is None:
        params = {}

    params["access_token"] = config.META_ACCESS_TOKEN

    url = f"{BASE_URL}{endpoint}"

    response = requests.get(url, params=params, timeout=30)

    try:
        data = response.json()
    except ValueError:
        raise GraphAPIError(f"Invalid JSON response: {response.text}")

    if response.status_code != 200:
        raise GraphAPIError(f"Graph API error: {data}")

    if "error" in data:
        raise GraphAPIError(f"Graph API error: {data['error']}")

    return data


def graph_get_all_pages(endpoint: str, params: dict | None = None) -> dict:
    """
    Получить все страницы ответа, если Meta вернула paging.next.
    """

    first_page = graph_get(endpoint, params)

    all_data = first_page.get("data", [])
    next_url = first_page.get("paging", {}).get("next")

    while next_url:
        response = requests.get(next_url, timeout=30)

        try:
            page_data = response.json()
        except ValueError:
            raise GraphAPIError(f"Invalid JSON response: {response.text}")

        if response.status_code != 200:
            raise GraphAPIError(f"Graph API error: {page_data}")

        if "error" in page_data:
            raise GraphAPIError(f"Graph API error: {page_data['error']}")

        all_data.extend(page_data.get("data", []))
        next_url = page_data.get("paging", {}).get("next")

    return {
        "data": all_data,
    }


def get_campaigns() -> dict:
    """
    Получить рекламные кампании из рекламного аккаунта.
    """

    endpoint = f"/{config.AD_ACCOUNT_ID}/campaigns"

    fields = [
        "id",
        "name",
        "objective",
        "status",
        "effective_status",
        "buying_type",
        "created_time",
        "updated_time",
        "start_time",
        "stop_time",
    ]

    params = {
        "fields": ",".join(fields),
        "limit": 100,
    }

    return graph_get_all_pages(endpoint, params)


def get_ads_insights(date_since: str, date_until: str) -> dict:
    """
    Получить рекламную статистику по объявлениям за указанный период.
    """

    endpoint = f"/{config.AD_ACCOUNT_ID}/insights"

    fields = [
        "date_start",
        "date_stop",
        "campaign_id",
        "campaign_name",
        "adset_id",
        "adset_name",
        "ad_id",
        "ad_name",
        "objective",
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
        "actions",
        "cost_per_action_type",
        "video_play_actions",
        "video_p25_watched_actions",
        "video_p50_watched_actions",
        "video_p75_watched_actions",
        "video_p100_watched_actions",
        "video_avg_time_watched_actions",
    ]

    time_range = {
        "since": date_since,
        "until": date_until,
    }

    params = {
        "fields": ",".join(fields),
        "level": "ad",
        "time_increment": 1,
        "breakdowns": HOURLY_BREAKDOWN_FIELD,
        "limit": 100,
        "time_range": json.dumps(time_range),
    }

    return graph_get_all_pages(endpoint, params)


def get_ad_creative(ad_id: str) -> dict:
    """
    Получает creative конкретного объявления.

    Вызывается только для ad_id, которые уже пришли в ads_insights.
    """
    endpoint = f"/{ad_id}"

    params = {
        "fields": (
            "creative{"
            "id,"
            "effective_instagram_media_id,"
            "object_url,"
            "link_url,"
            "template_url,"
            "url_tags,"
            "object_story_id,"
            "effective_object_story_id,"
            "object_story_spec,"
            "asset_feed_spec"
            "}"
        ),
    }

    return graph_get(endpoint, params)


def get_ig_media_info(ig_media_id: str) -> dict:
    """
    Получает media_type, media_product_type и children по Instagram media id.
    """
    endpoint = f"/{ig_media_id}"

    params = {
        "fields": (
            "id,"
            "media_type,"
            "media_product_type,"
            "media_url,"
            "thumbnail_url,"
            "children{id,media_type,media_url,thumbnail_url}"
        ),
    }

    return graph_get(endpoint, params)


def is_destination_url(value: Any) -> bool:
    """
    Проверяет, похоже ли значение на ссылку,
    куда ведёт пользователь после клика по рекламе.

    Отсекаем ссылки на картинки/видео/CDN,
    потому что они не являются destination URL.
    """
    if not isinstance(value, str):
        return False

    if not value.startswith(("http://", "https://")):
        return False

    blocked_parts = [
        "cdninstagram.com",
        "fbcdn.net",
        "scontent",
        ".jpg",
        ".jpeg",
        ".png",
        ".webp",
        ".mp4",
    ]

    lowered_value = value.lower()

    return not any(part in lowered_value for part in blocked_parts)


def find_url_recursively(data: Any) -> str | None:
    """
    Рекурсивно ищет destination URL в любом вложенном dict/list.

    Это fallback на случай, если Meta положила ссылку
    в нестандартное место внутри creative.
    """
    priority_key_parts = [
        "website_url",
        "object_url",
        "link_url",
        "template_url",
        "link",
        "url",
    ]

    if isinstance(data, dict):
        for key_part in priority_key_parts:
            for key, value in data.items():
                key_lower = str(key).lower()

                if key_part in key_lower and is_destination_url(value):
                    return value

        for value in data.values():
            found_url = find_url_recursively(value)

            if found_url:
                return found_url

    if isinstance(data, list):
        for item in data:
            found_url = find_url_recursively(item)

            if found_url:
                return found_url

    return None


def extract_destination_url_from_creative(
    creative: dict[str, Any],
) -> str | None:
    """
    Достаёт ссылку, куда ведёт объявление.

    Проверяет:
    - object_url
    - link_url
    - template_url
    - object_story_spec.link_data
    - object_story_spec.link_data.child_attachments
    - object_story_spec.video_data
    - asset_feed_spec.link_urls
    - asset_feed_spec.call_to_actions

    Если ничего не найдено, делает рекурсивный поиск
    по всему creative.
    """
    known_direct_fields = [
        "object_url",
        "link_url",
        "template_url",
    ]

    for field in known_direct_fields:
        value = creative.get(field)

        if is_destination_url(value):
            return value

    object_story_spec = creative.get("object_story_spec") or {}
    link_data = object_story_spec.get("link_data") or {}

    known_url_fields = [
        "link",
        "url",
        "website_url",
        "display_url",
    ]

    for field in known_url_fields:
        value = link_data.get(field)

        if is_destination_url(value):
            return value

    call_to_action = link_data.get("call_to_action") or {}
    call_to_action_value = call_to_action.get("value") or {}

    for field in known_url_fields:
        value = call_to_action_value.get(field)

        if is_destination_url(value):
            return value

    child_attachments = link_data.get("child_attachments") or []

    for child in child_attachments:
        for field in known_url_fields:
            value = child.get(field)

            if is_destination_url(value):
                return value

        child_call_to_action = child.get("call_to_action") or {}
        child_call_to_action_value = (
            child_call_to_action.get("value") or {}
        )

        for field in known_url_fields:
            value = child_call_to_action_value.get(field)

            if is_destination_url(value):
                return value

    video_data = object_story_spec.get("video_data") or {}

    for field in known_url_fields:
        value = video_data.get(field)

        if is_destination_url(value):
            return value

    video_call_to_action = video_data.get("call_to_action") or {}
    video_call_to_action_value = video_call_to_action.get("value") or {}

    for field in known_url_fields:
        value = video_call_to_action_value.get(field)

        if is_destination_url(value):
            return value

    asset_feed_spec = creative.get("asset_feed_spec") or {}
    link_urls = asset_feed_spec.get("link_urls") or []

    for link_item in link_urls:
        for field in known_url_fields:
            value = link_item.get(field)

            if is_destination_url(value):
                return value

    call_to_actions = asset_feed_spec.get("call_to_actions") or []

    for action in call_to_actions:
        action_value = action.get("value") or {}

        for field in known_url_fields:
            value = action_value.get(field)

            if is_destination_url(value):
                return value

    return find_url_recursively(creative)


def build_children_info(media_response: dict) -> tuple[int | None, str | None]:
    """
    Преобразует children из Instagram media в children_count и children_json.

    В children_json сохраняем только:
    - asset_position
    - media_type
    """
    children = media_response.get("children", {})
    children_data = children.get("data", [])

    if not children_data:
        return 0, None

    children_items = []

    for position, child in enumerate(children_data, start=1):
        children_items.append(
            {
                "asset_position": position,
                "media_type": child.get("media_type"),
            }
        )

    return len(children_items), json.dumps(
        children_items,
        ensure_ascii=False,
    )


def get_media_info_for_ads(
    ad_ids: list[str],
) -> dict[str, dict[str, Any]]:
    """
    Получает media_type, media_product_type и children
    только для ad_id, которые есть в ads_insights.
    """
    result: dict[str, dict[str, Any]] = {}

    for ad_id in sorted(set(ad_ids)):
        result[ad_id] = {
            "media_type": None,
            "media_product_type": None,
            "children_count": None,
            "children_json": None,
            "destination_url": None,
        }

        try:
            creative_response = get_ad_creative(ad_id)
        except GraphAPIError as error:
            print(f"Creative not found for ad_id={ad_id}: {error}")
            continue

        creative = creative_response.get("creative") or {}
        destination_url = extract_destination_url_from_creative(creative)
        ig_media_id = creative.get("effective_instagram_media_id")

        if not ig_media_id:
            result[ad_id]["destination_url"] = destination_url
            continue

        try:
            media_response = get_ig_media_info(ig_media_id)
        except GraphAPIError as error:
            print(f"IG media not found for ad_id={ad_id}: {error}")
            continue

        children_count, children_json = build_children_info(media_response)

        result[ad_id] = {
            "media_type": media_response.get("media_type"),
            "media_product_type": media_response.get("media_product_type"),
            "children_count": children_count,
            "children_json": children_json,
            "destination_url": destination_url,
        }

    return result


def normalize_budget_value(value: str | int | float | None) -> float | None:
    """
    Преобразует budget из минимальных единиц валюты в обычную сумму.

    Например, для USD:
    10000 -> 100.00
    """
    if value is None:
        return None

    try:
        return float(value) / 100
    except (TypeError, ValueError):
        return None


def to_json_or_none(value: Any) -> str | None:
    """
    Превращает dict/list в JSON-строку.

    Если значения нет, возвращает None.
    """
    if value in (None, [], {}):
        return None

    return json.dumps(value, ensure_ascii=False)


def get_conversion_location(
    adset_response: dict[str, Any],
) -> str | None:
    """
    Определяет место получения конверсий.

    Возможные значения:
    - website
    - app
    - messaging_destinations
    - instagram_or_facebook
    - calls
    - другое значение из destination_type / optimization_goal
    """
    destination_type = str(
        adset_response.get("destination_type") or ""
    ).upper()

    optimization_goal = str(
        adset_response.get("optimization_goal") or ""
    ).upper()

    promoted_object = adset_response.get("promoted_object") or {}

    if (
        "WEBSITE" in destination_type
        or promoted_object.get("pixel_id")
        or promoted_object.get("custom_event_type")
    ):
        return "website"

    if (
        "APP" in destination_type
        or promoted_object.get("application_id")
        or promoted_object.get("object_store_url")
    ):
        return "app"

    if (
        "MESSENGER" in destination_type
        or "WHATSAPP" in destination_type
        or "MESSAGING" in destination_type
        or "CONVERSATIONS" in optimization_goal
    ):
        return "messaging_destinations"

    if (
        "INSTAGRAM" in destination_type
        or "FACEBOOK" in destination_type
        or optimization_goal == "VISIT_INSTAGRAM_PROFILE"
    ):
        return "instagram_or_facebook"

    if "CALL" in destination_type or "CALL" in optimization_goal:
        return "calls"

    if destination_type:
        return destination_type.lower()

    if optimization_goal:
        return optimization_goal.lower()

    return None


def get_attribution_setting(
    adset_response: dict[str, Any],
) -> str | None:
    """
    Возвращает модель атрибуции.

    Если Meta не отдала attribution_spec,
    считаем модель стандартной.
    """
    attribution_spec = adset_response.get("attribution_spec")

    if attribution_spec:
        return json.dumps(attribution_spec, ensure_ascii=False)

    return "standard"


def get_age_range(targeting: dict[str, Any]) -> str | None:
    """
    Возвращает возрастной диапазон.

    Например:
    18-65+
    """
    age_min = targeting.get("age_min")
    age_max = targeting.get("age_max")

    if age_min is None and age_max is None:
        return None

    if age_min is None:
        age_min = 18

    if age_max is None:
        age_max = "65+"

    return f"{age_min}-{age_max}"


def get_gender(targeting: dict[str, Any]) -> str:
    """
    Возвращает пол таргетинга.

    Meta обычно кодирует:
    1 = male
    2 = female

    Если genders нет, значит все.
    """
    genders = targeting.get("genders") or []

    if not genders:
        return "all"

    gender_set = set(genders)

    if gender_set == {1}:
        return "male"

    if gender_set == {2}:
        return "female"

    if gender_set == {1, 2}:
        return "all"

    return json.dumps(genders, ensure_ascii=False)


def get_target_locations_json(
    targeting: dict[str, Any],
) -> str | None:
    """
    Возвращает местоположения таргетинга как JSON.
    """
    geo_locations = targeting.get("geo_locations") or {}

    return to_json_or_none(geo_locations)


def get_languages_json(
    targeting: dict[str, Any],
) -> str | None:
    """
    Возвращает языки таргетинга.

    Если locales нет, значит все языки.
    """
    locales = targeting.get("locales")

    if not locales:
        return json.dumps(["all"], ensure_ascii=False)

    return json.dumps(locales, ensure_ascii=False)


def get_placements_json(
    targeting: dict[str, Any],
) -> str | None:
    """
    Возвращает места размещения как JSON.

    Например:
    - instagram_positions
    - facebook_positions
    - messenger_positions
    - audience_network_positions
    - publisher_platforms
    """
    placement_fields = [
        "publisher_platforms",
        "facebook_positions",
        "instagram_positions",
        "messenger_positions",
        "audience_network_positions",
        "device_platforms",
    ]

    placements = {}

    for field in placement_fields:
        value = targeting.get(field)

        if value:
            placements[field] = value

    return to_json_or_none(placements)


def get_adset_details(adset_id: str) -> dict:
    """
    Получает настройки группы объявлений.

    Используем:
    - budget-поля
    - optimization_goal и destination_type
    - attribution_spec
    - promoted_object
    - targeting
    """
    endpoint = f"/{adset_id}"

    params = {
        "fields": (
            "id,"
            "daily_budget,"
            "lifetime_budget,"
            "budget_remaining,"
            "optimization_goal,"
            "destination_type,"
            "is_incremental_attribution_enabled,"
            "attribution_spec,"
            "promoted_object,"
            "targeting"
        ),
    }

    return graph_get(endpoint, params)


def get_adset_details_for_adsets(
    adset_ids: list[str],
) -> dict[str, dict[str, Any]]:
    """
    Получает budget, conversion, attribution и targeting
    только для тех adset_id, которые пришли из ads_insights.
    """
    result: dict[str, dict[str, Any]] = {}

    for adset_id in sorted(set(adset_ids)):
        result[adset_id] = {
            "daily_budget": None,
            "lifetime_budget": None,
            "budget_remaining": None,
            "optimization_goal": None,
            "destination_type": None,
            "conversion_location": None,
            "is_incremental_attribution_enabled": None,
            "attribution_setting": None,
            "target_locations_json": None,
            "age_range": None,
            "gender": None,
            "languages_json": None,
            "placements_json": None,
        }

        try:
            adset_response = get_adset_details(adset_id)
        except GraphAPIError as error:
            print(f"Adset details not found for adset_id={adset_id}: {error}")
            continue

        targeting = adset_response.get("targeting") or {}

        result[adset_id] = {
            "daily_budget": normalize_budget_value(
                adset_response.get("daily_budget")
            ),
            "lifetime_budget": normalize_budget_value(
                adset_response.get("lifetime_budget")
            ),
            "budget_remaining": normalize_budget_value(
                adset_response.get("budget_remaining")
            ),
            "optimization_goal": adset_response.get("optimization_goal"),
            "destination_type": adset_response.get("destination_type"),
            "conversion_location": get_conversion_location(adset_response),
            "is_incremental_attribution_enabled": adset_response.get(
                "is_incremental_attribution_enabled"
            ),
            "attribution_setting": get_attribution_setting(adset_response),
            "target_locations_json": get_target_locations_json(targeting),
            "age_range": get_age_range(targeting),
            "gender": get_gender(targeting),
            "languages_json": get_languages_json(targeting),
            "placements_json": get_placements_json(targeting),
        }

    return result


def get_campaign_details(campaign_id: str) -> dict:
    """
    Получает поля кампании.

    Используем:
    - budget-поля
    - status: статус, выбранный пользователем
    - effective_status: фактический статус Meta
    - start_time / stop_time: даты начала и завершения кампании
    """
    endpoint = f"/{campaign_id}"

    params = {
        "fields": (
            "id,"
            "daily_budget,"
            "lifetime_budget,"
            "budget_remaining,"
            "status,"
            "effective_status,"
            "start_time,"
            "stop_time"
        ),
    }

    return graph_get(endpoint, params)


def get_campaign_details_for_campaigns(
    campaign_ids: list[str],
) -> dict[str, dict[str, Any]]:
    """
    Получает campaign-поля только для campaign_id,
    которые пришли из ads_insights.
    """
    result: dict[str, dict[str, Any]] = {}

    for campaign_id in sorted(set(campaign_ids)):
        result[campaign_id] = {
            "daily_budget": None,
            "lifetime_budget": None,
            "budget_remaining": None,
            "status": None,
            "effective_status": None,
            "start_time": None,
            "stop_time": None,
        }

        try:
            campaign_response = get_campaign_details(campaign_id)
        except GraphAPIError as error:
            print(
                f"Campaign details not found "
                f"for campaign_id={campaign_id}: {error}"
            )
            continue

        result[campaign_id] = {
            "daily_budget": normalize_budget_value(
                campaign_response.get("daily_budget")
            ),
            "lifetime_budget": normalize_budget_value(
                campaign_response.get("lifetime_budget")
            ),
            "budget_remaining": normalize_budget_value(
                campaign_response.get("budget_remaining")
            ),
            "status": campaign_response.get("status"),
            "effective_status": campaign_response.get("effective_status"),
            "start_time": format_meta_datetime_to_almaty(
                campaign_response.get("start_time")
            ),
            "stop_time": format_meta_datetime_to_almaty(
                campaign_response.get("stop_time")
            ),
        }

    return result


def get_ads_insights_daily_reach(
    date_since: str,
    date_until: str,
) -> dict:
    """
    Получает дневной reach без hourly breakdown.

    Нужен потому, что reach плохо работает
    в почасовой разбивке, но нормально приходит по дням.
    """
    endpoint = f"/{config.AD_ACCOUNT_ID}/insights"

    fields = [
        "date_start",
        "date_stop",
        "campaign_id",
        "campaign_name",
        "adset_id",
        "adset_name",
        "ad_id",
        "ad_name",
        "reach",
        "impressions",
        "frequency",
    ]

    time_range = {
        "since": date_since,
        "until": date_until,
    }

    params = {
        "fields": ",".join(fields),
        "level": "ad",
        "time_increment": 1,
        "limit": 100,
        "time_range": json.dumps(time_range),
    }

    return graph_get_all_pages(endpoint, params)
