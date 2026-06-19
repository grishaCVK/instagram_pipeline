import json
import time

from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

import requests

import config


BASE_URL = (
    f"https://graph.facebook.com/{config.GRAPH_API_VERSION}"
)

HOURLY_BREAKDOWN_FIELD = (
    "hourly_stats_aggregated_by_advertiser_time_zone"
)


class GraphAPIError(Exception):
    pass


def format_meta_datetime_to_almaty(
    value: str | None,
) -> str | None:
    if not value:
        return None
    try:
        parsed = datetime.strptime(
            value, "%Y-%m-%dT%H:%M:%S%z"
        )
    except ValueError:
        return value
    almaty = parsed.astimezone(ZoneInfo("Asia/Almaty"))
    return almaty.strftime("%Y-%m-%d %H:%M:%S")


def graph_get(
    endpoint: str,
    params: dict | None = None,
) -> dict:
    if params is None:
        params = {}
    request_params = dict(params)
    request_params["access_token"] = (
        config.META_ACCESS_TOKEN
    )
    url = f"{BASE_URL}{endpoint}"
    last_error: Exception | None = None

    for attempt in range(1, 6):
        try:
            response = requests.get(
                url,
                params=request_params,
                timeout=30,
            )
            try:
                data = response.json()
            except ValueError:
                raise GraphAPIError(
                    f"Invalid JSON: {response.text}"
                )
            if response.status_code != 200:
                raise GraphAPIError(
                    f"Graph API error: {data}"
                )
            if "error" in data:
                raise GraphAPIError(
                    f"Graph API error: {data['error']}"
                )
            return data

        except requests.exceptions.RequestException as e:
            last_error = e
            wait = min(60, attempt * 10)
            print(
                f"Graph API network error: "
                f"endpoint={endpoint}, "
                f"attempt={attempt}/5, "
                f"wait={wait}s, error={e}"
            )
            time.sleep(wait)

    raise GraphAPIError(
        f"Graph API failed after retries: {last_error}"
    )


def graph_get_all_pages(
    endpoint: str,
    params: dict | None = None,
) -> dict:
    first_page = graph_get(endpoint, params)
    all_data = first_page.get("data", [])
    next_url = (
        first_page.get("paging", {}).get("next")
    )

    while next_url:
        response = requests.get(next_url, timeout=30)
        try:
            page_data = response.json()
        except ValueError:
            raise GraphAPIError(
                f"Invalid JSON: {response.text}"
            )
        if response.status_code != 200:
            raise GraphAPIError(
                f"Graph API error: {page_data}"
            )
        if "error" in page_data:
            raise GraphAPIError(
                f"Graph API error: {page_data['error']}"
            )
        all_data.extend(page_data.get("data", []))
        next_url = page_data.get("paging", {}).get("next")

    return {"data": all_data}


def get_ad_creative(ad_id: str) -> dict:
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
    endpoint = f"/{ig_media_id}"
    params = {
        "fields": (
            "id,"
            "media_type,"
            "media_product_type,"
            "media_url,"
            "thumbnail_url,"
            "children{"
            "id,media_type,media_url,thumbnail_url"
            "}"
        ),
    }
    return graph_get(endpoint, params)


def is_destination_url(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    if not value.startswith(("http://", "https://")):
        return False
    blocked = [
        "cdninstagram.com",
        "fbcdn.net",
        "scontent",
        ".jpg",
        ".jpeg",
        ".png",
        ".webp",
        ".mp4",
    ]
    low = value.lower()
    return not any(part in low for part in blocked)


def find_url_recursively(data: Any) -> str | None:
    priority_keys = [
        "website_url",
        "object_url",
        "link_url",
        "template_url",
        "link",
        "url",
    ]
    if isinstance(data, dict):
        for key_part in priority_keys:
            for key, value in data.items():
                if (
                    key_part in str(key).lower()
                    and is_destination_url(value)
                ):
                    return value
        for value in data.values():
            found = find_url_recursively(value)
            if found:
                return found
    if isinstance(data, list):
        for item in data:
            found = find_url_recursively(item)
            if found:
                return found
    return None


def extract_destination_url_from_creative(
    creative: dict[str, Any],
) -> str | None:
    for field in ("object_url", "link_url", "template_url"):
        value = creative.get(field)
        if is_destination_url(value):
            return value

    spec = creative.get("object_story_spec") or {}
    link_data = spec.get("link_data") or {}
    url_fields = ["link", "url", "website_url",
                  "display_url"]

    for field in url_fields:
        if is_destination_url(link_data.get(field)):
            return link_data[field]

    cta_value = (
        (link_data.get("call_to_action") or {})
        .get("value") or {}
    )
    for field in url_fields:
        if is_destination_url(cta_value.get(field)):
            return cta_value[field]

    for child in (link_data.get("child_attachments") or []):
        for field in url_fields:
            if is_destination_url(child.get(field)):
                return child[field]
        child_cta = (
            (child.get("call_to_action") or {})
            .get("value") or {}
        )
        for field in url_fields:
            if is_destination_url(child_cta.get(field)):
                return child_cta[field]

    video_data = spec.get("video_data") or {}
    for field in url_fields:
        if is_destination_url(video_data.get(field)):
            return video_data[field]
    video_cta = (
        (video_data.get("call_to_action") or {})
        .get("value") or {}
    )
    for field in url_fields:
        if is_destination_url(video_cta.get(field)):
            return video_cta[field]

    feed_spec = creative.get("asset_feed_spec") or {}
    for link_item in (feed_spec.get("link_urls") or []):
        for field in url_fields:
            if is_destination_url(link_item.get(field)):
                return link_item[field]
    for action in (feed_spec.get("call_to_actions") or []):
        action_val = action.get("value") or {}
        for field in url_fields:
            if is_destination_url(action_val.get(field)):
                return action_val[field]

    return find_url_recursively(creative)


def build_children_info(
    media_response: dict,
) -> tuple[int, str | None]:
    children_data = (
        media_response.get("children", {}).get("data", [])
    )
    if not children_data:
        return 0, None
    items = [
        {
            "asset_position": pos,
            "media_type": child.get("media_type"),
        }
        for pos, child in enumerate(children_data, start=1)
    ]
    return len(items), json.dumps(
        items, ensure_ascii=False
    )


def get_media_info_for_ads(
    ad_ids: list[str],
) -> dict[str, dict[str, Any]]:
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
            creative_resp = get_ad_creative(ad_id)
        except GraphAPIError as e:
            print(
                f"Creative not found for "
                f"ad_id={ad_id}: {e}"
            )
            continue

        creative = creative_resp.get("creative") or {}
        dest_url = extract_destination_url_from_creative(
            creative
        )
        ig_id = creative.get(
            "effective_instagram_media_id"
        )

        if not ig_id:
            result[ad_id]["destination_url"] = dest_url
            continue

        try:
            media_resp = get_ig_media_info(ig_id)
        except GraphAPIError as e:
            print(
                f"IG media not found for "
                f"ad_id={ad_id}: {e}"
            )
            continue

        children_count, children_json = (
            build_children_info(media_resp)
        )
        result[ad_id] = {
            "media_type": media_resp.get("media_type"),
            "media_product_type": media_resp.get(
                "media_product_type"
            ),
            "children_count": children_count,
            "children_json": children_json,
            "destination_url": dest_url,
        }

    return result


def normalize_budget_value(
    value: str | int | float | None,
) -> float | None:
    if value is None:
        return None
    try:
        return float(value) / 100
    except (TypeError, ValueError):
        return None


def to_json_or_none(value: Any) -> str | None:
    if value in (None, [], {}):
        return None
    return json.dumps(value, ensure_ascii=False)


def get_conversion_location(
    adset_response: dict[str, Any],
) -> str | None:
    destination_type = str(
        adset_response.get("destination_type") or ""
    ).upper()
    optimization_goal = str(
        adset_response.get("optimization_goal") or ""
    ).upper()
    promoted = adset_response.get("promoted_object") or {}

    if (
        "WEBSITE" in destination_type
        or promoted.get("pixel_id")
        or promoted.get("custom_event_type")
    ):
        return "website"
    if (
        "APP" in destination_type
        or promoted.get("application_id")
        or promoted.get("object_store_url")
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
    if (
        "CALL" in destination_type
        or "CALL" in optimization_goal
    ):
        return "calls"
    if destination_type:
        return destination_type.lower()
    if optimization_goal:
        return optimization_goal.lower()
    return None


def get_attribution_setting(
    adset_response: dict[str, Any],
) -> str | None:
    attribution_spec = adset_response.get(
        "attribution_spec"
    )
    if attribution_spec:
        return json.dumps(
            attribution_spec, ensure_ascii=False
        )
    return "standard"


def get_age_range(targeting: dict[str, Any]) -> str | None:
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
    return to_json_or_none(
        targeting.get("geo_locations") or {}
    )


def get_languages_json(
    targeting: dict[str, Any],
) -> str | None:
    locales = targeting.get("locales")
    if not locales:
        return json.dumps(["all"], ensure_ascii=False)
    return json.dumps(locales, ensure_ascii=False)


def get_placements_json(
    targeting: dict[str, Any],
) -> str | None:
    placement_fields = [
        "publisher_platforms",
        "facebook_positions",
        "instagram_positions",
        "messenger_positions",
        "audience_network_positions",
        "device_platforms",
    ]
    placements = {
        f: targeting[f]
        for f in placement_fields
        if targeting.get(f)
    }
    return to_json_or_none(placements)


def get_adset_details_for_adsets(
    adset_ids: list[str],
) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    empty = {
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

    for adset_id in sorted(set(adset_ids)):
        result[adset_id] = dict(empty)
        try:
            resp = graph_get(
                f"/{adset_id}",
                params={
                    "fields": (
                        "id,"
                        "daily_budget,"
                        "lifetime_budget,"
                        "budget_remaining,"
                        "optimization_goal,"
                        "destination_type,"
                        "is_incremental_attribution"
                        "_enabled,"
                        "attribution_spec,"
                        "promoted_object,"
                        "targeting"
                    ),
                },
            )
        except GraphAPIError as e:
            print(
                f"Adset not found for "
                f"adset_id={adset_id}: {e}"
            )
            continue

        targeting = resp.get("targeting") or {}
        result[adset_id] = {
            "daily_budget": normalize_budget_value(
                resp.get("daily_budget")
            ),
            "lifetime_budget": normalize_budget_value(
                resp.get("lifetime_budget")
            ),
            "budget_remaining": normalize_budget_value(
                resp.get("budget_remaining")
            ),
            "optimization_goal": resp.get(
                "optimization_goal"
            ),
            "destination_type": resp.get(
                "destination_type"
            ),
            "conversion_location": get_conversion_location(
                resp
            ),
            "is_incremental_attribution_enabled": (
                resp.get(
                    "is_incremental_attribution_enabled"
                )
            ),
            "attribution_setting": get_attribution_setting(
                resp
            ),
            "target_locations_json": (
                get_target_locations_json(targeting)
            ),
            "age_range": get_age_range(targeting),
            "gender": get_gender(targeting),
            "languages_json": get_languages_json(targeting),
            "placements_json": get_placements_json(
                targeting
            ),
        }

    return result


def get_campaign_details_for_campaigns(
    campaign_ids: list[str],
) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    empty = {
        "daily_budget": None,
        "lifetime_budget": None,
        "budget_remaining": None,
        "status": None,
        "effective_status": None,
        "start_time": None,
        "stop_time": None,
    }

    for cid in sorted(set(campaign_ids)):
        result[cid] = dict(empty)
        try:
            resp = graph_get(
                f"/{cid}",
                params={
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
                },
            )
        except GraphAPIError as e:
            print(
                f"Campaign not found for "
                f"campaign_id={cid}: {e}"
            )
            continue

        result[cid] = {
            "daily_budget": normalize_budget_value(
                resp.get("daily_budget")
            ),
            "lifetime_budget": normalize_budget_value(
                resp.get("lifetime_budget")
            ),
            "budget_remaining": normalize_budget_value(
                resp.get("budget_remaining")
            ),
            "status": resp.get("status"),
            "effective_status": resp.get(
                "effective_status"
            ),
            "start_time": format_meta_datetime_to_almaty(
                resp.get("start_time")
            ),
            "stop_time": format_meta_datetime_to_almaty(
                resp.get("stop_time")
            ),
        }

    return result


# ------------------------------------------------------------
# API calls — insights
# ------------------------------------------------------------

_INSIGHTS_COMMON_FIELDS = [
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

_GEO_DEVICE_FIELDS = [
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
    "ctr",
]


def _insights_params(
    date_since: str,
    date_until: str,
    fields: list[str],
    breakdowns: str | None = None,
) -> dict:
    params: dict = {
        "fields": ",".join(fields),
        "level": "ad",
        "time_increment": 1,
        "limit": 100,
        "time_range": json.dumps(
            {"since": date_since, "until": date_until}
        ),
    }
    if breakdowns:
        params["breakdowns"] = breakdowns
    return params


def get_ads_insights(
    date_since: str,
    date_until: str,
) -> dict:
    return graph_get_all_pages(
        f"/{config.AD_ACCOUNT_ID}/insights",
        _insights_params(
            date_since,
            date_until,
            _INSIGHTS_COMMON_FIELDS,
            breakdowns=HOURLY_BREAKDOWN_FIELD,
        ),
    )


def get_ads_insights_daily(
    date_since: str,
    date_until: str,
) -> dict:
    return graph_get_all_pages(
        f"/{config.AD_ACCOUNT_ID}/insights",
        _insights_params(
            date_since,
            date_until,
            _INSIGHTS_COMMON_FIELDS,
        ),
    )


def get_ads_insights_geo_daily(
    date_since: str,
    date_until: str,
) -> dict:
    return graph_get_all_pages(
        f"/{config.AD_ACCOUNT_ID}/insights",
        _insights_params(
            date_since,
            date_until,
            _GEO_DEVICE_FIELDS,
            breakdowns="country,region",
        ),
    )


def get_ads_insights_device_daily(
    date_since: str,
    date_until: str,
) -> dict:
    return graph_get_all_pages(
        f"/{config.AD_ACCOUNT_ID}/insights",
        _insights_params(
            date_since,
            date_until,
            _GEO_DEVICE_FIELDS,
            breakdowns="device_platform,impression_device",
        ),
    )


def get_ads_insights_gender_daily(
    date_since: str,
    date_until: str,
) -> dict:
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
        "ctr",
    ]
    return graph_get_all_pages(
        f"/{config.AD_ACCOUNT_ID}/insights",
        _insights_params(
            date_since,
            date_until,
            fields,
            breakdowns="age,gender",
        ),
    )
