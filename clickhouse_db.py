import json
import uuid
from datetime import datetime
from typing import Any

import clickhouse_connect

import config


# ------------------------------------------------------------
# DB clients
# ------------------------------------------------------------

def _get_raw_client():
    return clickhouse_connect.get_client(
        host=config.CLICKHOUSE_HOST,
        port=config.CLICKHOUSE_PORT,
        username=config.CLICKHOUSE_USER,
        password=config.CLICKHOUSE_PASSWORD,
        database=config.CLICKHOUSE_RAW_DB,
    )


def _get_staging_client():
    return clickhouse_connect.get_client(
        host=config.CLICKHOUSE_HOST,
        port=config.CLICKHOUSE_PORT,
        username=config.CLICKHOUSE_USER,
        password=config.CLICKHOUSE_PASSWORD,
        database=config.CLICKHOUSE_STAGING_DB,
    )


# ------------------------------------------------------------
# Objective → table key mapping (single source of truth)
# ------------------------------------------------------------

_OBJECTIVE_MAP: dict[str, list[str]] = {
    "awareness": [
        "OUTCOME_AWARENESS", "AWARENESS",
        "REACH", "BRAND_AWARENESS",
    ],
    "traffic": [
        "OUTCOME_TRAFFIC", "TRAFFIC", "LINK_CLICKS",
    ],
    "engagement": [
        "OUTCOME_ENGAGEMENT", "ENGAGEMENT",
        "POST_ENGAGEMENT", "VIDEO_VIEWS",
    ],
    "leads": [
        "OUTCOME_LEADS", "LEADS", "LEAD_GENERATION",
    ],
    "app_promotion": [
        "OUTCOME_APP_PROMOTION",
        "APP_PROMOTION", "APP_INSTALLS",
    ],
    "sales": [
        "OUTCOME_SALES", "SALES",
        "CONVERSIONS", "PRODUCT_CATALOG_SALES",
    ],
}

OBJECTIVES: list[str] = list(_OBJECTIVE_MAP.keys())

_OBJ_LOOKUP: dict[str, str] = {
    v: obj
    for obj, values in _OBJECTIVE_MAP.items()
    for v in values
}


def get_objective_key(objective: str) -> str | None:
    return _OBJ_LOOKUP.get(objective.upper())


# ------------------------------------------------------------
# Table name constants
# ------------------------------------------------------------

_P = "instagram_ads_"
_S = "_staging"

HOURLY_TABLES: dict[str, str] = {
    obj: f"{_P}{obj}_hourly_ad_level{_S}"
    for obj in OBJECTIVES
}
DAILY_TABLES: dict[str, str] = {
    obj: f"{_P}{obj}_daily_ad_level{_S}"
    for obj in OBJECTIVES
}

GEO_TABLE = f"{_P}geo_daily_level{_S}"
DEVICE_TABLE = f"{_P}device_daily_level{_S}"
GENDER_TABLE = f"{_P}gender_daily_level{_S}"


def get_hourly_table(objective: str) -> str | None:
    key = get_objective_key(objective)
    return HOURLY_TABLES.get(key) if key else None


def get_daily_table(objective: str) -> str | None:
    key = get_objective_key(objective)
    return DAILY_TABLES.get(key) if key else None


# ------------------------------------------------------------
# Column definitions
# ------------------------------------------------------------

_BASE: list[str] = [
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
    "messaging_conversation_started",
    "cost_per_messaging_conversation_started",
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
]

_DAILY_EXTRA: list[str] = ["reach", "frequency"]

_OBJECTIVE_TAIL: dict[str, list[str]] = {
    "awareness": [],
    "traffic": [
        "landing_page_view",
        "cost_per_landing_page_view",
    ],
    "engagement": [
        "comments_count",
        "likes_count",
        "saved",
        "shares",
        "post_engagement",
        "cost_per_post_engagement",
    ],
    "leads": [
        "profile_visits",
        "leads",
        "cost_per_lead",
    ],
    "app_promotion": [
        "mobile_app_install",
        "cost_per_mobile_app_install",
        "mobile_app_registration",
        "mobile_app_purchase",
    ],
    "sales": [
        "purchase",
        "cost_per_purchase",
        "add_to_cart",
        "cost_per_add_to_cart",
        "initiate_checkout",
        "cost_per_initiate_checkout",
        "view_content",
        "cost_per_view_content",
    ],
}

HOURLY_COLUMNS: dict[str, list[str]] = {
    HOURLY_TABLES[obj]: (
        _BASE + _OBJECTIVE_TAIL[obj] + ["loaded_at"]
    )
    for obj in OBJECTIVES
}

DAILY_COLUMNS: dict[str, list[str]] = {
    DAILY_TABLES[obj]: (
        _BASE
        + _DAILY_EXTRA
        + _OBJECTIVE_TAIL[obj]
        + ["loaded_at"]
    )
    for obj in OBJECTIVES
}

GEO_COLUMNS: list[str] = [
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

DEVICE_COLUMNS: list[str] = [
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

GENDER_COLUMNS: list[str] = [
    "date_start",
    "date_stop",
    "campaign_id",
    "campaign_name",
    "adset_id",
    "adset_name",
    "ad_id",
    "ad_name",
    "objective",
    "age_range",
    "gender_type",
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


# ------------------------------------------------------------
# Raw layer
# ------------------------------------------------------------

def insert_raw_data(
    source: str,
    api_type: str,
    endpoint: str,
    object_id: str | None,
    response_data: dict[str, Any],
    request_params: dict[str, Any],
) -> None:
    client = _get_raw_client()
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


# ------------------------------------------------------------
# Raw layer — read
# ------------------------------------------------------------

def read_raw(
    source: str,
    date_since: str,
    date_until: str,
) -> list[dict[str, Any]]:
    """
    Читает последний сохранённый ответ API из raw_data
    за указанный период данных.

    Фильтр идёт по периоду данных, который хранится в
    request_params (date_since / date_until), а НЕ по
    fetched_at — fetched_at это момент выгрузки, он не
    совпадает с периодом данных.
    """
    client = _get_raw_client()
    result = client.query(
        "SELECT response_json"
        " FROM raw_data"
        " WHERE source = %(source)s"
        " AND JSONExtractString("
        "     request_params, 'date_since') = %(since)s"
        " AND JSONExtractString("
        "     request_params, 'date_until') = %(until)s"
        " ORDER BY fetched_at DESC"
        " LIMIT 1",
        parameters={
            "source": source,
            "since": date_since,
            "until": date_until,
        },
    )
    if not result.result_rows:
        return []
    response_json = result.result_rows[0][0]
    data = json.loads(response_json)
    return data.get("data", [])


def read_staging_unique_ads(
    date_since: str | None = None,
    date_until: str | None = None,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    """
    Возвращает уникальные объявления из hourly staging
    таблиц за период (для построения эмбеддингов).

    Дедупликация по (campaign_id, adset_id, ad_id),
    имена берутся через any().
    """
    client = _get_staging_client()

    selects: list[str] = []
    for table_name in HOURLY_TABLES.values():
        where_parts = [
            "ad_id IS NOT NULL",
            "ad_id != ''",
        ]
        if date_since:
            where_parts.append(
                "toDate(date_start) >= "
                f"toDate('{date_since}')"
            )
        if date_until:
            where_parts.append(
                "toDate(date_start) <= "
                f"toDate('{date_until}')"
            )
        selects.append(
            "SELECT"
            " campaign_id,"
            " campaign_name,"
            " adset_id,"
            " adset_name,"
            " ad_id,"
            " ad_name"
            f" FROM {table_name}"
            f" WHERE {' AND '.join(where_parts)}"
        )

    limit_sql = f"LIMIT {int(limit)}" if limit else ""

    query = (
        "SELECT"
        " campaign_id,"
        " any(campaign_name) AS campaign_name,"
        " adset_id,"
        " any(adset_name) AS adset_name,"
        " ad_id,"
        " any(ad_name) AS ad_name"
        " FROM ("
        + " UNION ALL ".join(selects)
        + ")"
        " GROUP BY campaign_id, adset_id, ad_id"
        " ORDER BY campaign_id, adset_id, ad_id"
        f" {limit_sql}"
    )

    result = client.query(query)
    return [
        dict(zip(result.column_names, row))
        for row in result.result_rows
    ]


# ------------------------------------------------------------
# Staging layer — write helpers
# ------------------------------------------------------------

_ALMATY = "Asia/Almaty"


def _delete_hourly_range(
    client,
    db: str,
    table: str,
    date_since: str,
    date_until: str,
) -> None:
    client.command(
        f"ALTER TABLE {db}.{table} DELETE "
        f"WHERE date_start >= toDateTime("
        f"'{date_since}', '{_ALMATY}') "
        f"AND date_start < addDays(toDateTime("
        f"'{date_until}', '{_ALMATY}'), 1)",
        settings={"mutations_sync": 1},
    )


def _delete_daily_range(
    client,
    db: str,
    table: str,
    date_since: str,
    date_until: str,
) -> None:
    client.command(
        f"ALTER TABLE {db}.{table} DELETE "
        f"WHERE date_start >= toDate('{date_since}') "
        f"AND date_start <= toDate('{date_until}')",
        settings={"mutations_sync": 1},
    )


# ------------------------------------------------------------
# Staging layer — public write functions
# ------------------------------------------------------------

def write_hourly_staging(
    grouped: dict[str, list[list]],
    date_since: str,
    date_until: str,
) -> int:
    client = _get_staging_client()
    db = config.CLICKHOUSE_STAGING_DB
    total = 0
    for table, rows in grouped.items():
        _delete_hourly_range(
            client, db, table, date_since, date_until
        )
        if rows:
            client.insert(
                table,
                rows,
                column_names=HOURLY_COLUMNS[table],
            )
        print(f"{table}: inserted {len(rows)} rows")
        total += len(rows)
    return total


def write_daily_staging(
    grouped: dict[str, list[list]],
    date_since: str,
    date_until: str,
) -> int:
    client = _get_staging_client()
    db = config.CLICKHOUSE_STAGING_DB
    total = 0
    for table, rows in grouped.items():
        _delete_daily_range(
            client, db, table, date_since, date_until
        )
        if rows:
            client.insert(
                table,
                rows,
                column_names=DAILY_COLUMNS[table],
            )
        print(f"{table}: inserted {len(rows)} rows")
        total += len(rows)
    return total


def write_geo_staging(
    rows: list[list],
    date_since: str,
    date_until: str,
) -> int:
    client = _get_staging_client()
    db = config.CLICKHOUSE_STAGING_DB
    _delete_daily_range(
        client, db, GEO_TABLE, date_since, date_until
    )
    if rows:
        client.insert(
            GEO_TABLE,
            rows,
            column_names=GEO_COLUMNS,
        )
    print(f"{GEO_TABLE}: inserted {len(rows)} rows")
    return len(rows)


def write_device_staging(
    rows: list[list],
    date_since: str,
    date_until: str,
) -> int:
    client = _get_staging_client()
    db = config.CLICKHOUSE_STAGING_DB
    _delete_daily_range(
        client, db, DEVICE_TABLE, date_since, date_until
    )
    if rows:
        client.insert(
            DEVICE_TABLE,
            rows,
            column_names=DEVICE_COLUMNS,
        )
    print(f"{DEVICE_TABLE}: inserted {len(rows)} rows")
    return len(rows)


def write_gender_staging(
    rows: list[list],
    date_since: str,
    date_until: str,
) -> int:
    client = _get_staging_client()
    db = config.CLICKHOUSE_STAGING_DB
    _delete_daily_range(
        client, db, GENDER_TABLE, date_since, date_until
    )
    if rows:
        client.insert(
            GENDER_TABLE,
            rows,
            column_names=GENDER_COLUMNS,
        )
    print(f"{GENDER_TABLE}: inserted {len(rows)} rows")
    return len(rows)
