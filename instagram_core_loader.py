"""
instagram_core_loader.py

Загрузка данных из instagram_staging в instagram_core.
6 таблиц: daily_campaign, daily_ad, hourly_ad,
          geo_daily, device_daily, gender_daily
"""

from datetime import datetime
from zoneinfo import ZoneInfo

import clickhouse_connect

import config
import etl_logger as instagram_etl_logger

ALMATY_TZ = ZoneInfo("Asia/Almaty")
STAGING_DB = config.CLICKHOUSE_STAGING_DB
CORE_DB = config.CLICKHOUSE_CORE_DB

OBJECTIVES = [
    "awareness", "traffic", "engagement",
    "leads", "app_promotion", "sales",
]

HOURLY_STAGING_TABLES = [
    f"paid_ads_{obj}_hourly_ad_level_staging"
    for obj in OBJECTIVES
]

DAILY_STAGING_TABLES = [
    f"paid_ads_{obj}_daily_ad_level_staging"
    for obj in OBJECTIVES
]


def _get_client(database: str):
    return clickhouse_connect.get_client(
        host=config.CLICKHOUSE_HOST,
        port=config.CLICKHOUSE_PORT,
        username=config.CLICKHOUSE_USER,
        password=config.CLICKHOUSE_PASSWORD,
        database=database,
    )


def _count(
    client,
    database: str,
    table: str,
    date_since: str,
    date_until: str,
) -> int:
    sql = (
        f"SELECT count() FROM {database}.{table} "
        f"WHERE toDate(date_start) "
        f"BETWEEN toDate('{date_since}') "
        f"AND toDate('{date_until}')"
    )
    return client.query(sql).first_row[0]


def _delete(
    client,
    database: str,
    table: str,
    date_since: str,
    date_until: str,
) -> None:
    client.command(
        f"ALTER TABLE {database}.{table} DELETE "
        f"WHERE toDate(date_start) "
        f"BETWEEN toDate('{date_since}') "
        f"AND toDate('{date_until}')"
    )


def _union_daily_staging(
    select_sql: str,
    date_since: str,
    date_until: str,
) -> str:
    """UNION ALL всех 6 daily staging таблиц."""
    parts = []
    for table in DAILY_STAGING_TABLES:
        parts.append(
            select_sql.format(
                db=STAGING_DB,
                table=table,
                date_since=date_since,
                date_until=date_until,
            )
        )
    return "\nUNION ALL\n".join(parts)


# ============================================================
# 1. instagram_ads_core_daily_campaign_level
# Источник: daily_staging — GROUP BY date + campaign
# reach/frequency из daily API (корректны)
# cost_per_link_clicks = spend / inline_link_clicks
# ============================================================

def load_daily_campaign(
    *,
    run_id: str,
    date_since: str,
    date_until: str,
) -> int:
    table = "instagram_ads_core_daily_campaign_level"
    client = _get_client(CORE_DB)

    rows_before = _count(
        client, CORE_DB, table, date_since, date_until
    )
    _delete(client, CORE_DB, table, date_since, date_until)

    # Агрегация по кампании за день
    _inner_sql = (
        "SELECT"
        " toDate(date_start) AS d_start,"
        " campaign_id AS c_id,"
        " any(campaign_name) AS c_name,"
        " any(campaign_status) AS c_status,"
        " any(campaign_effective_status) AS c_eff,"
        " any(destination_url) AS dest_url,"
        " any(age_range) AS age_r,"
        " any(gender) AS gen,"
        " any(languages_json) AS lang,"
        " any(objective) AS obj,"
        " any(media_type) AS m_type,"
        " any(daily_budget) AS d_budget,"
        " any(lifetime_budget) AS l_budget,"
        " sum(spend) AS s_spend,"
        " sum(impressions) AS s_impr,"
        " max(reach) AS s_reach,"
        " avg(frequency) AS s_freq,"
        " sum(clicks) AS s_clicks,"
        " sum(inline_link_clicks) AS s_lclicks,"
        " avg(inline_link_click_ctr) AS s_lctr,"
        " avg(ctr) AS s_ctr,"
        " avg(cpc) AS s_cpc"
        " FROM {db}.{table}"
        " WHERE toDate(date_start)"
        " BETWEEN toDate('{date_since}')"
        " AND toDate('{date_until}')"
        " GROUP BY d_start, c_id"
    )

    union_sql = _union_daily_staging(
        _inner_sql, date_since, date_until
    )

    _outer_sql = (
        "SELECT"
        " d_start AS date_start,"
        " d_start + INTERVAL 1 DAY AS date_stop,"
        " c_id AS campaign_id,"
        " any(c_name) AS campaign_name,"
        " any(c_status) AS campaign_status,"
        " any(c_eff) AS campaign_effective_status,"
        " any(dest_url) AS destination_url,"
        " any(age_r) AS age_range,"
        " any(gen) AS gender,"
        " any(lang) AS languages,"
        " any(obj) AS objective,"
        " sum(s_spend) AS spend,"
        " sum(s_impr) AS impressions,"
        " max(s_reach) AS reach,"
        " avg(s_freq) AS frequency,"
        " if(sum(s_impr)>0,"
        "    sum(s_spend)/sum(s_impr)*1000, NULL) AS cpm,"
        " sum(s_clicks) AS clicks,"
        " sum(s_lclicks) AS link_clicks,"
        " avg(s_lctr) AS link_ctr,"
        " if(sum(s_lclicks)>0,"
        "    sum(s_spend)/sum(s_lclicks),"
        "    NULL) AS cost_per_link_clicks,"
        " avg(s_ctr) AS ctr,"
        " avg(s_cpc) AS cpc,"
        " any(m_type) AS media_type,"
        " any(d_budget) AS daily_budget,"
        " any(l_budget) AS lifetime_budget,"
        " now() AS loaded_at"
        " FROM (" + union_sql + ")"
        " GROUP BY d_start, c_id"
    )

    insert_sql = (
        f"INSERT INTO {CORE_DB}.{table} " + _outer_sql
    )
    client.command(insert_sql)

    rows_after = _count(
        client, CORE_DB, table, date_since, date_until
    )

    instagram_etl_logger.log_table_load(
        run_id=run_id,
        layer="core",
        database_name=CORE_DB,
        table_name=table,
        date_since=date_since,
        date_until=date_until,
        rows_before=rows_before,
        rows_deleted=rows_before,
        rows_inserted=rows_after,
        rows_after=rows_after,
        min_loaded_date=date_since,
        max_loaded_date=date_until,
    )

    instagram_etl_logger.run_quality_checks(
        run_id=run_id,
        database_name=CORE_DB,
        table_name=table,
        date_since=date_since,
        date_until=date_until,
        key_columns=["date_start", "campaign_id"],
    )

    print(
        f"[CORE] {table}: "
        f"before={rows_before}, after={rows_after}"
    )
    return rows_after


# ============================================================
# 2. instagram_ads_core_daily_ad_level
# Источник: daily_staging — один ряд на (date + ad)
# ============================================================

def load_daily_ad(
    *,
    run_id: str,
    date_since: str,
    date_until: str,
) -> int:
    table = "instagram_ads_core_daily_ad_level"
    client = _get_client(CORE_DB)

    rows_before = _count(
        client, CORE_DB, table, date_since, date_until
    )
    _delete(client, CORE_DB, table, date_since, date_until)

    _select = (
        "SELECT"
        " toDate(date_start) AS date_start,"
        " toDate(date_stop) AS date_stop,"
        " campaign_id,"
        " campaign_name,"
        " campaign_status,"
        " campaign_effective_status,"
        " adset_id,"
        " adset_name,"
        " ad_id,"
        " ad_name,"
        " destination_url,"
        " age_range,"
        " gender,"
        " languages_json AS languages,"
        " objective,"
        " spend,"
        " impressions,"
        " reach,"
        " frequency,"
        " if(impressions>0,"
        "    spend/impressions*1000, NULL) AS cpm,"
        " clicks,"
        " inline_link_clicks AS link_clicks,"
        " inline_link_click_ctr AS link_ctr,"
        " if(inline_link_clicks>0,"
        "    spend/inline_link_clicks,"
        "    NULL) AS cost_per_link_clicks,"
        " ctr,"
        " cpc,"
        " media_type,"
        " daily_budget,"
        " lifetime_budget,"
        " now() AS loaded_at"
        " FROM {db}.{table}"
        " WHERE toDate(date_start)"
        " BETWEEN toDate('{date_since}')"
        " AND toDate('{date_until}')"
    )

    union_sql = _union_daily_staging(
        _select, date_since, date_until
    )

    insert_sql = (
        f"INSERT INTO {CORE_DB}.{table} " + union_sql
    )
    client.command(insert_sql)

    rows_after = _count(
        client, CORE_DB, table, date_since, date_until
    )

    instagram_etl_logger.log_table_load(
        run_id=run_id,
        layer="core",
        database_name=CORE_DB,
        table_name=table,
        date_since=date_since,
        date_until=date_until,
        rows_before=rows_before,
        rows_deleted=rows_before,
        rows_inserted=rows_after,
        rows_after=rows_after,
        min_loaded_date=date_since,
        max_loaded_date=date_until,
    )

    instagram_etl_logger.run_quality_checks(
        run_id=run_id,
        database_name=CORE_DB,
        table_name=table,
        date_since=date_since,
        date_until=date_until,
        key_columns=["date_start", "campaign_id", "ad_id"],
    )

    print(
        f"[CORE] {table}: "
        f"before={rows_before}, after={rows_after}"
    )
    return rows_after


# ============================================================
# 3. instagram_ads_core_hourly_ad_level
# Источник: hourly_staging (paid_ads_*_hourly_ad_level_staging)
# Без reach/frequency
# ============================================================

def load_hourly_ad(
    *,
    run_id: str,
    date_since: str,
    date_until: str,
) -> int:
    table = "instagram_ads_core_hourly_ad_level"
    client = _get_client(CORE_DB)

    rows_before = _count(
        client, CORE_DB, table, date_since, date_until
    )
    _delete(client, CORE_DB, table, date_since, date_until)

    _select = (
        "SELECT"
        " date_start,"
        " date_stop,"
        " campaign_id,"
        " campaign_name,"
        " campaign_status,"
        " campaign_effective_status,"
        " adset_id,"
        " adset_name,"
        " ad_id,"
        " ad_name,"
        " destination_url,"
        " age_range,"
        " gender,"
        " languages_json AS languages,"
        " objective,"
        " spend,"
        " impressions,"
        " if(impressions>0,"
        "    spend/impressions*1000, NULL) AS cpm,"
        " clicks,"
        " inline_link_clicks AS link_clicks,"
        " inline_link_click_ctr AS link_ctr,"
        " if(inline_link_clicks>0,"
        "    spend/inline_link_clicks,"
        "    NULL) AS cost_per_link_clicks,"
        " ctr,"
        " cpc,"
        " media_type,"
        " daily_budget,"
        " lifetime_budget,"
        " now() AS loaded_at"
        " FROM {db}.{table}"
        " WHERE toDate(date_start)"
        " BETWEEN toDate('{date_since}')"
        " AND toDate('{date_until}')"
    )

    parts = []
    for t in HOURLY_STAGING_TABLES:
        parts.append(
            _select.format(
                db=STAGING_DB,
                table=t,
                date_since=date_since,
                date_until=date_until,
            )
        )
    union_sql = "\nUNION ALL\n".join(parts)

    insert_sql = (
        f"INSERT INTO {CORE_DB}.{table} " + union_sql
    )
    client.command(insert_sql)

    rows_after = _count(
        client, CORE_DB, table, date_since, date_until
    )

    instagram_etl_logger.log_table_load(
        run_id=run_id,
        layer="core",
        database_name=CORE_DB,
        table_name=table,
        date_since=date_since,
        date_until=date_until,
        rows_before=rows_before,
        rows_deleted=rows_before,
        rows_inserted=rows_after,
        rows_after=rows_after,
        min_loaded_date=date_since,
        max_loaded_date=date_until,
    )

    print(
        f"[CORE] {table}: "
        f"before={rows_before}, after={rows_after}"
    )
    return rows_after


# ============================================================
# 4. instagram_ads_core_geo_daily_level
# Источник: paid_ads_geo_daily_level_staging
# media_type — JOIN с daily_ad staging по ad_id + date
# ============================================================

def load_geo_daily(
    *,
    run_id: str,
    date_since: str,
    date_until: str,
) -> int:
    table = "instagram_ads_core_geo_daily_level"
    staging_geo = "paid_ads_geo_daily_level_staging"

    # Строим lookup media_type из daily_ad staging
    _daily_union = " UNION ALL ".join([
        f"SELECT toDate(date_start) AS ds, ad_id, "
        f"any(media_type) AS mt "
        f"FROM {STAGING_DB}.{t} "
        f"WHERE toDate(date_start) "
        f"BETWEEN toDate('{date_since}') "
        f"AND toDate('{date_until}') "
        f"GROUP BY ds, ad_id"
        for t in DAILY_STAGING_TABLES
    ])

    client = _get_client(CORE_DB)

    rows_before = _count(
        client, CORE_DB, table, date_since, date_until
    )
    _delete(client, CORE_DB, table, date_since, date_until)

    insert_sql = (
        f"INSERT INTO {CORE_DB}.{table} "
        f"SELECT"
        f" g.date_start AS date_start,"
        f" g.date_stop AS date_stop,"
        f" g.campaign_id AS campaign_id,"
        f" g.campaign_name AS campaign_name,"
        f" NULL AS campaign_status,"
        f" NULL AS campaign_effective_status,"
        f" g.adset_id AS adset_id,"
        f" g.adset_name AS adset_name,"
        f" g.ad_id AS ad_id,"
        f" g.ad_name AS ad_name,"
        f" g.objective AS objective,"
        f" g.country AS country,"
        f" g.region AS region,"
        f" g.spend AS spend,"
        f" g.impressions AS impressions,"
        f" g.reach AS reach,"
        f" g.frequency AS frequency,"
        f" if(g.impressions>0,"
        f"    g.spend/g.impressions*1000, NULL) AS cpm,"
        f" g.clicks AS clicks,"
        f" g.inline_link_clicks AS link_clicks,"
        f" g.ctr AS ctr,"
        f" if(g.inline_link_clicks>0,"
        f"    g.spend/g.inline_link_clicks,"
        f"    NULL) AS cost_per_link_clicks,"
        f" NULL AS link_ctr,"
        f" m.mt AS media_type,"
        f" NULL AS daily_budget,"
        f" NULL AS lifetime_budget,"
        f" now() AS loaded_at"
        f" FROM {STAGING_DB}.{staging_geo} g"
        f" LEFT JOIN ({_daily_union}) m"
        f" ON g.ad_id = m.ad_id"
        f" AND g.date_start = m.ds"
        f" WHERE g.date_start"
        f" BETWEEN toDate('{date_since}')"
        f" AND toDate('{date_until}')"
    )

    client.command(insert_sql)

    rows_after = _count(
        client, CORE_DB, table, date_since, date_until
    )

    instagram_etl_logger.log_table_load(
        run_id=run_id,
        layer="core",
        database_name=CORE_DB,
        table_name=table,
        date_since=date_since,
        date_until=date_until,
        rows_before=rows_before,
        rows_deleted=rows_before,
        rows_inserted=rows_after,
        rows_after=rows_after,
        min_loaded_date=date_since,
        max_loaded_date=date_until,
    )

    print(
        f"[CORE] {table}: "
        f"before={rows_before}, after={rows_after}"
    )
    return rows_after


# ============================================================
# 5. instagram_ads_core_device_daily_level
# Источник: paid_ads_device_daily_level_staging
# media_type — JOIN с daily_ad staging
# ============================================================

def load_device_daily(
    *,
    run_id: str,
    date_since: str,
    date_until: str,
) -> int:
    table = "instagram_ads_core_device_daily_level"
    staging_dev = "paid_ads_device_daily_level_staging"

    _daily_union = " UNION ALL ".join([
        f"SELECT toDate(date_start) AS ds, ad_id, "
        f"any(media_type) AS mt "
        f"FROM {STAGING_DB}.{t} "
        f"WHERE toDate(date_start) "
        f"BETWEEN toDate('{date_since}') "
        f"AND toDate('{date_until}') "
        f"GROUP BY ds, ad_id"
        for t in DAILY_STAGING_TABLES
    ])

    client = _get_client(CORE_DB)

    rows_before = _count(
        client, CORE_DB, table, date_since, date_until
    )
    _delete(client, CORE_DB, table, date_since, date_until)

    insert_sql = (
        f"INSERT INTO {CORE_DB}.{table} "
        f"SELECT"
        f" d.date_start AS date_start,"
        f" d.date_stop AS date_stop,"
        f" d.campaign_id AS campaign_id,"
        f" d.campaign_name AS campaign_name,"
        f" NULL AS campaign_status,"
        f" NULL AS campaign_effective_status,"
        f" d.adset_id AS adset_id,"
        f" d.adset_name AS adset_name,"
        f" d.ad_id AS ad_id,"
        f" d.ad_name AS ad_name,"
        f" d.objective AS objective,"
        f" d.device_platform AS device_platform,"
        f" d.impression_device AS impression_device,"
        f" d.device_type AS device_type,"
        f" d.os_type AS os_type,"
        f" d.spend AS spend,"
        f" d.impressions AS impressions,"
        f" d.reach AS reach,"
        f" d.frequency AS frequency,"
        f" if(d.impressions>0,"
        f"    d.spend/d.impressions*1000, NULL) AS cpm,"
        f" d.clicks AS clicks,"
        f" d.inline_link_clicks AS link_clicks,"
        f" d.ctr AS ctr,"
        f" if(d.inline_link_clicks>0,"
        f"    d.spend/d.inline_link_clicks,"
        f"    NULL) AS cost_per_link_clicks,"
        f" NULL AS link_ctr,"
        f" m.mt AS media_type,"
        f" NULL AS daily_budget,"
        f" NULL AS lifetime_budget,"
        f" now() AS loaded_at"
        f" FROM {STAGING_DB}.{staging_dev} d"
        f" LEFT JOIN ({_daily_union}) m"
        f" ON d.ad_id = m.ad_id"
        f" AND d.date_start = m.ds"
        f" WHERE d.date_start"
        f" BETWEEN toDate('{date_since}')"
        f" AND toDate('{date_until}')"
    )

    client.command(insert_sql)

    rows_after = _count(
        client, CORE_DB, table, date_since, date_until
    )

    instagram_etl_logger.log_table_load(
        run_id=run_id,
        layer="core",
        database_name=CORE_DB,
        table_name=table,
        date_since=date_since,
        date_until=date_until,
        rows_before=rows_before,
        rows_deleted=rows_before,
        rows_inserted=rows_after,
        rows_after=rows_after,
        min_loaded_date=date_since,
        max_loaded_date=date_until,
    )

    print(
        f"[CORE] {table}: "
        f"before={rows_before}, after={rows_after}"
    )
    return rows_after


# ============================================================
# 6. instagram_ads_core_gender_daily_level
# Источник: paid_ads_gender_daily_level_staging
# media_type — JOIN с daily_ad staging
# ============================================================

def load_gender_daily(
    *,
    run_id: str,
    date_since: str,
    date_until: str,
) -> int:
    table = "instagram_ads_core_gender_daily_level"
    staging_gen = "paid_ads_gender_daily_level_staging"

    _daily_union = " UNION ALL ".join([
        f"SELECT toDate(date_start) AS ds, ad_id, "
        f"any(media_type) AS mt "
        f"FROM {STAGING_DB}.{t} "
        f"WHERE toDate(date_start) "
        f"BETWEEN toDate('{date_since}') "
        f"AND toDate('{date_until}') "
        f"GROUP BY ds, ad_id"
        for t in DAILY_STAGING_TABLES
    ])

    client = _get_client(CORE_DB)

    rows_before = _count(
        client, CORE_DB, table, date_since, date_until
    )
    _delete(client, CORE_DB, table, date_since, date_until)

    insert_sql = (
        f"INSERT INTO {CORE_DB}.{table} "
        f"SELECT"
        f" g.date_start AS date_start,"
        f" g.date_stop AS date_stop,"
        f" g.campaign_id AS campaign_id,"
        f" g.campaign_name AS campaign_name,"
        f" NULL AS campaign_status,"
        f" NULL AS campaign_effective_status,"
        f" g.adset_id AS adset_id,"
        f" g.adset_name AS adset_name,"
        f" g.ad_id AS ad_id,"
        f" g.ad_name AS ad_name,"
        f" g.objective AS objective,"
        f" g.gender_type AS gender_type,"
        f" g.spend AS spend,"
        f" g.impressions AS impressions,"
        f" g.reach AS reach,"
        f" g.frequency AS frequency,"
        f" if(g.impressions>0,"
        f"    g.spend/g.impressions*1000, NULL) AS cpm,"
        f" g.clicks AS clicks,"
        f" g.inline_link_clicks AS link_clicks,"
        f" g.ctr AS ctr,"
        f" if(g.inline_link_clicks>0,"
        f"    g.spend/g.inline_link_clicks,"
        f"    NULL) AS cost_per_link_clicks,"
        f" NULL AS link_ctr,"
        f" m.mt AS media_type,"
        f" NULL AS daily_budget,"
        f" NULL AS lifetime_budget,"
        f" now() AS loaded_at"
        f" FROM {STAGING_DB}.{staging_gen} g"
        f" LEFT JOIN ({_daily_union}) m"
        f" ON g.ad_id = m.ad_id"
        f" AND g.date_start = m.ds"
        f" WHERE g.date_start"
        f" BETWEEN toDate('{date_since}')"
        f" AND toDate('{date_until}')"
    )

    client.command(insert_sql)

    rows_after = _count(
        client, CORE_DB, table, date_since, date_until
    )

    instagram_etl_logger.log_table_load(
        run_id=run_id,
        layer="core",
        database_name=CORE_DB,
        table_name=table,
        date_since=date_since,
        date_until=date_until,
        rows_before=rows_before,
        rows_deleted=rows_before,
        rows_inserted=rows_after,
        rows_after=rows_after,
        min_loaded_date=date_since,
        max_loaded_date=date_until,
    )

    print(
        f"[CORE] {table}: "
        f"before={rows_before}, after={rows_after}"
    )
    return rows_after


# ============================================================
# Главная функция
# ============================================================

def run_staging_to_core(
    *,
    run_id: str,
    date_since: str,
    date_until: str,
) -> int:
    """Запускает все core загрузки."""
    total = 0

    with instagram_etl_logger.etl_step(
        run_id=run_id,
        step_name="staging_to_core",
        step_order=3,
        target_database=CORE_DB,
    ) as step:

        total += load_daily_campaign(
            run_id=run_id,
            date_since=date_since,
            date_until=date_until,
        )
        total += load_daily_ad(
            run_id=run_id,
            date_since=date_since,
            date_until=date_until,
        )
        total += load_hourly_ad(
            run_id=run_id,
            date_since=date_since,
            date_until=date_until,
        )
        total += load_geo_daily(
            run_id=run_id,
            date_since=date_since,
            date_until=date_until,
        )
        total += load_device_daily(
            run_id=run_id,
            date_since=date_since,
            date_until=date_until,
        )
        total += load_gender_daily(
            run_id=run_id,
            date_since=date_since,
            date_until=date_until,
        )

        step["output_rows"] = total

    return total
