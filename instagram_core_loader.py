"""
instagram_core_loader.py

Загрузка данных из instagram_staging в instagram_core.
6 таблиц: daily_campaign, daily_ad, hourly_ad,
          geo_daily, device_daily, gender_daily
"""

import clickhouse_connect

import config
import etl_logger as instagram_etl_logger
from clickhouse_db import (
    DAILY_TABLES,
    DEVICE_TABLE,
    GEO_TABLE,
    GENDER_TABLE,
    HOURLY_TABLES,
)

STAGING_DB = config.CLICKHOUSE_STAGING_DB
CORE_DB = config.CLICKHOUSE_CORE_DB

HOURLY_STAGING_TABLES = list(HOURLY_TABLES.values())
DAILY_STAGING_TABLES = list(DAILY_TABLES.values())


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
    parts = [
        select_sql.format(
            db=STAGING_DB,
            table=table,
            date_since=date_since,
            date_until=date_until,
        )
        for table in DAILY_STAGING_TABLES
    ]
    return "\nUNION ALL\n".join(parts)


# ============================================================
# 1. instagram_ads_core_daily_campaign_level
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

    _inner = (
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
        _inner, date_since, date_until
    )

    _outer = (
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
        "    sum(s_spend)/sum(s_impr)*1000,"
        "    NULL) AS cpm,"
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

    client.command(
        f"INSERT INTO {CORE_DB}.{table} " + _outer
    )

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
        "    spend/impressions*1000,"
        "    NULL) AS cpm,"
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
    client.command(
        f"INSERT INTO {CORE_DB}.{table} " + union_sql
    )

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
        "    spend/impressions*1000,"
        "    NULL) AS cpm,"
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

    parts = [
        _select.format(
            db=STAGING_DB,
            table=t,
            date_since=date_since,
            date_until=date_until,
        )
        for t in HOURLY_STAGING_TABLES
    ]
    union_sql = "\nUNION ALL\n".join(parts)
    client.command(
        f"INSERT INTO {CORE_DB}.{table} " + union_sql
    )

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
# Helper: daily staging media_type lookup
# ============================================================

def _daily_media_lookup(
    date_since: str,
    date_until: str,
) -> str:
    parts = [
        f"SELECT toDate(date_start) AS ds, "
        f"ad_id, any(media_type) AS mt "
        f"FROM {STAGING_DB}.{t} "
        f"WHERE toDate(date_start) "
        f"BETWEEN toDate('{date_since}') "
        f"AND toDate('{date_until}') "
        f"GROUP BY ds, ad_id"
        for t in DAILY_STAGING_TABLES
    ]
    return " UNION ALL ".join(parts)


# ============================================================
# 4. instagram_ads_core_geo_daily_level
# ============================================================

def load_geo_daily(
    *,
    run_id: str,
    date_since: str,
    date_until: str,
) -> int:
    table = "instagram_ads_core_geo_daily_level"
    client = _get_client(CORE_DB)

    rows_before = _count(
        client, CORE_DB, table, date_since, date_until
    )
    _delete(client, CORE_DB, table, date_since, date_until)

    lookup = _daily_media_lookup(date_since, date_until)

    insert_sql = (
        f"INSERT INTO {CORE_DB}.{table} "
        f"SELECT"
        f" g.date_start,"
        f" g.date_stop,"
        f" g.campaign_id,"
        f" g.campaign_name,"
        f" NULL AS campaign_status,"
        f" NULL AS campaign_effective_status,"
        f" g.adset_id,"
        f" g.adset_name,"
        f" g.ad_id,"
        f" g.ad_name,"
        f" g.objective,"
        f" g.country,"
        f" g.region,"
        f" g.spend,"
        f" g.impressions,"
        f" g.reach,"
        f" g.frequency,"
        f" if(g.impressions>0,"
        f"    g.spend/g.impressions*1000,"
        f"    NULL) AS cpm,"
        f" g.clicks,"
        f" g.inline_link_clicks AS link_clicks,"
        f" g.ctr,"
        f" if(g.inline_link_clicks>0,"
        f"    g.spend/g.inline_link_clicks,"
        f"    NULL) AS cost_per_link_clicks,"
        f" NULL AS link_ctr,"
        f" m.mt AS media_type,"
        f" NULL AS daily_budget,"
        f" NULL AS lifetime_budget,"
        f" now() AS loaded_at"
        f" FROM {STAGING_DB}.{GEO_TABLE} g"
        f" LEFT JOIN ({lookup}) m"
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
# ============================================================

def load_device_daily(
    *,
    run_id: str,
    date_since: str,
    date_until: str,
) -> int:
    table = "instagram_ads_core_device_daily_level"
    client = _get_client(CORE_DB)

    rows_before = _count(
        client, CORE_DB, table, date_since, date_until
    )
    _delete(client, CORE_DB, table, date_since, date_until)

    lookup = _daily_media_lookup(date_since, date_until)

    insert_sql = (
        f"INSERT INTO {CORE_DB}.{table} "
        f"SELECT"
        f" d.date_start,"
        f" d.date_stop,"
        f" d.campaign_id,"
        f" d.campaign_name,"
        f" NULL AS campaign_status,"
        f" NULL AS campaign_effective_status,"
        f" d.adset_id,"
        f" d.adset_name,"
        f" d.ad_id,"
        f" d.ad_name,"
        f" d.objective,"
        f" d.device_platform,"
        f" d.impression_device,"
        f" d.device_type,"
        f" d.os_type,"
        f" d.spend,"
        f" d.impressions,"
        f" d.reach,"
        f" d.frequency,"
        f" if(d.impressions>0,"
        f"    d.spend/d.impressions*1000,"
        f"    NULL) AS cpm,"
        f" d.clicks,"
        f" d.inline_link_clicks AS link_clicks,"
        f" d.ctr,"
        f" if(d.inline_link_clicks>0,"
        f"    d.spend/d.inline_link_clicks,"
        f"    NULL) AS cost_per_link_clicks,"
        f" NULL AS link_ctr,"
        f" m.mt AS media_type,"
        f" NULL AS daily_budget,"
        f" NULL AS lifetime_budget,"
        f" now() AS loaded_at"
        f" FROM {STAGING_DB}.{DEVICE_TABLE} d"
        f" LEFT JOIN ({lookup}) m"
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
# ============================================================

def load_gender_daily(
    *,
    run_id: str,
    date_since: str,
    date_until: str,
) -> int:
    table = "instagram_ads_core_gender_daily_level"
    client = _get_client(CORE_DB)

    rows_before = _count(
        client, CORE_DB, table, date_since, date_until
    )
    _delete(client, CORE_DB, table, date_since, date_until)

    lookup = _daily_media_lookup(date_since, date_until)

    insert_sql = (
        f"INSERT INTO {CORE_DB}.{table} "
        f"SELECT"
        f" g.date_start,"
        f" g.date_stop,"
        f" g.campaign_id,"
        f" g.campaign_name,"
        f" NULL AS campaign_status,"
        f" NULL AS campaign_effective_status,"
        f" g.adset_id,"
        f" g.adset_name,"
        f" g.ad_id,"
        f" g.ad_name,"
        f" g.objective,"
        f" g.gender_type,"
        f" g.spend,"
        f" g.impressions,"
        f" g.reach,"
        f" g.frequency,"
        f" if(g.impressions>0,"
        f"    g.spend/g.impressions*1000,"
        f"    NULL) AS cpm,"
        f" g.clicks,"
        f" g.inline_link_clicks AS link_clicks,"
        f" g.ctr,"
        f" if(g.inline_link_clicks>0,"
        f"    g.spend/g.inline_link_clicks,"
        f"    NULL) AS cost_per_link_clicks,"
        f" NULL AS link_ctr,"
        f" m.mt AS media_type,"
        f" NULL AS daily_budget,"
        f" NULL AS lifetime_budget,"
        f" now() AS loaded_at"
        f" FROM {STAGING_DB}.{GENDER_TABLE} g"
        f" LEFT JOIN ({lookup}) m"
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
# Entry point
# ============================================================

def run_staging_to_core(
    *,
    run_id: str,
    date_since: str,
    date_until: str,
) -> int:
    total = 0
    with instagram_etl_logger.etl_step(
        run_id=run_id,
        step_name="staging_to_core",
        step_order=3,
        target_database=CORE_DB,
    ) as step:
        for loader in (
            load_daily_campaign,
            load_daily_ad,
            load_hourly_ad,
            load_geo_daily,
            load_device_daily,
            load_gender_daily,
        ):
            total += loader(
                run_id=run_id,
                date_since=date_since,
                date_until=date_until,
            )
        step["output_rows"] = total
    return total
