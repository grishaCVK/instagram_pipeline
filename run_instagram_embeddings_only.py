import os
from typing import Any

import clickhouse_db
import embeddings


PAID_AD_TABLES = [
    "paid_ads_awareness_hourly_ad_level",
    "paid_ads_traffic_hourly_ad_level",
    "paid_ads_engagement_hourly_ad_level",
    "paid_ads_leads_hourly_ad_level",
    "paid_ads_app_promotion_hourly_ad_level",
    "paid_ads_sales_hourly_ad_level",
]


def fetch_unique_ads_from_clickhouse(
    *,
    date_since: str | None = None,
    date_until: str | None = None,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    client = clickhouse_db.get_client()

    selects: list[str] = []

    for table_name in PAID_AD_TABLES:
        where_parts = [
            "ad_id IS NOT NULL",
            "ad_id != ''",
        ]

        if date_since:
            where_parts.append(
                f"toDate(date_start) >= toDate('{date_since}')"
            )

        if date_until:
            where_parts.append(
                f"toDate(date_start) <= toDate('{date_until}')"
            )

        selects.append(
            f"""
            SELECT
                campaign_id,
                campaign_name,
                adset_id,
                adset_name,
                ad_id,
                ad_name
            FROM instagram_ads.{table_name}
            WHERE {" AND ".join(where_parts)}
            """
        )

    limit_sql = f"LIMIT {int(limit)}" if limit else ""

    query = f"""
    SELECT
        campaign_id,
        any(campaign_name) AS campaign_name,
        adset_id,
        any(adset_name) AS adset_name,
        ad_id,
        any(ad_name) AS ad_name
    FROM
    (
        {" UNION ALL ".join(selects)}
    )
    GROUP BY
        campaign_id,
        adset_id,
        ad_id
    ORDER BY
        campaign_id,
        adset_id,
        ad_id
    {limit_sql}
    """

    result = client.query(query)

    return [
        dict(zip(result.column_names, row))
        for row in result.result_rows
    ]


def main() -> None:
    date_since = os.getenv("INSTAGRAM_EMBEDDINGS_START_DATE")
    date_until = os.getenv("INSTAGRAM_EMBEDDINGS_END_DATE")
    limit_value = os.getenv("INSTAGRAM_EMBEDDINGS_LIMIT")
    limit = int(limit_value) if limit_value else None

    print("Instagram embeddings only started")
    print(f"date_since={date_since or 'ALL'}")
    print(f"date_until={date_until or 'ALL'}")
    print(f"limit={limit or 'ALL'}")

    ads = fetch_unique_ads_from_clickhouse(
        date_since=date_since,
        date_until=date_until,
        limit=limit,
    )

    print(f"Fetched unique ads from ClickHouse: {len(ads)}")

    if not ads:
        print("No ads found for embeddings")
        return

    embeddings.process_ads_insights_image_embeddings(ads)

    print("Instagram embeddings only finished")


if __name__ == "__main__":
    main()
