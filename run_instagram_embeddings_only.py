"""
run_instagram_embeddings_only.py

Отдельный запуск построения image embeddings без
основного ETL. Читает уникальные объявления из
instagram_staging и строит эмбеддинги в pgvector.

Управление через env:
- INSTAGRAM_EMBEDDINGS_START_DATE
- INSTAGRAM_EMBEDDINGS_END_DATE
- INSTAGRAM_EMBEDDINGS_LIMIT
"""

import os

import clickhouse_db
import embeddings


def main() -> None:
    date_since = os.getenv(
        "INSTAGRAM_EMBEDDINGS_START_DATE"
    )
    date_until = os.getenv(
        "INSTAGRAM_EMBEDDINGS_END_DATE"
    )
    limit_value = os.getenv("INSTAGRAM_EMBEDDINGS_LIMIT")
    limit = int(limit_value) if limit_value else None

    print("Instagram embeddings only started")
    print(f"date_since={date_since or 'ALL'}")
    print(f"date_until={date_until or 'ALL'}")
    print(f"limit={limit or 'ALL'}")

    ads = clickhouse_db.read_staging_unique_ads(
        date_since=date_since,
        date_until=date_until,
        limit=limit,
    )

    print(f"Fetched unique ads: {len(ads)}")

    if not ads:
        print("No ads found for embeddings")
        return

    embeddings.process_ads_insights_image_embeddings(ads)
    print("Instagram embeddings only finished")


if __name__ == "__main__":
    main()
