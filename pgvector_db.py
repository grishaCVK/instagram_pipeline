import uuid
from typing import Any

import psycopg

import config


def get_connection():
    """
    Создаёт подключение к PostgreSQL + pgvector.
    """
    return psycopg.connect(
        host=config.POSTGRES_HOST,
        port=config.POSTGRES_PORT,
        dbname=config.POSTGRES_DB,
        user=config.POSTGRES_USER,
        password=config.POSTGRES_PASSWORD,
        connect_timeout=10,
    )


def embedding_to_pgvector(embedding: list[float]) -> str:
    """
    Преобразует Python list[float] в формат pgvector.

    Например:
    [0.1, 0.2, 0.3] -> "[0.1,0.2,0.3]"
    """
    return "[" + ",".join(str(value) for value in embedding) + "]"


def insert_ad_embedding(
    campaign_id: str | None,
    campaign_name: str | None,
    adset_id: str | None,
    adset_name: str | None,
    ad_id: str,
    ad_name: str | None,
    media_type: str | None,
    media_product_type: str | None,
    asset_position: int | None,
    embedding: list[float],
) -> None:
    """
    Сохраняет один embedding рекламного креатива в PostgreSQL.

    Одна строка = один embedding для изображения.
    Видео мы не обрабатываем.
    """
    if len(embedding) != 512:
        raise ValueError(
            f"Embedding must have 512 dimensions, got {len(embedding)}"
        )

    embedding_id = str(uuid.uuid4())
    embedding_vector = embedding_to_pgvector(embedding)

    query = """
        INSERT INTO ad_embeddings
        (
            embedding_id,
            campaign_id,
            campaign_name,
            adset_id,
            adset_name,
            ad_id,
            ad_name,
            media_type,
            media_product_type,
            asset_position,
            embedding
        )
        VALUES
        (
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s::vector
        )
    """

    values: tuple[Any, ...] = (
        embedding_id,
        campaign_id,
        campaign_name,
        adset_id,
        adset_name,
        ad_id,
        ad_name,
        media_type,
        media_product_type,
        asset_position,
        embedding_vector,
    )

    with get_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(query, values)


def test_connection() -> None:
    """
    Проверяет подключение к PostgreSQL.
    """
    with get_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1")
            result = cursor.fetchone()

    if result != (1,):
        raise RuntimeError("PostgreSQL connection test failed")


def delete_ad_embeddings_for_ads(ad_ids: list[str]) -> None:
    """
    Удаляет старые embeddings для указанных ad_id.

    Нужно, чтобы при повторном запуске pipeline
    не создавались дубли embeddings.
    """
    unique_ad_ids = sorted(set(ad_ids))

    if not unique_ad_ids:
        return

    placeholders = ", ".join(["%s"] * len(unique_ad_ids))

    query = f"""
        DELETE FROM ad_embeddings
        WHERE ad_id IN ({placeholders})
    """

    with get_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(query, tuple(unique_ad_ids))
