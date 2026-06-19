import os
import tempfile
from typing import Any

import requests
from PIL import Image
from sentence_transformers import SentenceTransformer

import graph_api
import pgvector_db


MODEL_NAME = "sentence-transformers/clip-ViT-B-32"

_model: SentenceTransformer | None = None


def get_model() -> SentenceTransformer:
    """
    Загружает локальную модель для image embeddings.

    Модель загружается один раз и потом переиспользуется.
    Первый запуск может быть долгим, потому что модель
    скачивается локально.
    """
    global _model

    if _model is None:
        _model = SentenceTransformer(MODEL_NAME)

    return _model


def download_image(media_url: str) -> str:
    """
    Скачивает изображение во временный файл.

    Файл нужен только на время построения embedding.
    После обработки он удаляется.
    """
    response = requests.get(
        media_url,
        timeout=(10, 90),
    )
    response.raise_for_status()

    temp_file = tempfile.NamedTemporaryFile(
        delete=False,
        suffix=".jpg",
    )

    try:
        temp_file.write(response.content)
        return temp_file.name
    finally:
        temp_file.close()


def delete_file(file_path: str | None) -> None:
    """
    Удаляет временный файл, если он существует.
    """
    if not file_path:
        return

    if os.path.exists(file_path):
        os.remove(file_path)


def create_image_embedding(image_path: str) -> list[float]:
    """
    Строит реальный embedding изображения через CLIP.

    Возвращает list[float] размерности 512.
    """
    model = get_model()

    image = Image.open(image_path).convert("RGB")

    embedding = model.encode(
        image,
        convert_to_numpy=True,
        normalize_embeddings=True,
    )

    embedding_list = embedding.tolist()

    if len(embedding_list) != 512:
        raise ValueError(
            f"Expected embedding dimension 512, got {len(embedding_list)}"
        )

    return embedding_list


def get_image_assets_for_ad(ad_id: str) -> list[dict[str, Any]]:
    """
    Получает визуальные assets для одного объявления.

    IMAGE:
    - embedding строим по media_url
    - image_url = media_url
    - video_url = None

    VIDEO:
    - embedding строим по thumbnail_url
    - image_url = thumbnail_url
    - video_url = media_url

    CAROUSEL_ALBUM:
    - IMAGE children обрабатываем как image
    - VIDEO children обрабатываем по thumbnail_url
    """
    creative_response = graph_api.get_ad_creative(ad_id)
    creative = creative_response.get("creative") or {}

    ig_media_id = creative.get("effective_instagram_media_id")

    if not ig_media_id:
        return []

    media_response = graph_api.get_ig_media_info(ig_media_id)

    media_type = media_response.get("media_type")
    media_product_type = media_response.get("media_product_type")

    image_assets = []

    if media_type == "IMAGE":
        media_url = media_response.get("media_url")

        if media_url:
            image_assets.append(
                {
                    "media_url": media_url,
                    "image_url": media_url,
                    "video_url": None,
                    "media_type": "IMAGE",
                    "media_product_type": media_product_type,
                    "asset_position": 1,
                    "frame_percent": None,
                }
            )

        return image_assets

    if media_type == "VIDEO":
        video_url = media_response.get("media_url")
        thumbnail_url = media_response.get("thumbnail_url")

        if thumbnail_url:
            image_assets.append(
                {
                    "media_url": thumbnail_url,
                    "image_url": thumbnail_url,
                    "video_url": video_url,
                    "media_type": "VIDEO",
                    "media_product_type": media_product_type,
                    "asset_position": 1,
                    "frame_percent": None,
                }
            )

        return image_assets

    if media_type == "CAROUSEL_ALBUM":
        children = media_response.get("children", {})
        children_data = children.get("data", [])

        for position, child in enumerate(children_data, start=1):
            child_media_type = child.get("media_type")
            child_media_url = child.get("media_url")
            child_thumbnail_url = child.get("thumbnail_url")

            if child_media_type == "IMAGE" and child_media_url:
                image_assets.append(
                    {
                        "media_url": child_media_url,
                        "image_url": child_media_url,
                        "video_url": None,
                        "media_type": "IMAGE",
                        "media_product_type": media_product_type,
                        "asset_position": position,
                        "frame_percent": None,
                    }
                )

            if child_media_type == "VIDEO" and child_thumbnail_url:
                image_assets.append(
                    {
                        "media_url": child_thumbnail_url,
                        "image_url": child_thumbnail_url,
                        "video_url": child_media_url,
                        "media_type": "VIDEO",
                        "media_product_type": media_product_type,
                        "asset_position": position,
                        "frame_percent": None,
                    }
                )

        return image_assets

    return []


def get_unique_ads_from_insights(
    ads_insights_rows: list[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    """
    Берёт уникальные объявления из результата Ads Insights.

    Если один ad_id встречается за несколько дат,
    embedding всё равно строим один раз.
    """
    unique_ads: dict[str, dict[str, Any]] = {}

    for row in ads_insights_rows:
        ad_id = row.get("ad_id")

        if not ad_id:
            continue

        if ad_id not in unique_ads:
            unique_ads[ad_id] = row

    return unique_ads


def process_ads_insights_image_embeddings(
    ads_insights_rows: list[dict[str, Any]],
) -> None:
    """
    Строит embeddings только для изображений из Ads Insights.

    Важно:
    - не берём все кампании аккаунта
    - берём только ad_id из ads_insights_rows
    - видео пропускаем
    - картинки скачиваем временно
    - после embedding файл сразу удаляем
    """
    unique_ads = get_unique_ads_from_insights(ads_insights_rows)
    ad_ids = list(unique_ads.keys())

    pgvector_db.delete_ad_embeddings_for_ads(ad_ids)

    for ad_id, row in unique_ads.items():
        try:
            image_assets = get_image_assets_for_ad(ad_id)
        except Exception as error:
            print(
                f"{ad_id}: failed to get media assets, "
                f"skipped, error={error}"
            )
            continue

        if not image_assets:
            print(f"{ad_id}: no image assets, skipped")
            continue

        for asset in image_assets:
            image_path = None

            try:
                image_path = download_image(asset["media_url"])
                embedding = create_image_embedding(image_path)

                pgvector_db.insert_ad_embedding(
                    campaign_id=row.get("campaign_id"),
                    campaign_name=row.get("campaign_name"),
                    adset_id=row.get("adset_id"),
                    adset_name=row.get("adset_name"),
                    ad_id=ad_id,
                    ad_name=row.get("ad_name"),
                    media_type=asset.get("media_type"),
                    media_product_type=asset.get("media_product_type"),
                    asset_position=asset.get("asset_position"),
                    image_url=asset.get("image_url"),
                    video_url=asset.get("video_url"),
                    frame_percent=asset.get("frame_percent"),
                    embedding_model=MODEL_NAME,
                    embedding=embedding,
                )

                print(
                    f"{ad_id}: embedding inserted, "
                    f"media_type={asset.get('media_type')}, "
                    f"asset_position={asset.get('asset_position')}"
                )

            except Exception as error:
                print(
                    f"{ad_id}: image embedding failed, "
                    f"asset_position={asset.get('asset_position')}, "
                    f"error={error}"
                )

            finally:
                delete_file(image_path)
