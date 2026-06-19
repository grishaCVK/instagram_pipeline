CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS pgcrypto;


CREATE TABLE IF NOT EXISTS ad_embeddings
(
    embedding_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    campaign_id TEXT,
    campaign_name TEXT,

    adset_id TEXT,
    adset_name TEXT,

    ad_id TEXT NOT NULL,
    ad_name TEXT,

    media_type TEXT,
    media_product_type TEXT,

    asset_position INTEGER,

    image_url TEXT,
    video_url TEXT,
    frame_percent SMALLINT,
    embedding_model TEXT,

    embedding vector(512) NOT NULL,

    created_at TIMESTAMP DEFAULT now(),
    updated_at TIMESTAMP DEFAULT now()
);


CREATE INDEX IF NOT EXISTS ad_embeddings_ad_id_idx
ON ad_embeddings(ad_id);


CREATE INDEX IF NOT EXISTS ad_embeddings_image_url_idx
ON ad_embeddings(image_url);


CREATE INDEX IF NOT EXISTS ad_embeddings_video_url_idx
ON ad_embeddings(video_url);


CREATE INDEX IF NOT EXISTS ad_embeddings_vector_idx
ON ad_embeddings
USING ivfflat (embedding vector_cosine_ops)
WITH (lists = 100);
