-- ============================================================
-- instagram_raw
-- ============================================================

CREATE DATABASE IF NOT EXISTS instagram_raw;

CREATE TABLE IF NOT EXISTS instagram_raw.raw_data
(
    raw_id          String,
    source          String,
    api_type        String,
    endpoint        String,
    object_id       Nullable(String),
    response_json   String,
    fetched_at      DateTime,
    request_params  String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(fetched_at)
ORDER BY (source, fetched_at);


-- ============================================================
-- instagram_staging — hourly ad level (6 tables)
-- date_start / date_stop: DateTime (hourly breakdown)
-- No reach / frequency (inaccurate at hourly level)
-- ============================================================

CREATE DATABASE IF NOT EXISTS instagram_staging;

-- Общие колонки для всех hourly таблиц (макрос-комментарий)
-- date_start DateTime('Asia/Almaty')
-- date_stop  DateTime('Asia/Almaty')
-- campaign_id / campaign_name / campaign_status /
--   campaign_effective_status /
--   campaign_start_time / campaign_stop_time
-- adset_id / adset_name
-- ad_id / ad_name / destination_url
-- media_type / media_product_type /
--   children_count / children_json
-- conversion_location /
--   is_incremental_attribution_enabled /
--   attribution_setting
-- target_locations_json / age_range / gender /
--   languages_json / placements_json
-- objective / result_name / result_value / cost_per_result
-- spend / impressions
-- messaging_conversation_started /
--   cost_per_messaging_conversation_started
-- cpm / clicks / inline_link_clicks /
--   inline_link_click_ctr / ctr / cpc
-- video_play_actions / video_p25/p50/p75/p100_watched /
--   video_avg_time_watched_actions
-- daily_budget / lifetime_budget / budget_remaining
-- loaded_at DateTime

CREATE TABLE IF NOT EXISTS
    instagram_staging.instagram_ads_awareness_hourly_ad_level_staging
(
    date_start                              DateTime('Asia/Almaty'),
    date_stop                               DateTime('Asia/Almaty'),
    campaign_id                             String,
    campaign_name                           String,
    campaign_status                         Nullable(String),
    campaign_effective_status               Nullable(String),
    campaign_start_time                     Nullable(String),
    campaign_stop_time                      Nullable(String),
    adset_id                                String,
    adset_name                              String,
    ad_id                                   String,
    ad_name                                 String,
    destination_url                         Nullable(String),
    media_type                              Nullable(String),
    media_product_type                      Nullable(String),
    children_count                          Nullable(UInt16),
    children_json                           Nullable(String),
    conversion_location                     Nullable(String),
    is_incremental_attribution_enabled      Nullable(Bool),
    attribution_setting                     Nullable(String),
    target_locations_json                   Nullable(String),
    age_range                               Nullable(String),
    gender                                  Nullable(String),
    languages_json                          Nullable(String),
    placements_json                         Nullable(String),
    objective                               String,
    result_name                             Nullable(String),
    result_value                            Nullable(UInt64),
    cost_per_result                         Nullable(Float64),
    spend                                   Nullable(Float64),
    impressions                             Nullable(UInt64),
    messaging_conversation_started          Nullable(UInt64),
    cost_per_messaging_conversation_started Nullable(Float64),
    cpm                                     Nullable(Float64),
    clicks                                  Nullable(UInt64),
    inline_link_clicks                      Nullable(UInt64),
    inline_link_click_ctr                   Nullable(Float64),
    ctr                                     Nullable(Float64),
    cpc                                     Nullable(Float64),
    video_play_actions                      Nullable(UInt64),
    video_p25_watched_actions               Nullable(UInt64),
    video_p50_watched_actions               Nullable(UInt64),
    video_p75_watched_actions               Nullable(UInt64),
    video_p100_watched_actions              Nullable(UInt64),
    video_avg_time_watched_actions          Nullable(Float64),
    daily_budget                            Nullable(Float64),
    lifetime_budget                         Nullable(Float64),
    budget_remaining                        Nullable(Float64),
    loaded_at                               DateTime
)
ENGINE = MergeTree
ORDER BY (date_start, campaign_id, adset_id, ad_id);


CREATE TABLE IF NOT EXISTS
    instagram_staging.instagram_ads_traffic_hourly_ad_level_staging
(
    date_start                              DateTime('Asia/Almaty'),
    date_stop                               DateTime('Asia/Almaty'),
    campaign_id                             String,
    campaign_name                           String,
    campaign_status                         Nullable(String),
    campaign_effective_status               Nullable(String),
    campaign_start_time                     Nullable(String),
    campaign_stop_time                      Nullable(String),
    adset_id                                String,
    adset_name                              String,
    ad_id                                   String,
    ad_name                                 String,
    destination_url                         Nullable(String),
    media_type                              Nullable(String),
    media_product_type                      Nullable(String),
    children_count                          Nullable(UInt16),
    children_json                           Nullable(String),
    conversion_location                     Nullable(String),
    is_incremental_attribution_enabled      Nullable(Bool),
    attribution_setting                     Nullable(String),
    target_locations_json                   Nullable(String),
    age_range                               Nullable(String),
    gender                                  Nullable(String),
    languages_json                          Nullable(String),
    placements_json                         Nullable(String),
    objective                               String,
    result_name                             Nullable(String),
    result_value                            Nullable(UInt64),
    cost_per_result                         Nullable(Float64),
    spend                                   Nullable(Float64),
    impressions                             Nullable(UInt64),
    messaging_conversation_started          Nullable(UInt64),
    cost_per_messaging_conversation_started Nullable(Float64),
    cpm                                     Nullable(Float64),
    clicks                                  Nullable(UInt64),
    inline_link_clicks                      Nullable(UInt64),
    inline_link_click_ctr                   Nullable(Float64),
    ctr                                     Nullable(Float64),
    cpc                                     Nullable(Float64),
    video_play_actions                      Nullable(UInt64),
    video_p25_watched_actions               Nullable(UInt64),
    video_p50_watched_actions               Nullable(UInt64),
    video_p75_watched_actions               Nullable(UInt64),
    video_p100_watched_actions              Nullable(UInt64),
    video_avg_time_watched_actions          Nullable(Float64),
    daily_budget                            Nullable(Float64),
    lifetime_budget                         Nullable(Float64),
    budget_remaining                        Nullable(Float64),
    landing_page_view                       Nullable(UInt64),
    cost_per_landing_page_view              Nullable(Float64),
    loaded_at                               DateTime
)
ENGINE = MergeTree
ORDER BY (date_start, campaign_id, adset_id, ad_id);


CREATE TABLE IF NOT EXISTS
    instagram_staging.instagram_ads_engagement_hourly_ad_level_staging
(
    date_start                              DateTime('Asia/Almaty'),
    date_stop                               DateTime('Asia/Almaty'),
    campaign_id                             String,
    campaign_name                           String,
    campaign_status                         Nullable(String),
    campaign_effective_status               Nullable(String),
    campaign_start_time                     Nullable(String),
    campaign_stop_time                      Nullable(String),
    adset_id                                String,
    adset_name                              String,
    ad_id                                   String,
    ad_name                                 String,
    destination_url                         Nullable(String),
    media_type                              Nullable(String),
    media_product_type                      Nullable(String),
    children_count                          Nullable(UInt16),
    children_json                           Nullable(String),
    conversion_location                     Nullable(String),
    is_incremental_attribution_enabled      Nullable(Bool),
    attribution_setting                     Nullable(String),
    target_locations_json                   Nullable(String),
    age_range                               Nullable(String),
    gender                                  Nullable(String),
    languages_json                          Nullable(String),
    placements_json                         Nullable(String),
    objective                               String,
    result_name                             Nullable(String),
    result_value                            Nullable(UInt64),
    cost_per_result                         Nullable(Float64),
    spend                                   Nullable(Float64),
    impressions                             Nullable(UInt64),
    messaging_conversation_started          Nullable(UInt64),
    cost_per_messaging_conversation_started Nullable(Float64),
    cpm                                     Nullable(Float64),
    clicks                                  Nullable(UInt64),
    inline_link_clicks                      Nullable(UInt64),
    inline_link_click_ctr                   Nullable(Float64),
    ctr                                     Nullable(Float64),
    cpc                                     Nullable(Float64),
    video_play_actions                      Nullable(UInt64),
    video_p25_watched_actions               Nullable(UInt64),
    video_p50_watched_actions               Nullable(UInt64),
    video_p75_watched_actions               Nullable(UInt64),
    video_p100_watched_actions              Nullable(UInt64),
    video_avg_time_watched_actions          Nullable(Float64),
    daily_budget                            Nullable(Float64),
    lifetime_budget                         Nullable(Float64),
    budget_remaining                        Nullable(Float64),
    comments_count                          Nullable(UInt64),
    likes_count                             Nullable(UInt64),
    saved                                   Nullable(UInt64),
    shares                                  Nullable(UInt64),
    post_engagement                         Nullable(UInt64),
    cost_per_post_engagement                Nullable(Float64),
    loaded_at                               DateTime
)
ENGINE = MergeTree
ORDER BY (date_start, campaign_id, adset_id, ad_id);


CREATE TABLE IF NOT EXISTS
    instagram_staging.instagram_ads_leads_hourly_ad_level_staging
(
    date_start                              DateTime('Asia/Almaty'),
    date_stop                               DateTime('Asia/Almaty'),
    campaign_id                             String,
    campaign_name                           String,
    campaign_status                         Nullable(String),
    campaign_effective_status               Nullable(String),
    campaign_start_time                     Nullable(String),
    campaign_stop_time                      Nullable(String),
    adset_id                                String,
    adset_name                              String,
    ad_id                                   String,
    ad_name                                 String,
    destination_url                         Nullable(String),
    media_type                              Nullable(String),
    media_product_type                      Nullable(String),
    children_count                          Nullable(UInt16),
    children_json                           Nullable(String),
    conversion_location                     Nullable(String),
    is_incremental_attribution_enabled      Nullable(Bool),
    attribution_setting                     Nullable(String),
    target_locations_json                   Nullable(String),
    age_range                               Nullable(String),
    gender                                  Nullable(String),
    languages_json                          Nullable(String),
    placements_json                         Nullable(String),
    objective                               String,
    result_name                             Nullable(String),
    result_value                            Nullable(UInt64),
    cost_per_result                         Nullable(Float64),
    spend                                   Nullable(Float64),
    impressions                             Nullable(UInt64),
    messaging_conversation_started          Nullable(UInt64),
    cost_per_messaging_conversation_started Nullable(Float64),
    cpm                                     Nullable(Float64),
    clicks                                  Nullable(UInt64),
    inline_link_clicks                      Nullable(UInt64),
    inline_link_click_ctr                   Nullable(Float64),
    ctr                                     Nullable(Float64),
    cpc                                     Nullable(Float64),
    video_play_actions                      Nullable(UInt64),
    video_p25_watched_actions               Nullable(UInt64),
    video_p50_watched_actions               Nullable(UInt64),
    video_p75_watched_actions               Nullable(UInt64),
    video_p100_watched_actions              Nullable(UInt64),
    video_avg_time_watched_actions          Nullable(Float64),
    daily_budget                            Nullable(Float64),
    lifetime_budget                         Nullable(Float64),
    budget_remaining                        Nullable(Float64),
    profile_visits                          Nullable(UInt64),
    leads                                   Nullable(UInt64),
    cost_per_lead                           Nullable(Float64),
    loaded_at                               DateTime
)
ENGINE = MergeTree
ORDER BY (date_start, campaign_id, adset_id, ad_id);


CREATE TABLE IF NOT EXISTS
    instagram_staging.instagram_ads_app_promotion_hourly_ad_level_staging
(
    date_start                              DateTime('Asia/Almaty'),
    date_stop                               DateTime('Asia/Almaty'),
    campaign_id                             String,
    campaign_name                           String,
    campaign_status                         Nullable(String),
    campaign_effective_status               Nullable(String),
    campaign_start_time                     Nullable(String),
    campaign_stop_time                      Nullable(String),
    adset_id                                String,
    adset_name                              String,
    ad_id                                   String,
    ad_name                                 String,
    destination_url                         Nullable(String),
    media_type                              Nullable(String),
    media_product_type                      Nullable(String),
    children_count                          Nullable(UInt16),
    children_json                           Nullable(String),
    conversion_location                     Nullable(String),
    is_incremental_attribution_enabled      Nullable(Bool),
    attribution_setting                     Nullable(String),
    target_locations_json                   Nullable(String),
    age_range                               Nullable(String),
    gender                                  Nullable(String),
    languages_json                          Nullable(String),
    placements_json                         Nullable(String),
    objective                               String,
    result_name                             Nullable(String),
    result_value                            Nullable(UInt64),
    cost_per_result                         Nullable(Float64),
    spend                                   Nullable(Float64),
    impressions                             Nullable(UInt64),
    messaging_conversation_started          Nullable(UInt64),
    cost_per_messaging_conversation_started Nullable(Float64),
    cpm                                     Nullable(Float64),
    clicks                                  Nullable(UInt64),
    inline_link_clicks                      Nullable(UInt64),
    inline_link_click_ctr                   Nullable(Float64),
    ctr                                     Nullable(Float64),
    cpc                                     Nullable(Float64),
    video_play_actions                      Nullable(UInt64),
    video_p25_watched_actions               Nullable(UInt64),
    video_p50_watched_actions               Nullable(UInt64),
    video_p75_watched_actions               Nullable(UInt64),
    video_p100_watched_actions              Nullable(UInt64),
    video_avg_time_watched_actions          Nullable(Float64),
    daily_budget                            Nullable(Float64),
    lifetime_budget                         Nullable(Float64),
    budget_remaining                        Nullable(Float64),
    mobile_app_install                      Nullable(UInt64),
    cost_per_mobile_app_install             Nullable(Float64),
    mobile_app_registration                 Nullable(UInt64),
    mobile_app_purchase                     Nullable(UInt64),
    loaded_at                               DateTime
)
ENGINE = MergeTree
ORDER BY (date_start, campaign_id, adset_id, ad_id);


CREATE TABLE IF NOT EXISTS
    instagram_staging.instagram_ads_sales_hourly_ad_level_staging
(
    date_start                              DateTime('Asia/Almaty'),
    date_stop                               DateTime('Asia/Almaty'),
    campaign_id                             String,
    campaign_name                           String,
    campaign_status                         Nullable(String),
    campaign_effective_status               Nullable(String),
    campaign_start_time                     Nullable(String),
    campaign_stop_time                      Nullable(String),
    adset_id                                String,
    adset_name                              String,
    ad_id                                   String,
    ad_name                                 String,
    destination_url                         Nullable(String),
    media_type                              Nullable(String),
    media_product_type                      Nullable(String),
    children_count                          Nullable(UInt16),
    children_json                           Nullable(String),
    conversion_location                     Nullable(String),
    is_incremental_attribution_enabled      Nullable(Bool),
    attribution_setting                     Nullable(String),
    target_locations_json                   Nullable(String),
    age_range                               Nullable(String),
    gender                                  Nullable(String),
    languages_json                          Nullable(String),
    placements_json                         Nullable(String),
    objective                               String,
    result_name                             Nullable(String),
    result_value                            Nullable(UInt64),
    cost_per_result                         Nullable(Float64),
    spend                                   Nullable(Float64),
    impressions                             Nullable(UInt64),
    messaging_conversation_started          Nullable(UInt64),
    cost_per_messaging_conversation_started Nullable(Float64),
    cpm                                     Nullable(Float64),
    clicks                                  Nullable(UInt64),
    inline_link_clicks                      Nullable(UInt64),
    inline_link_click_ctr                   Nullable(Float64),
    ctr                                     Nullable(Float64),
    cpc                                     Nullable(Float64),
    video_play_actions                      Nullable(UInt64),
    video_p25_watched_actions               Nullable(UInt64),
    video_p50_watched_actions               Nullable(UInt64),
    video_p75_watched_actions               Nullable(UInt64),
    video_p100_watched_actions              Nullable(UInt64),
    video_avg_time_watched_actions          Nullable(Float64),
    daily_budget                            Nullable(Float64),
    lifetime_budget                         Nullable(Float64),
    budget_remaining                        Nullable(Float64),
    purchase                                Nullable(UInt64),
    cost_per_purchase                       Nullable(Float64),
    add_to_cart                             Nullable(UInt64),
    cost_per_add_to_cart                    Nullable(Float64),
    initiate_checkout                       Nullable(UInt64),
    cost_per_initiate_checkout              Nullable(Float64),
    view_content                            Nullable(UInt64),
    cost_per_view_content                   Nullable(Float64),
    loaded_at                               DateTime
)
ENGINE = MergeTree
ORDER BY (date_start, campaign_id, adset_id, ad_id);


-- ============================================================
-- instagram_staging — daily ad level (6 tables)
-- date_start / date_stop: Date
-- Has reach and frequency (accurate at daily level)
-- ============================================================

CREATE TABLE IF NOT EXISTS
    instagram_staging.instagram_ads_awareness_daily_ad_level_staging
(
    date_start                              Date,
    date_stop                               Date,
    campaign_id                             String,
    campaign_name                           String,
    campaign_status                         Nullable(String),
    campaign_effective_status               Nullable(String),
    campaign_start_time                     Nullable(String),
    campaign_stop_time                      Nullable(String),
    adset_id                                String,
    adset_name                              String,
    ad_id                                   String,
    ad_name                                 String,
    destination_url                         Nullable(String),
    media_type                              Nullable(String),
    media_product_type                      Nullable(String),
    children_count                          Nullable(UInt16),
    children_json                           Nullable(String),
    conversion_location                     Nullable(String),
    is_incremental_attribution_enabled      Nullable(Bool),
    attribution_setting                     Nullable(String),
    target_locations_json                   Nullable(String),
    age_range                               Nullable(String),
    gender                                  Nullable(String),
    languages_json                          Nullable(String),
    placements_json                         Nullable(String),
    objective                               String,
    result_name                             Nullable(String),
    result_value                            Nullable(UInt64),
    cost_per_result                         Nullable(Float64),
    spend                                   Nullable(Float64),
    impressions                             Nullable(UInt64),
    messaging_conversation_started          Nullable(UInt64),
    cost_per_messaging_conversation_started Nullable(Float64),
    cpm                                     Nullable(Float64),
    clicks                                  Nullable(UInt64),
    inline_link_clicks                      Nullable(UInt64),
    inline_link_click_ctr                   Nullable(Float64),
    ctr                                     Nullable(Float64),
    cpc                                     Nullable(Float64),
    video_play_actions                      Nullable(UInt64),
    video_p25_watched_actions               Nullable(UInt64),
    video_p50_watched_actions               Nullable(UInt64),
    video_p75_watched_actions               Nullable(UInt64),
    video_p100_watched_actions              Nullable(UInt64),
    video_avg_time_watched_actions          Nullable(Float64),
    daily_budget                            Nullable(Float64),
    lifetime_budget                         Nullable(Float64),
    budget_remaining                        Nullable(Float64),
    reach                                   Nullable(UInt64),
    frequency                               Nullable(Float64),
    loaded_at                               DateTime
)
ENGINE = MergeTree
ORDER BY (date_start, campaign_id, adset_id, ad_id);


CREATE TABLE IF NOT EXISTS
    instagram_staging.instagram_ads_traffic_daily_ad_level_staging
(
    date_start                              Date,
    date_stop                               Date,
    campaign_id                             String,
    campaign_name                           String,
    campaign_status                         Nullable(String),
    campaign_effective_status               Nullable(String),
    campaign_start_time                     Nullable(String),
    campaign_stop_time                      Nullable(String),
    adset_id                                String,
    adset_name                              String,
    ad_id                                   String,
    ad_name                                 String,
    destination_url                         Nullable(String),
    media_type                              Nullable(String),
    media_product_type                      Nullable(String),
    children_count                          Nullable(UInt16),
    children_json                           Nullable(String),
    conversion_location                     Nullable(String),
    is_incremental_attribution_enabled      Nullable(Bool),
    attribution_setting                     Nullable(String),
    target_locations_json                   Nullable(String),
    age_range                               Nullable(String),
    gender                                  Nullable(String),
    languages_json                          Nullable(String),
    placements_json                         Nullable(String),
    objective                               String,
    result_name                             Nullable(String),
    result_value                            Nullable(UInt64),
    cost_per_result                         Nullable(Float64),
    spend                                   Nullable(Float64),
    impressions                             Nullable(UInt64),
    messaging_conversation_started          Nullable(UInt64),
    cost_per_messaging_conversation_started Nullable(Float64),
    cpm                                     Nullable(Float64),
    clicks                                  Nullable(UInt64),
    inline_link_clicks                      Nullable(UInt64),
    inline_link_click_ctr                   Nullable(Float64),
    ctr                                     Nullable(Float64),
    cpc                                     Nullable(Float64),
    video_play_actions                      Nullable(UInt64),
    video_p25_watched_actions               Nullable(UInt64),
    video_p50_watched_actions               Nullable(UInt64),
    video_p75_watched_actions               Nullable(UInt64),
    video_p100_watched_actions              Nullable(UInt64),
    video_avg_time_watched_actions          Nullable(Float64),
    daily_budget                            Nullable(Float64),
    lifetime_budget                         Nullable(Float64),
    budget_remaining                        Nullable(Float64),
    reach                                   Nullable(UInt64),
    frequency                               Nullable(Float64),
    landing_page_view                       Nullable(UInt64),
    cost_per_landing_page_view              Nullable(Float64),
    loaded_at                               DateTime
)
ENGINE = MergeTree
ORDER BY (date_start, campaign_id, adset_id, ad_id);


CREATE TABLE IF NOT EXISTS
    instagram_staging.instagram_ads_engagement_daily_ad_level_staging
(
    date_start                              Date,
    date_stop                               Date,
    campaign_id                             String,
    campaign_name                           String,
    campaign_status                         Nullable(String),
    campaign_effective_status               Nullable(String),
    campaign_start_time                     Nullable(String),
    campaign_stop_time                      Nullable(String),
    adset_id                                String,
    adset_name                              String,
    ad_id                                   String,
    ad_name                                 String,
    destination_url                         Nullable(String),
    media_type                              Nullable(String),
    media_product_type                      Nullable(String),
    children_count                          Nullable(UInt16),
    children_json                           Nullable(String),
    conversion_location                     Nullable(String),
    is_incremental_attribution_enabled      Nullable(Bool),
    attribution_setting                     Nullable(String),
    target_locations_json                   Nullable(String),
    age_range                               Nullable(String),
    gender                                  Nullable(String),
    languages_json                          Nullable(String),
    placements_json                         Nullable(String),
    objective                               String,
    result_name                             Nullable(String),
    result_value                            Nullable(UInt64),
    cost_per_result                         Nullable(Float64),
    spend                                   Nullable(Float64),
    impressions                             Nullable(UInt64),
    messaging_conversation_started          Nullable(UInt64),
    cost_per_messaging_conversation_started Nullable(Float64),
    cpm                                     Nullable(Float64),
    clicks                                  Nullable(UInt64),
    inline_link_clicks                      Nullable(UInt64),
    inline_link_click_ctr                   Nullable(Float64),
    ctr                                     Nullable(Float64),
    cpc                                     Nullable(Float64),
    video_play_actions                      Nullable(UInt64),
    video_p25_watched_actions               Nullable(UInt64),
    video_p50_watched_actions               Nullable(UInt64),
    video_p75_watched_actions               Nullable(UInt64),
    video_p100_watched_actions              Nullable(UInt64),
    video_avg_time_watched_actions          Nullable(Float64),
    daily_budget                            Nullable(Float64),
    lifetime_budget                         Nullable(Float64),
    budget_remaining                        Nullable(Float64),
    reach                                   Nullable(UInt64),
    frequency                               Nullable(Float64),
    comments_count                          Nullable(UInt64),
    likes_count                             Nullable(UInt64),
    saved                                   Nullable(UInt64),
    shares                                  Nullable(UInt64),
    post_engagement                         Nullable(UInt64),
    cost_per_post_engagement                Nullable(Float64),
    loaded_at                               DateTime
)
ENGINE = MergeTree
ORDER BY (date_start, campaign_id, adset_id, ad_id);


CREATE TABLE IF NOT EXISTS
    instagram_staging.instagram_ads_leads_daily_ad_level_staging
(
    date_start                              Date,
    date_stop                               Date,
    campaign_id                             String,
    campaign_name                           String,
    campaign_status                         Nullable(String),
    campaign_effective_status               Nullable(String),
    campaign_start_time                     Nullable(String),
    campaign_stop_time                      Nullable(String),
    adset_id                                String,
    adset_name                              String,
    ad_id                                   String,
    ad_name                                 String,
    destination_url                         Nullable(String),
    media_type                              Nullable(String),
    media_product_type                      Nullable(String),
    children_count                          Nullable(UInt16),
    children_json                           Nullable(String),
    conversion_location                     Nullable(String),
    is_incremental_attribution_enabled      Nullable(Bool),
    attribution_setting                     Nullable(String),
    target_locations_json                   Nullable(String),
    age_range                               Nullable(String),
    gender                                  Nullable(String),
    languages_json                          Nullable(String),
    placements_json                         Nullable(String),
    objective                               String,
    result_name                             Nullable(String),
    result_value                            Nullable(UInt64),
    cost_per_result                         Nullable(Float64),
    spend                                   Nullable(Float64),
    impressions                             Nullable(UInt64),
    messaging_conversation_started          Nullable(UInt64),
    cost_per_messaging_conversation_started Nullable(Float64),
    cpm                                     Nullable(Float64),
    clicks                                  Nullable(UInt64),
    inline_link_clicks                      Nullable(UInt64),
    inline_link_click_ctr                   Nullable(Float64),
    ctr                                     Nullable(Float64),
    cpc                                     Nullable(Float64),
    video_play_actions                      Nullable(UInt64),
    video_p25_watched_actions               Nullable(UInt64),
    video_p50_watched_actions               Nullable(UInt64),
    video_p75_watched_actions               Nullable(UInt64),
    video_p100_watched_actions              Nullable(UInt64),
    video_avg_time_watched_actions          Nullable(Float64),
    daily_budget                            Nullable(Float64),
    lifetime_budget                         Nullable(Float64),
    budget_remaining                        Nullable(Float64),
    reach                                   Nullable(UInt64),
    frequency                               Nullable(Float64),
    profile_visits                          Nullable(UInt64),
    leads                                   Nullable(UInt64),
    cost_per_lead                           Nullable(Float64),
    loaded_at                               DateTime
)
ENGINE = MergeTree
ORDER BY (date_start, campaign_id, adset_id, ad_id);


CREATE TABLE IF NOT EXISTS
    instagram_staging.instagram_ads_app_promotion_daily_ad_level_staging
(
    date_start                              Date,
    date_stop                               Date,
    campaign_id                             String,
    campaign_name                           String,
    campaign_status                         Nullable(String),
    campaign_effective_status               Nullable(String),
    campaign_start_time                     Nullable(String),
    campaign_stop_time                      Nullable(String),
    adset_id                                String,
    adset_name                              String,
    ad_id                                   String,
    ad_name                                 String,
    destination_url                         Nullable(String),
    media_type                              Nullable(String),
    media_product_type                      Nullable(String),
    children_count                          Nullable(UInt16),
    children_json                           Nullable(String),
    conversion_location                     Nullable(String),
    is_incremental_attribution_enabled      Nullable(Bool),
    attribution_setting                     Nullable(String),
    target_locations_json                   Nullable(String),
    age_range                               Nullable(String),
    gender                                  Nullable(String),
    languages_json                          Nullable(String),
    placements_json                         Nullable(String),
    objective                               String,
    result_name                             Nullable(String),
    result_value                            Nullable(UInt64),
    cost_per_result                         Nullable(Float64),
    spend                                   Nullable(Float64),
    impressions                             Nullable(UInt64),
    messaging_conversation_started          Nullable(UInt64),
    cost_per_messaging_conversation_started Nullable(Float64),
    cpm                                     Nullable(Float64),
    clicks                                  Nullable(UInt64),
    inline_link_clicks                      Nullable(UInt64),
    inline_link_click_ctr                   Nullable(Float64),
    ctr                                     Nullable(Float64),
    cpc                                     Nullable(Float64),
    video_play_actions                      Nullable(UInt64),
    video_p25_watched_actions               Nullable(UInt64),
    video_p50_watched_actions               Nullable(UInt64),
    video_p75_watched_actions               Nullable(UInt64),
    video_p100_watched_actions              Nullable(UInt64),
    video_avg_time_watched_actions          Nullable(Float64),
    daily_budget                            Nullable(Float64),
    lifetime_budget                         Nullable(Float64),
    budget_remaining                        Nullable(Float64),
    reach                                   Nullable(UInt64),
    frequency                               Nullable(Float64),
    mobile_app_install                      Nullable(UInt64),
    cost_per_mobile_app_install             Nullable(Float64),
    mobile_app_registration                 Nullable(UInt64),
    mobile_app_purchase                     Nullable(UInt64),
    loaded_at                               DateTime
)
ENGINE = MergeTree
ORDER BY (date_start, campaign_id, adset_id, ad_id);


CREATE TABLE IF NOT EXISTS
    instagram_staging.instagram_ads_sales_daily_ad_level_staging
(
    date_start                              Date,
    date_stop                               Date,
    campaign_id                             String,
    campaign_name                           String,
    campaign_status                         Nullable(String),
    campaign_effective_status               Nullable(String),
    campaign_start_time                     Nullable(String),
    campaign_stop_time                      Nullable(String),
    adset_id                                String,
    adset_name                              String,
    ad_id                                   String,
    ad_name                                 String,
    destination_url                         Nullable(String),
    media_type                              Nullable(String),
    media_product_type                      Nullable(String),
    children_count                          Nullable(UInt16),
    children_json                           Nullable(String),
    conversion_location                     Nullable(String),
    is_incremental_attribution_enabled      Nullable(Bool),
    attribution_setting                     Nullable(String),
    target_locations_json                   Nullable(String),
    age_range                               Nullable(String),
    gender                                  Nullable(String),
    languages_json                          Nullable(String),
    placements_json                         Nullable(String),
    objective                               String,
    result_name                             Nullable(String),
    result_value                            Nullable(UInt64),
    cost_per_result                         Nullable(Float64),
    spend                                   Nullable(Float64),
    impressions                             Nullable(UInt64),
    messaging_conversation_started          Nullable(UInt64),
    cost_per_messaging_conversation_started Nullable(Float64),
    cpm                                     Nullable(Float64),
    clicks                                  Nullable(UInt64),
    inline_link_clicks                      Nullable(UInt64),
    inline_link_click_ctr                   Nullable(Float64),
    ctr                                     Nullable(Float64),
    cpc                                     Nullable(Float64),
    video_play_actions                      Nullable(UInt64),
    video_p25_watched_actions               Nullable(UInt64),
    video_p50_watched_actions               Nullable(UInt64),
    video_p75_watched_actions               Nullable(UInt64),
    video_p100_watched_actions              Nullable(UInt64),
    video_avg_time_watched_actions          Nullable(Float64),
    daily_budget                            Nullable(Float64),
    lifetime_budget                         Nullable(Float64),
    budget_remaining                        Nullable(Float64),
    reach                                   Nullable(UInt64),
    frequency                               Nullable(Float64),
    purchase                                Nullable(UInt64),
    cost_per_purchase                       Nullable(Float64),
    add_to_cart                             Nullable(UInt64),
    cost_per_add_to_cart                    Nullable(Float64),
    initiate_checkout                       Nullable(UInt64),
    cost_per_initiate_checkout              Nullable(Float64),
    view_content                            Nullable(UInt64),
    cost_per_view_content                   Nullable(Float64),
    loaded_at                               DateTime
)
ENGINE = MergeTree
ORDER BY (date_start, campaign_id, adset_id, ad_id);


-- ============================================================
-- instagram_staging — breakdown tables
-- ============================================================

CREATE TABLE IF NOT EXISTS
    instagram_staging.instagram_ads_geo_daily_level_staging
(
    date_start          Date,
    date_stop           Date,
    campaign_id         String,
    campaign_name       Nullable(String),
    adset_id            String,
    adset_name          Nullable(String),
    ad_id               String,
    ad_name             Nullable(String),
    objective           Nullable(String),
    country             Nullable(String),
    region              Nullable(String),
    spend               Nullable(Float64),
    impressions         UInt64,
    reach               Nullable(UInt64),
    frequency           Nullable(Float64),
    cpm                 Nullable(Float64),
    clicks              UInt64,
    inline_link_clicks  Nullable(UInt64),
    ctr                 Nullable(Float64),
    loaded_at           DateTime('Asia/Almaty')
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(date_start)
ORDER BY (
    date_start, campaign_id, adset_id, ad_id,
    ifNull(country, ''), ifNull(region, '')
);


CREATE TABLE IF NOT EXISTS
    instagram_staging.instagram_ads_device_daily_level_staging
(
    date_start          Date,
    date_stop           Date,
    campaign_id         String,
    campaign_name       Nullable(String),
    adset_id            String,
    adset_name          Nullable(String),
    ad_id               String,
    ad_name             Nullable(String),
    objective           Nullable(String),
    device_platform     Nullable(String),
    impression_device   Nullable(String),
    device_type         LowCardinality(String),
    os_type             LowCardinality(String),
    spend               Nullable(Float64),
    impressions         UInt64,
    reach               Nullable(UInt64),
    frequency           Nullable(Float64),
    cpm                 Nullable(Float64),
    clicks              UInt64,
    inline_link_clicks  Nullable(UInt64),
    ctr                 Nullable(Float64),
    loaded_at           DateTime('Asia/Almaty')
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(date_start)
ORDER BY (
    date_start, campaign_id, adset_id, ad_id,
    ifNull(device_platform, ''),
    ifNull(impression_device, '')
);


CREATE TABLE IF NOT EXISTS
    instagram_staging.instagram_ads_gender_daily_level_staging
(
    date_start          Date,
    date_stop           Date,
    campaign_id         String,
    campaign_name       Nullable(String),
    adset_id            String,
    adset_name          Nullable(String),
    ad_id               String,
    ad_name             Nullable(String),
    objective           Nullable(String),
    age_range           Nullable(String),
    gender_type         Nullable(String),
    spend               Nullable(Float64),
    impressions         Nullable(UInt64),
    reach               Nullable(UInt64),
    frequency           Nullable(Float64),
    cpm                 Nullable(Float64),
    clicks              Nullable(UInt64),
    inline_link_clicks  Nullable(UInt64),
    ctr                 Nullable(Float64),
    loaded_at           DateTime('Asia/Almaty')
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(date_start)
ORDER BY (
    date_start, campaign_id, adset_id, ad_id,
    ifNull(age_range, ''), ifNull(gender_type, '')
);


-- ============================================================
-- etl_metadata
-- ============================================================

CREATE DATABASE IF NOT EXISTS etl_metadata;

CREATE TABLE IF NOT EXISTS etl_metadata.etl_runs
(
    run_id                  String,
    pipeline_name           String,
    source_platform         String,
    run_type                String,
    status                  String,
    requested_date_since    Date,
    requested_date_until    Date,
    actual_min_date         Nullable(Date),
    actual_max_date         Nullable(Date),
    started_at              DateTime,
    finished_at             Nullable(DateTime),
    duration_seconds        Nullable(UInt32),
    total_raw_rows          UInt64,
    total_staging_rows      UInt64,
    total_core_rows         UInt64,
    error_stage             Nullable(String),
    error_message           Nullable(String),
    error_trace             Nullable(String)
)
ENGINE = MergeTree
ORDER BY (pipeline_name, started_at);


CREATE TABLE IF NOT EXISTS etl_metadata.etl_step_runs
(
    run_id          String,
    pipeline_name   String,
    source_platform String,
    step_name       String,
    step_order      UInt8,
    status          String,
    started_at      DateTime,
    finished_at     Nullable(DateTime),
    duration_seconds Nullable(UInt32),
    input_rows      UInt64,
    output_rows     UInt64,
    target_database Nullable(String),
    target_table    Nullable(String),
    error_message   Nullable(String),
    error_trace     Nullable(String)
)
ENGINE = MergeTree
ORDER BY (run_id, step_order);


CREATE TABLE IF NOT EXISTS etl_metadata.etl_table_loads
(
    run_id          String,
    pipeline_name   String,
    source_platform String,
    layer           String,
    database_name   String,
    table_name      String,
    date_since      Date,
    date_until      Date,
    rows_before     UInt64,
    rows_deleted    UInt64,
    rows_inserted   UInt64,
    rows_after      UInt64,
    min_loaded_date Nullable(Date),
    max_loaded_date Nullable(Date),
    loaded_at       DateTime
)
ENGINE = MergeTree
ORDER BY (pipeline_name, table_name, date_since);


CREATE TABLE IF NOT EXISTS etl_metadata.etl_data_quality_checks
(
    run_id          String,
    pipeline_name   String,
    source_platform String,
    check_name      String,
    check_level     String,
    database_name   String,
    table_name      String,
    status          String,
    checked_at      DateTime,
    failed_rows     UInt64,
    details         Nullable(String)
)
ENGINE = MergeTree
ORDER BY (pipeline_name, table_name, checked_at);
