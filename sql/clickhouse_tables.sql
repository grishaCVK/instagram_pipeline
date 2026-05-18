CREATE DATABASE IF NOT EXISTS instagram_ads;

CREATE TABLE IF NOT EXISTS instagram_ads.raw_data
(
    raw_id String,

    source String,
    api_type String,
    endpoint String,
    object_id Nullable(String),

    response_json String,
    fetched_at DateTime,
    request_params String
)
ENGINE = MergeTree
ORDER BY (source, fetched_at);


CREATE TABLE IF NOT EXISTS instagram_ads.paid_ads_awareness
(
    date_start DateTime('Asia/Almaty'),
    date_stop DateTime('Asia/Almaty'),

    campaign_id String,
    campaign_name String,
    campaign_status Nullable(String),
    campaign_effective_status Nullable(String),
    campaign_start_time Nullable(String),
    campaign_stop_time Nullable(String),

    adset_id String,
    adset_name String,

    ad_id String,
    ad_name String,
    destination_url Nullable(String),

    media_type Nullable(String),
    media_product_type Nullable(String),
    children_count Nullable(UInt16),
    children_json Nullable(String),
    conversion_location Nullable(String),
    is_incremental_attribution_enabled Nullable(Bool),
    attribution_setting Nullable(String),
    target_locations_json Nullable(String),
    age_range Nullable(String),
    gender Nullable(String),
    languages_json Nullable(String),
    placements_json Nullable(String),

    objective String,
    result_name Nullable(String),
    result_value Nullable(UInt64),
    cost_per_result Nullable(Float64),

    spend Nullable(Float64),
    impressions Nullable(UInt64),
    reach Nullable(UInt64),

    frequency Nullable(Float64),
    cpm Nullable(Float64),
    clicks Nullable(UInt64),
    inline_link_clicks Nullable(UInt64),
    inline_link_click_ctr Nullable(Float64),
    ctr Nullable(Float64),
    cpc Nullable(Float64),

    messaging_conversation_started Nullable(UInt64),
    cost_per_messaging_conversation_started Nullable(Float64),

    video_play_actions Nullable(UInt64),
    video_p25_watched_actions Nullable(UInt64),
    video_p50_watched_actions Nullable(UInt64),
    video_p75_watched_actions Nullable(UInt64),
    video_p100_watched_actions Nullable(UInt64),
    video_avg_time_watched_actions Nullable(Float64),

    daily_budget Nullable(Float64),
    lifetime_budget Nullable(Float64),
    budget_remaining Nullable(Float64),

    loaded_at DateTime
)
ENGINE = MergeTree
ORDER BY (date_start, campaign_id, adset_id, ad_id);


CREATE TABLE IF NOT EXISTS instagram_ads.paid_ads_traffic
(
    date_start DateTime('Asia/Almaty'),
    date_stop DateTime('Asia/Almaty'),

    campaign_id String,
    campaign_name String,
    campaign_status Nullable(String),
    campaign_effective_status Nullable(String),
    campaign_start_time Nullable(String),
    campaign_stop_time Nullable(String),

    adset_id String,
    adset_name String,

    ad_id String,
    ad_name String,
    destination_url Nullable(String),

    media_type Nullable(String),
    media_product_type Nullable(String),
    children_count Nullable(UInt16),
    children_json Nullable(String),
    conversion_location Nullable(String),
    is_incremental_attribution_enabled Nullable(Bool),
    attribution_setting Nullable(String),
    target_locations_json Nullable(String),
    age_range Nullable(String),
    gender Nullable(String),
    languages_json Nullable(String),
    placements_json Nullable(String),

    objective String,
    result_name Nullable(String),
    result_value Nullable(UInt64),
    cost_per_result Nullable(Float64),

    spend Nullable(Float64),
    impressions Nullable(UInt64),
    reach Nullable(UInt64),

    landing_page_view Nullable(UInt64),
    cost_per_landing_page_view Nullable(Float64),

    frequency Nullable(Float64),
    cpm Nullable(Float64),
    clicks Nullable(UInt64),
    inline_link_clicks Nullable(UInt64),
    inline_link_click_ctr Nullable(Float64),
    ctr Nullable(Float64),
    cpc Nullable(Float64),

    messaging_conversation_started Nullable(UInt64),
    cost_per_messaging_conversation_started Nullable(Float64),

    video_play_actions Nullable(UInt64),
    video_p25_watched_actions Nullable(UInt64),
    video_p50_watched_actions Nullable(UInt64),
    video_p75_watched_actions Nullable(UInt64),
    video_p100_watched_actions Nullable(UInt64),
    video_avg_time_watched_actions Nullable(Float64),

    daily_budget Nullable(Float64),
    lifetime_budget Nullable(Float64),
    budget_remaining Nullable(Float64),

    loaded_at DateTime
)
ENGINE = MergeTree
ORDER BY (date_start, campaign_id, adset_id, ad_id);


CREATE TABLE IF NOT EXISTS instagram_ads.paid_ads_engagement
(
    date_start DateTime('Asia/Almaty'),
    date_stop DateTime('Asia/Almaty'),

    campaign_id String,
    campaign_name String,
    campaign_status Nullable(String),
    campaign_effective_status Nullable(String),
    campaign_start_time Nullable(String),
    campaign_stop_time Nullable(String),

    adset_id String,
    adset_name String,

    ad_id String,
    ad_name String,
    destination_url Nullable(String),

    media_type Nullable(String),
    media_product_type Nullable(String),
    children_count Nullable(UInt16),
    children_json Nullable(String),
    conversion_location Nullable(String),
    is_incremental_attribution_enabled Nullable(Bool),
    attribution_setting Nullable(String),
    target_locations_json Nullable(String),
    age_range Nullable(String),
    gender Nullable(String),
    languages_json Nullable(String),
    placements_json Nullable(String),

    objective String,
    result_name Nullable(String),
    result_value Nullable(UInt64),
    cost_per_result Nullable(Float64),

    spend Nullable(Float64),
    impressions Nullable(UInt64),
    reach Nullable(UInt64),

    messaging_conversation_started Nullable(UInt64),
    cost_per_messaging_conversation_started Nullable(Float64),

    frequency Nullable(Float64),
    cpm Nullable(Float64),
    clicks Nullable(UInt64),
    inline_link_clicks Nullable(UInt64),
    inline_link_click_ctr Nullable(Float64),
    ctr Nullable(Float64),
    cpc Nullable(Float64),

    video_play_actions Nullable(UInt64),
    video_p25_watched_actions Nullable(UInt64),
    video_p50_watched_actions Nullable(UInt64),
    video_p75_watched_actions Nullable(UInt64),
    video_p100_watched_actions Nullable(UInt64),
    video_avg_time_watched_actions Nullable(Float64),

    daily_budget Nullable(Float64),
    lifetime_budget Nullable(Float64),
    budget_remaining Nullable(Float64),

    comments_count Nullable(UInt64),
    likes_count Nullable(UInt64),
    saved Nullable(UInt64),
    shares Nullable(UInt64),

    post_engagement Nullable(UInt64),
    cost_per_post_engagement Nullable(Float64),

    loaded_at DateTime
)
ENGINE = MergeTree
ORDER BY (date_start, campaign_id, adset_id, ad_id);


CREATE TABLE IF NOT EXISTS instagram_ads.paid_ads_leads
(
    date_start DateTime('Asia/Almaty'),
    date_stop DateTime('Asia/Almaty'),

    campaign_id String,
    campaign_name String,
    campaign_status Nullable(String),
    campaign_effective_status Nullable(String),
    campaign_start_time Nullable(String),
    campaign_stop_time Nullable(String),

    adset_id String,
    adset_name String,

    ad_id String,
    ad_name String,
    destination_url Nullable(String),

    media_type Nullable(String),
    media_product_type Nullable(String),
    children_count Nullable(UInt16),
    children_json Nullable(String),
    conversion_location Nullable(String),
    is_incremental_attribution_enabled Nullable(Bool),
    attribution_setting Nullable(String),
    target_locations_json Nullable(String),
    age_range Nullable(String),
    gender Nullable(String),
    languages_json Nullable(String),
    placements_json Nullable(String),

    objective String,
    result_name Nullable(String),
    result_value Nullable(UInt64),
    cost_per_result Nullable(Float64),

    spend Nullable(Float64),
    impressions Nullable(UInt64),
    reach Nullable(UInt64),

    messaging_conversation_started Nullable(UInt64),
    cost_per_messaging_conversation_started Nullable(Float64),

    frequency Nullable(Float64),
    cpm Nullable(Float64),
    clicks Nullable(UInt64),
    inline_link_clicks Nullable(UInt64),
    inline_link_click_ctr Nullable(Float64),
    ctr Nullable(Float64),
    cpc Nullable(Float64),

    video_play_actions Nullable(UInt64),
    video_p25_watched_actions Nullable(UInt64),
    video_p50_watched_actions Nullable(UInt64),
    video_p75_watched_actions Nullable(UInt64),
    video_p100_watched_actions Nullable(UInt64),
    video_avg_time_watched_actions Nullable(Float64),

    daily_budget Nullable(Float64),
    lifetime_budget Nullable(Float64),
    budget_remaining Nullable(Float64),

    profile_visits Nullable(UInt64),
    leads Nullable(UInt64),
    cost_per_lead Nullable(Float64),

    loaded_at DateTime
)
ENGINE = MergeTree
ORDER BY (date_start, campaign_id, adset_id, ad_id);


CREATE TABLE IF NOT EXISTS instagram_ads.paid_ads_app_promotion
(
    date_start DateTime('Asia/Almaty'),
    date_stop DateTime('Asia/Almaty'),

    campaign_id String,
    campaign_name String,
    campaign_status Nullable(String),
    campaign_effective_status Nullable(String),
    campaign_start_time Nullable(String),
    campaign_stop_time Nullable(String),

    adset_id String,
    adset_name String,

    ad_id String,
    ad_name String,
    destination_url Nullable(String),

    media_type Nullable(String),
    media_product_type Nullable(String),
    children_count Nullable(UInt16),
    children_json Nullable(String),
    conversion_location Nullable(String),
    is_incremental_attribution_enabled Nullable(Bool),
    attribution_setting Nullable(String),
    target_locations_json Nullable(String),
    age_range Nullable(String),
    gender Nullable(String),
    languages_json Nullable(String),
    placements_json Nullable(String),

    objective String,
    result_name Nullable(String),
    result_value Nullable(UInt64),
    cost_per_result Nullable(Float64),

    spend Nullable(Float64),
    impressions Nullable(UInt64),
    reach Nullable(UInt64),

    messaging_conversation_started Nullable(UInt64),
    cost_per_messaging_conversation_started Nullable(Float64),

    frequency Nullable(Float64),
    cpm Nullable(Float64),
    clicks Nullable(UInt64),
    inline_link_clicks Nullable(UInt64),
    inline_link_click_ctr Nullable(Float64),
    ctr Nullable(Float64),
    cpc Nullable(Float64),

    video_play_actions Nullable(UInt64),
    video_p25_watched_actions Nullable(UInt64),
    video_p50_watched_actions Nullable(UInt64),
    video_p75_watched_actions Nullable(UInt64),
    video_p100_watched_actions Nullable(UInt64),
    video_avg_time_watched_actions Nullable(Float64),

    daily_budget Nullable(Float64),
    lifetime_budget Nullable(Float64),
    budget_remaining Nullable(Float64),

    mobile_app_install Nullable(UInt64),
    cost_per_mobile_app_install Nullable(Float64),
    mobile_app_registration Nullable(UInt64),
    mobile_app_purchase Nullable(UInt64),

    loaded_at DateTime
)
ENGINE = MergeTree
ORDER BY (date_start, campaign_id, adset_id, ad_id);


CREATE TABLE IF NOT EXISTS instagram_ads.paid_ads_sales
(
    date_start DateTime('Asia/Almaty'),
    date_stop DateTime('Asia/Almaty'),

    campaign_id String,
    campaign_name String,
    campaign_status Nullable(String),
    campaign_effective_status Nullable(String),
    campaign_start_time Nullable(String),
    campaign_stop_time Nullable(String),

    adset_id String,
    adset_name String,

    ad_id String,
    ad_name String,
    destination_url Nullable(String),

    media_type Nullable(String),
    media_product_type Nullable(String),
    children_count Nullable(UInt16),
    children_json Nullable(String),
    conversion_location Nullable(String),
    is_incremental_attribution_enabled Nullable(Bool),
    attribution_setting Nullable(String),
    target_locations_json Nullable(String),
    age_range Nullable(String),
    gender Nullable(String),
    languages_json Nullable(String),
    placements_json Nullable(String),

    objective String,
    result_name Nullable(String),
    result_value Nullable(UInt64),
    cost_per_result Nullable(Float64),

    spend Nullable(Float64),
    impressions Nullable(UInt64),
    reach Nullable(UInt64),

    messaging_conversation_started Nullable(UInt64),
    cost_per_messaging_conversation_started Nullable(Float64),

    frequency Nullable(Float64),
    cpm Nullable(Float64),
    clicks Nullable(UInt64),
    inline_link_clicks Nullable(UInt64),
    inline_link_click_ctr Nullable(Float64),
    ctr Nullable(Float64),
    cpc Nullable(Float64),

    video_play_actions Nullable(UInt64),
    video_p25_watched_actions Nullable(UInt64),
    video_p50_watched_actions Nullable(UInt64),
    video_p75_watched_actions Nullable(UInt64),
    video_p100_watched_actions Nullable(UInt64),
    video_avg_time_watched_actions Nullable(Float64),

    daily_budget Nullable(Float64),
    lifetime_budget Nullable(Float64),
    budget_remaining Nullable(Float64),

    purchase Nullable(UInt64),
    cost_per_purchase Nullable(Float64),

    add_to_cart Nullable(UInt64),
    cost_per_add_to_cart Nullable(Float64),

    initiate_checkout Nullable(UInt64),
    cost_per_initiate_checkout Nullable(Float64),

    view_content Nullable(UInt64),
    cost_per_view_content Nullable(Float64),

    loaded_at DateTime
)
ENGINE = MergeTree
ORDER BY (date_start, campaign_id, adset_id, ad_id);
