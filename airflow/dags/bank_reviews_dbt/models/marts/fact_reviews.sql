{{ config(
    materialized='table',
    indexes=[
      {'columns': ['review_id'], 'type': 'btree', 'unique': True},
      {'columns': ['bank_key'], 'type': 'btree'},
      {'columns': ['branch_key'], 'type': 'btree'},
      {'columns': ['date_key'], 'type': 'btree'},
      {'columns': ['sentiment_key'], 'type': 'btree'},
      {'columns': ['rating'], 'type': 'btree'},
      {'columns': ['review_timestamp'], 'type': 'btree'}
    ]
) }}

/*
FACT_REVIEWS - Central Fact Table
Contains all review data with foreign keys to dimension tables
Stores measures (facts) and descriptive attributes
*/

WITH base_reviews AS (
    SELECT 
        -- Business keys
        review_id,
        place_id as google_place_id,
        
        -- Source data
        bank_name,
        place_id,
        author_name,
        author_url,
        profile_photo_url,
        
        -- Measures (Facts)
        rating,
        sentiment_score,
        sentiment_confidence,
        subjectivity,
        word_count,
        char_count,
        language_confidence,
        
        -- Review content
        text as review_text,
        cleaned_text,
        original_language,
        detected_language,
        translated as is_translated,
        
        -- Topic analysis
        dominant_topic,
        topic_distribution,
        
        -- Content flags
        mentions_service,
        mentions_waiting,
        mentions_fees,
        CASE WHEN LOWER(cleaned_text) LIKE '%staff%' OR LOWER(cleaned_text) LIKE '%employee%' OR LOWER(cleaned_text) LIKE '%personnel%' THEN TRUE ELSE FALSE END as mentions_staff,
        CASE WHEN LOWER(cleaned_text) LIKE '%digital%' OR LOWER(cleaned_text) LIKE '%online%' OR LOWER(cleaned_text) LIKE '%app%' OR LOWER(cleaned_text) LIKE '%internet%' THEN TRUE ELSE FALSE END as mentions_digital,
        
        -- Time information
        review_time as review_timestamp,
        collected_at,
        relative_time_description,
        
        -- Calculated fields
        rating_category,
        review_detail_level as review_length_category
        
    FROM {{ ref('stg_bank_reviews') }}
    WHERE review_id IS NOT NULL
),

enriched_reviews AS (
    SELECT 
        br.*,
        
        -- Get dimension keys
        db.bank_key,
        dbr.branch_key,
        
        -- Calculate date key from review timestamp
        CASE 
            WHEN br.review_timestamp IS NOT NULL THEN
                EXTRACT(YEAR FROM br.review_timestamp) * 10000 + 
                EXTRACT(MONTH FROM br.review_timestamp) * 100 + 
                EXTRACT(DAY FROM br.review_timestamp)
            ELSE NULL
        END as date_key,
        
        -- Map sentiment to sentiment key
        CASE 
            WHEN br.sentiment_score >= 0.5 THEN (SELECT sentiment_key FROM {{ ref('dim_sentiment') }} WHERE sentiment_id = 'very_positive')
            WHEN br.sentiment_score >= 0.1 THEN (SELECT sentiment_key FROM {{ ref('dim_sentiment') }} WHERE sentiment_id = 'positive')
            WHEN br.sentiment_score <= -0.5 THEN (SELECT sentiment_key FROM {{ ref('dim_sentiment') }} WHERE sentiment_id = 'very_negative')
            WHEN br.sentiment_score <= -0.1 THEN (SELECT sentiment_key FROM {{ ref('dim_sentiment') }} WHERE sentiment_id = 'negative')
            ELSE (SELECT sentiment_key FROM {{ ref('dim_sentiment') }} WHERE sentiment_id = 'neutral')
        END as sentiment_key,
        
        -- Create reviewer key (simplified - using hash of author info)
        ABS(HASHTEXT(COALESCE(br.author_name, '') || COALESCE(br.author_url, ''))) as reviewer_key
        
    FROM base_reviews br
    LEFT JOIN {{ ref('dim_bank') }} db ON br.bank_name = db.bank_name
    LEFT JOIN {{ ref('dim_branch') }} dbr ON br.place_id = dbr.branch_id
)

SELECT 
    ROW_NUMBER() OVER (ORDER BY review_timestamp DESC, review_id) as review_key,
    
    -- Dimension Keys (Foreign Keys)
    bank_key,
    branch_key,
    NULL as location_key, -- Will be populated when dim_location is implemented
    sentiment_key,
    date_key,
    reviewer_key,
    
    -- Business Keys
    review_id,
    google_place_id,
    
    -- Measures (Facts)
    rating,
    sentiment_score,
    sentiment_confidence,
    subjectivity as subjectivity_score,
    word_count,
    char_count,
    language_confidence,
    
    -- Review Content
    review_text,
    cleaned_text,
    original_language,
    detected_language,
    is_translated,
    
    -- Topic Analysis
    dominant_topic,
    topic_distribution::JSONB,
    
    -- Content Flags
    mentions_service,
    mentions_waiting,
    mentions_fees,
    mentions_staff,
    mentions_digital,
    
    -- Metadata
    review_timestamp,
    collected_at,
    relative_time_description,
    
    -- Calculated Fields
    rating_category,
    review_length_category,
    
    -- Audit Fields
    CURRENT_TIMESTAMP as created_at,
    CURRENT_TIMESTAMP as updated_at
    
FROM enriched_reviews
WHERE bank_key IS NOT NULL -- Ensure we have valid dimension references
ORDER BY review_timestamp DESC 