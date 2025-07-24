{{
  config(
    materialized='table',
    description='Intermediate model with enriched review data including language detection and sentiment analysis'
  )
}}

-- Reviews with enrichment features
with base_reviews as (
  select * from {{ ref('stg_raw_reviews') }}
),

-- Language detection (placeholder - will be enriched by Python script)
language_enriched as (
  select
    *,
    -- Language detection will be added via Python preprocessing
    case 
      when language = 'fr' then 'French'
      when language = 'ar' then 'Arabic'
      when language = 'en' then 'English'
      else 'Other'
    end as detected_language_name,
    
    -- Text preprocessing for NLP
    regexp_replace(
      regexp_replace(
        regexp_replace(cleaned_text, '\s+', ' ', 'g'),  -- Multiple spaces to single
        '^[^a-zA-Z0-9àáâãäçèéêëìíîïñòóôõöùúûüÿ]+|[^a-zA-Z0-9àáâãäçèéêëìíîïñòóôõöùúûüÿ]+$', '', 'g'  -- Leading/trailing non-alphanumeric
      ),
      '[[:space:]]+', ' ', 'g'  -- Normalize spaces
    ) as preprocessed_text
    
  from base_reviews
),

-- Sentiment analysis (placeholder columns - will be populated by Python script)
sentiment_enriched as (
  select
    *,
    -- These columns will be populated by the Python NLP pipeline
    null::float as sentiment_score,
    null::varchar as sentiment_label,
    null::float as sentiment_confidence,
    
    -- Topic modeling placeholders
    null::varchar as dominant_topic,
    null::json as topic_distribution,
    
    -- Additional text features
    array_length(string_to_array(preprocessed_text, ' '), 1) as word_count,
    case 
      when preprocessed_text like '%service%' or preprocessed_text like '%accueil%' then true
      else false
    end as mentions_service,
    case 
      when preprocessed_text like '%attente%' or preprocessed_text like '%queue%' then true
      else false
    end as mentions_waiting,
    case 
      when preprocessed_text like '%personnel%' or preprocessed_text like '%employé%' then true
      else false
    end as mentions_staff
    
  from language_enriched
)

select
  review_id,
  place_id,
  bank_name,
  branch_name,
  author_name,
  language,
  detected_language_name,
  rating,
  rating_category,
  review_text,
  cleaned_text,
  preprocessed_text,
  review_length,
  word_count,
  review_time,
  collected_at,
  
  -- Sentiment analysis results (to be populated)
  sentiment_score,
  sentiment_label,
  sentiment_confidence,
  
  -- Topic modeling results (to be populated)
  dominant_topic,
  topic_distribution,
  
  -- Feature flags
  mentions_service,
  mentions_waiting,
  mentions_staff,
  
  -- Metadata
  translated,
  data_quality_flag
  
from sentiment_enriched 