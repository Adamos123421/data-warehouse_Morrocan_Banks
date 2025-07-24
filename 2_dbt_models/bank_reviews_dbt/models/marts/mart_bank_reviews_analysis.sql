{{
  config(
    materialized='table',
    description='Final analysis-ready mart for bank reviews with aggregated metrics and insights'
  )
}}

-- Final analytical model for bank reviews
with enriched_reviews as (
  select * from {{ ref('int_reviews_enriched') }}
),

-- Add analytical features
analysis_ready as (
  select
    review_id,
    place_id,
    bank_name,
    branch_name,
    author_name,
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
    
    -- Sentiment analysis (enhanced when Python script populates data)
    coalesce(sentiment_score, 
      case 
        when rating >= 4 then 0.5
        when rating <= 2 then -0.5
        else 0.0
      end
    ) as final_sentiment_score,
    
    coalesce(sentiment_label,
      case 
        when rating >= 4 then 'Positive'
        when rating <= 2 then 'Negative'
        else 'Neutral'
      end
    ) as final_sentiment_label,
    
    -- Topic modeling results
    dominant_topic,
    topic_distribution,
    
    -- Text features
    mentions_service,
    mentions_waiting,
    mentions_staff,
    
    -- Time-based features
    date_trunc('month', review_time::date) as review_month,
    date_trunc('year', review_time::date) as review_year,
    extract(dow from review_time::date) as day_of_week,
    
    -- Review quality metrics
    case 
      when word_count >= 50 then 'detailed'
      when word_count >= 20 then 'moderate'
      else 'brief'
    end as review_detail_level,
    
    -- Helpful flags for analysis
    case when rating = 1 and word_count > 30 then true else false end as detailed_negative,
    case when rating = 5 and word_count > 30 then true else false end as detailed_positive,
    
    -- Metadata
    translated,
    data_quality_flag
    
  from enriched_reviews
)

select
  -- Identifiers
  review_id,
  place_id,
  bank_name,
  branch_name,
  author_name,
  
  -- Review content
  review_text,
  cleaned_text,
  preprocessed_text,
  
  -- Language
  detected_language_name,
  
  -- Ratings
  rating,
  rating_category,
  
  -- Sentiment (final)
  final_sentiment_score,
  final_sentiment_label,
  
  -- Topics
  dominant_topic,
  topic_distribution,
  
  -- Text metrics
  review_length,
  word_count,
  review_detail_level,
  
  -- Content flags
  mentions_service,
  mentions_waiting,
  mentions_staff,
  detailed_negative,
  detailed_positive,
  
  -- Temporal
  review_time,
  review_month,
  review_year,
  day_of_week,
  collected_at,
  
  -- Metadata
  translated,
  data_quality_flag

from analysis_ready 