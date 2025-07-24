{{ config(materialized='view') }}

-- Advanced analytics using NLP-enriched data
-- This model creates business metrics and advanced analytics features

with nlp_base as (
  select * from {{ ref('stg_nlp_enriched_reviews') }}
),

business_metrics as (
  select
    *,
    
    -- Customer satisfaction scoring
    case 
      when rating >= 4 and sentiment_score > 0.1 then 'Highly Satisfied'
      when rating >= 4 or sentiment_score > 0.1 then 'Satisfied'
      when rating = 3 and sentiment_score between -0.1 and 0.1 then 'Neutral'
      when rating <= 2 or sentiment_score < -0.1 then 'Dissatisfied'
      else 'Unknown'
    end as customer_satisfaction_level,
    
    -- Issue severity scoring
    case 
      when (service_complaint or waiting_complaint or fees_complaint) 
           and sentiment_score < -0.5 then 'Critical'
      when (service_complaint or waiting_complaint or fees_complaint) 
           and sentiment_score < -0.1 then 'High'
      when mentions_service or mentions_waiting or mentions_fees then 'Medium'
      else 'Low'
    end as issue_severity,
    
    -- Review influence score (based on length and sentiment strength)
    case 
      when word_count >= 50 and abs(sentiment_score) > 0.3 then 'High Impact'
      when word_count >= 20 and abs(sentiment_score) > 0.1 then 'Medium Impact'
      else 'Low Impact'
    end as review_influence,
    
    -- Time-based features
    extract(hour from review_time) as review_hour,
    extract(dow from review_time) as day_of_week_num,
    
    -- Sentiment alignment with rating
    case 
      when (rating >= 4 and sentiment_score > 0) or 
           (rating <= 2 and sentiment_score < 0) then 'Aligned'
      when (rating >= 4 and sentiment_score < -0.1) or 
           (rating <= 2 and sentiment_score > 0.1) then 'Misaligned'
      else 'Neutral'
    end as sentiment_rating_alignment
    
  from nlp_base
),

topic_enriched as (
  select
    *,
    
    -- Topic categorization for business insights
    case 
      when dominant_topic ilike '%service%' then 'Service Quality'
      when dominant_topic ilike '%wait%' or dominant_topic ilike '%queue%' then 'Wait Times'
      when dominant_topic ilike '%staff%' or dominant_topic ilike '%personnel%' then 'Staff Performance'
      when dominant_topic ilike '%digital%' or dominant_topic ilike '%online%' then 'Digital Services'
      when dominant_topic ilike '%fee%' or dominant_topic ilike '%cost%' then 'Pricing'
      when dominant_topic ilike '%branch%' or dominant_topic ilike '%location%' then 'Facilities'
      else 'General'
    end as topic_category,
    
    -- Competitive insights
    case 
      when rating >= 4 and sentiment_score > 0.2 then 'Competitive Advantage'
      when rating <= 2 and sentiment_score < -0.2 then 'Competitive Risk'
      else 'Neutral Position'
    end as competitive_position
    
  from business_metrics
)

select
  -- Core identifiers
  review_id,
  place_id,
  bank_name,
  branch_name,
  author_name,
  
  -- Review content
  original_text,
  cleaned_text,
  rating,
  review_time,
  
  -- NLP features
  detected_language,
  language_confidence,
  sentiment_score,
  sentiment_label,
  sentiment_confidence,
  subjectivity,
  dominant_topic,
  topic_distribution,
  
  -- Text metrics
  word_count,
  char_count,
  review_detail_level,
  
  -- Temporal features
  review_year,
  review_month,
  review_quarter,
  day_of_week,
  is_weekend,
  review_hour,
  day_of_week_num,
  
  -- Content flags
  mentions_service,
  mentions_waiting,
  mentions_fees,
  mentions_staff,
  
  -- Complaint flags
  service_complaint,
  waiting_complaint,
  fees_complaint,
  
  -- Business metrics
  detailed_rating_category,
  detailed_sentiment_category,
  customer_satisfaction_level,
  issue_severity,
  review_influence,
  sentiment_rating_alignment,
  topic_category,
  competitive_position,
  
  -- Metadata
  original_language,
  translated,
  relative_time_description,
  collected_at,
  processed_at,
  data_quality_flag
  
from topic_enriched 