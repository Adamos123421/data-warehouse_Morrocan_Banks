{{
  config(
    materialized='view',
    description='Staging model for raw bank reviews with initial data cleaning'
  )
}}

-- Raw reviews data with basic cleaning and validation
with raw_reviews as (
  select
    place_id,
    bank_name,
    branch_name,
    author_name,
    author_url,
    language,
    original_language,
    profile_photo_url,
    rating,
    relative_time_description,
    text as review_text,
    time as review_time,
    translated,
    review_id,
    collected_at,
    
    -- Add data quality flags
    case 
      when text is null or trim(text) = '' then 'empty_text'
      when length(text) < {{ var('min_review_length') }} then 'too_short'
      when length(text) > {{ var('max_review_length') }} then 'too_long'
      else 'valid'
    end as data_quality_flag,
    
    -- Clean and normalize text
    lower(trim(regexp_replace(text, '[^a-zA-Z0-9àáâãäçèéêëìíîïñòóôõöùúûüÿ\s]', ' ', 'g'))) as cleaned_text,
    
    -- Extract metadata
    length(text) as review_length,
    case when rating >= 4 then 'positive'
         when rating <= 2 then 'negative'
         else 'neutral'
    end as rating_category
    
  from {{ source('raw_data', 'bank_reviews') }}
  where review_id is not null
),

-- Remove duplicate reviews
deduped_reviews as (
  select distinct
    place_id,
    bank_name,
    branch_name,
    author_name,
    language,
    original_language,
    rating,
    review_text,
    review_time,
    translated,
    review_id,
    collected_at,
    data_quality_flag,
    cleaned_text,
    review_length,
    rating_category
  from raw_reviews
)

select * from deduped_reviews
where data_quality_flag = 'valid' 