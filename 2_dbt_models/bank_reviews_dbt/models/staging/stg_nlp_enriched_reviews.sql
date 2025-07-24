{{ config(materialized='view') }}

-- Staging model for NLP-enriched bank reviews
-- This model references the enriched data created by the NLP pipeline

with source_data as (
    select 
        review_id,
        place_id,
        bank_name,
        branch_name,
        author_name,
        rating,
        original_text,
        cleaned_text,
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
        
        -- Analytical features
        word_count,
        char_count,
        review_year,
        review_month,
        review_quarter,
        day_of_week,
        is_weekend,
        rating_category,
        review_detail_level,
        
        -- Content flags
        mentions_service,
        mentions_waiting,
        mentions_fees,
        mentions_staff,
        
        -- Metadata
        original_language,
        translated,
        relative_time_description,
        collected_at,
        processed_at
        
    from {{ source('enriched', 'nlp_processed_reviews') }}
    where review_id is not null
),

validated_data as (
    select 
        *,
        -- Data quality flags
        case 
            when review_id is null then 'missing_id'
            when rating < 1 or rating > 5 then 'invalid_rating'
            when word_count = 0 then 'empty_text'
            when sentiment_score < -1 or sentiment_score > 1 then 'invalid_sentiment'
            else 'valid'
        end as data_quality_flag,
        
        -- Enhanced categorizations
        case 
            when rating >= 4.5 then 'Excellent'
            when rating >= 4 then 'Good'
            when rating >= 3 then 'Average'
            when rating >= 2 then 'Poor'
            else 'Very Poor'
        end as detailed_rating_category,
        
        case 
            when sentiment_score > 0.5 then 'Very Positive'
            when sentiment_score > 0.1 then 'Positive'
            when sentiment_score >= -0.1 then 'Neutral'
            when sentiment_score >= -0.5 then 'Negative'
            else 'Very Negative'
        end as detailed_sentiment_category,
        
        -- Business flags
        case 
            when mentions_service and sentiment_score < 0 then true
            else false
        end as service_complaint,
        
        case 
            when mentions_waiting and sentiment_score < 0 then true
            else false
        end as waiting_complaint,
        
        case 
            when mentions_fees and sentiment_score < 0 then true
            else false
        end as fees_complaint
        
    from source_data
)

select * from validated_data
where data_quality_flag = 'valid' 