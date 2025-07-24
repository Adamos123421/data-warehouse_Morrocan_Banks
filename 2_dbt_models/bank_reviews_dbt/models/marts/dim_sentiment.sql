{{ config(
    materialized='table',
    indexes=[
      {'columns': ['sentiment_id'], 'type': 'btree', 'unique': True},
      {'columns': ['sentiment_label'], 'type': 'btree'}
    ]
) }}

/*
DIM_SENTIMENT - Sentiment Analysis Dimension Table
Static dimension with predefined sentiment categories
Used for sentiment analysis classification
*/

SELECT 
    ROW_NUMBER() OVER (ORDER BY sentiment_label) as sentiment_key,
    sentiment_id,
    sentiment_label,
    sentiment_description,
    score_range_min,
    score_range_max,
    color_code,
    icon,
    CURRENT_TIMESTAMP as created_at
FROM (
    VALUES 
        ('positive', 'Positive', 'Positive customer sentiment - satisfied customers', 0.1000, 1.0000, '#4CAF50', '😊'),
        ('negative', 'Negative', 'Negative customer sentiment - dissatisfied customers', -1.0000, -0.1000, '#F44336', '😞'),
        ('neutral', 'Neutral', 'Neutral customer sentiment - neutral or mixed feelings', -0.1000, 0.1000, '#FFC107', '😐'),
        ('very_positive', 'Very Positive', 'Highly positive sentiment - extremely satisfied', 0.5000, 1.0000, '#2E7D32', '🤩'),
        ('very_negative', 'Very Negative', 'Highly negative sentiment - extremely dissatisfied', -1.0000, -0.5000, '#C62828', '😡')
) AS sentiment_data(
    sentiment_id,
    sentiment_label, 
    sentiment_description,
    score_range_min,
    score_range_max,
    color_code,
    icon
)
ORDER BY sentiment_label 