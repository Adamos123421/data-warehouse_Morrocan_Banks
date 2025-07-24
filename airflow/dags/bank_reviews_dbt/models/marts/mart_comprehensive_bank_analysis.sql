{{ config(materialized='table') }}

-- Comprehensive bank analysis mart with NLP insights
-- Final analytical dataset for business intelligence and dashboards

with nlp_analytics as (
  select * from {{ ref('int_nlp_analytics') }}
),

bank_metrics as (
  select
    bank_name,
    count(*) as total_reviews,
    avg(rating) as avg_rating,
    avg(sentiment_score) as avg_sentiment_score,
    stddev(sentiment_score) as sentiment_volatility,
    
    -- Rating distribution
    count(case when rating = 5 then 1 end) as five_star_reviews,
    count(case when rating = 4 then 1 end) as four_star_reviews,
    count(case when rating = 3 then 1 end) as three_star_reviews,
    count(case when rating = 2 then 1 end) as two_star_reviews,
    count(case when rating = 1 then 1 end) as one_star_reviews,
    
    -- Sentiment distribution
    count(case when detailed_sentiment_category = 'Very Positive' then 1 end) as very_positive_sentiment,
    count(case when detailed_sentiment_category = 'Positive' then 1 end) as positive_sentiment,
    count(case when detailed_sentiment_category = 'Neutral' then 1 end) as neutral_sentiment,
    count(case when detailed_sentiment_category = 'Negative' then 1 end) as negative_sentiment,
    count(case when detailed_sentiment_category = 'Very Negative' then 1 end) as very_negative_sentiment,
    
    -- Issue analysis
    count(case when service_complaint then 1 end) as service_complaints,
    count(case when waiting_complaint then 1 end) as waiting_complaints,
    count(case when fees_complaint then 1 end) as fees_complaints,
    count(case when issue_severity = 'Critical' then 1 end) as critical_issues,
    count(case when issue_severity = 'High' then 1 end) as high_issues,
    
    -- Customer satisfaction
    count(case when customer_satisfaction_level = 'Highly Satisfied' then 1 end) as highly_satisfied,
    count(case when customer_satisfaction_level = 'Satisfied' then 1 end) as satisfied,
    count(case when customer_satisfaction_level = 'Dissatisfied' then 1 end) as dissatisfied,
    
    -- Content analysis
    avg(word_count) as avg_review_length,
    count(case when review_influence = 'High Impact' then 1 end) as high_impact_reviews,
    count(case when sentiment_rating_alignment = 'Misaligned' then 1 end) as misaligned_reviews
    
  from nlp_analytics
  group by bank_name
),

branch_metrics as (
  select
    bank_name,
    branch_name,
    place_id,
    count(*) as branch_total_reviews,
    avg(rating) as branch_avg_rating,
    avg(sentiment_score) as branch_avg_sentiment,
    
    -- Top issues by branch
    count(case when service_complaint then 1 end) as branch_service_complaints,
    count(case when waiting_complaint then 1 end) as branch_waiting_complaints,
    count(case when fees_complaint then 1 end) as branch_fees_complaints,
    
    -- Branch satisfaction
    round(count(case when customer_satisfaction_level in ('Highly Satisfied', 'Satisfied') then 1 end)::decimal / count(*) * 100, 2) as branch_satisfaction_rate
    
  from nlp_analytics
  group by bank_name, branch_name, place_id
),

topic_insights as (
  select
    bank_name,
    topic_category,
    count(*) as topic_mentions,
    avg(sentiment_score) as topic_avg_sentiment,
    avg(rating) as topic_avg_rating,
    
    -- Topic sentiment distribution
    count(case when sentiment_score > 0.1 then 1 end) as positive_topic_mentions,
    count(case when sentiment_score < -0.1 then 1 end) as negative_topic_mentions
    
  from nlp_analytics
  where topic_category != 'General'
  group by bank_name, topic_category
),

temporal_trends as (
  select
    bank_name,
    review_year,
    review_quarter,
    count(*) as quarterly_reviews,
    avg(rating) as quarterly_avg_rating,
    avg(sentiment_score) as quarterly_avg_sentiment,
    
    -- Trend indicators
    lag(avg(sentiment_score)) over (partition by bank_name order by review_year, review_quarter) as prev_quarter_sentiment,
    lag(avg(rating)) over (partition by bank_name order by review_year, review_quarter) as prev_quarter_rating
    
  from nlp_analytics
  where review_year is not null and review_quarter is not null
  group by bank_name, review_year, review_quarter
),

final_analysis as (
  select
    n.*,
    
    -- Bank-level metrics
    bm.total_reviews as bank_total_reviews,
    bm.avg_rating as bank_avg_rating,
    bm.avg_sentiment_score as bank_avg_sentiment,
    bm.sentiment_volatility as bank_sentiment_volatility,
    
    -- Performance indicators
    round(bm.five_star_reviews::decimal / bm.total_reviews * 100, 2) as bank_five_star_rate,
    round((bm.highly_satisfied + bm.satisfied)::decimal / bm.total_reviews * 100, 2) as bank_satisfaction_rate,
    round((bm.service_complaints + bm.waiting_complaints + bm.fees_complaints)::decimal / bm.total_reviews * 100, 2) as bank_complaint_rate,
    
    -- Branch performance
    br.branch_total_reviews,
    br.branch_avg_rating,
    br.branch_avg_sentiment,
    br.branch_satisfaction_rate,
    
    -- Competitive positioning
    rank() over (order by bm.avg_rating desc, bm.avg_sentiment_score desc) as bank_rating_rank,
    rank() over (order by bm.avg_sentiment_score desc, bm.avg_rating desc) as bank_sentiment_rank,
    
    -- Quality scores
    case 
      when bm.avg_rating >= 4.0 and bm.avg_sentiment_score > 0.1 then 'Excellent'
      when bm.avg_rating >= 3.5 and bm.avg_sentiment_score > 0.0 then 'Good'
      when bm.avg_rating >= 3.0 and bm.avg_sentiment_score >= -0.1 then 'Average'
      when bm.avg_rating >= 2.5 and bm.avg_sentiment_score >= -0.2 then 'Below Average'
      else 'Poor'
    end as bank_quality_tier,
    
    -- Risk indicators
    case 
      when bm.critical_issues > bm.total_reviews * 0.05 then 'High Risk'
      when bm.high_issues > bm.total_reviews * 0.10 then 'Medium Risk'
      else 'Low Risk'
    end as operational_risk_level
    
  from nlp_analytics n
  left join bank_metrics bm on n.bank_name = bm.bank_name
  left join branch_metrics br on n.bank_name = br.bank_name and n.branch_name = br.branch_name
)

select * from final_analysis 