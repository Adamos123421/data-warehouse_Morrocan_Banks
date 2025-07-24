{{ config(
    materialized='table',
    indexes=[
      {'columns': ['bank_name'], 'type': 'btree'},
      {'columns': ['bank_id'], 'type': 'btree', 'unique': True}
    ]
) }}

/*
DIM_BANK - Bank Dimension Table
Slowly Changing Dimension Type 1 (SCD1)
Contains master data about banks in Morocco
*/

WITH bank_data AS (
    SELECT DISTINCT
        bank_name,
        -- Create a standardized bank_id from bank_name
        LOWER(REPLACE(REPLACE(bank_name, ' ', '_'), '-', '_')) as bank_id,
        
        -- Bank categorization based on name patterns
        CASE 
            WHEN LOWER(bank_name) LIKE '%islamic%' OR LOWER(bank_name) LIKE '%barid%' THEN 'Islamic'
            WHEN LOWER(bank_name) LIKE '%credit%' OR LOWER(bank_name) LIKE '%crédit%' THEN 'Credit'
            WHEN LOWER(bank_name) LIKE '%populaire%' THEN 'Cooperative'
            WHEN LOWER(bank_name) LIKE '%international%' THEN 'International'
            ELSE 'Commercial'
        END as bank_category,
        
        -- Bank type classification
        CASE 
            WHEN LOWER(bank_name) IN ('attijariwafa bank', 'banque populaire', 'bmce bank') THEN 'Large Commercial'
            WHEN LOWER(bank_name) LIKE '%crédit%' OR LOWER(bank_name) LIKE '%credit%' THEN 'Credit Institution'
            WHEN LOWER(bank_name) LIKE '%islamic%' THEN 'Islamic Banking'
            ELSE 'Commercial'
        END as bank_type,
        
        -- Estimated establishment year (based on known data)
        CASE 
            WHEN LOWER(bank_name) = 'attijariwafa bank' THEN 2004
            WHEN LOWER(bank_name) = 'banque populaire' THEN 1961
            WHEN LOWER(bank_name) = 'bmce bank' THEN 1959
            WHEN LOWER(bank_name) = 'crédit agricole du maroc' THEN 1961
            WHEN LOWER(bank_name) = 'bmci' THEN 1943
            WHEN LOWER(bank_name) = 'société générale maroc' THEN 1962
            WHEN LOWER(bank_name) = 'cih bank' THEN 1920
            WHEN LOWER(bank_name) = 'cdm' THEN 1959
            WHEN LOWER(bank_name) = 'al barid bank' THEN 2009
            ELSE NULL
        END as established_year,
        
        -- Headquarters city (based on known data)
        CASE 
            WHEN LOWER(bank_name) IN ('attijariwafa bank', 'bmce bank', 'bmci') THEN 'Casablanca'
            WHEN LOWER(bank_name) IN ('banque populaire', 'crédit agricole du maroc', 'cdm') THEN 'Rabat'
            WHEN LOWER(bank_name) = 'société générale maroc' THEN 'Casablanca'
            WHEN LOWER(bank_name) = 'cih bank' THEN 'Casablanca'
            WHEN LOWER(bank_name) = 'al barid bank' THEN 'Rabat'
            ELSE 'Casablanca' -- Default to economic capital
        END as headquarters_city,
        
        -- Website URLs (approximated)
        CASE 
            WHEN LOWER(bank_name) = 'attijariwafa bank' THEN 'https://www.attijariwafabank.com'
            WHEN LOWER(bank_name) = 'banque populaire' THEN 'https://www.gbp.ma'
            WHEN LOWER(bank_name) = 'bmce bank' THEN 'https://www.bmcebank.ma'
            WHEN LOWER(bank_name) = 'crédit agricole du maroc' THEN 'https://www.creditagricole.ma'
            WHEN LOWER(bank_name) = 'bmci' THEN 'https://www.bmci.ma'
            WHEN LOWER(bank_name) = 'société générale maroc' THEN 'https://www.sgmaroc.com'
            WHEN LOWER(bank_name) = 'cih bank' THEN 'https://www.cih.co.ma'
            WHEN LOWER(bank_name) = 'al barid bank' THEN 'https://www.albaridbank.ma'
            ELSE NULL
        END as website_url
        
    FROM {{ ref('stg_bank_reviews') }}
    WHERE bank_name IS NOT NULL
),

bank_metrics AS (
    SELECT 
        bank_name,
        COUNT(DISTINCT place_id) as total_branches
    FROM {{ ref('stg_bank_reviews') }}
    WHERE bank_name IS NOT NULL
    GROUP BY bank_name
)

SELECT 
    ROW_NUMBER() OVER (ORDER BY bd.bank_name) as bank_key,
    bd.bank_id,
    bd.bank_name,
    bd.bank_category,
    COALESCE(bm.total_branches, 0) as total_branches,
    bd.established_year,
    bd.bank_type,
    bd.headquarters_city,
    bd.website_url,
    CURRENT_TIMESTAMP as created_at,
    CURRENT_TIMESTAMP as updated_at
FROM bank_data bd
LEFT JOIN bank_metrics bm ON bd.bank_name = bm.bank_name
ORDER BY bd.bank_name 