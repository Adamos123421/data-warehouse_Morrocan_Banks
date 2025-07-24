{{ config(
    materialized='table',
    indexes=[
      {'columns': ['branch_id'], 'type': 'btree', 'unique': True},
      {'columns': ['city'], 'type': 'btree'},
      {'columns': ['bank_key'], 'type': 'btree'}
    ]
) }}

/*
DIM_BRANCH - Branch/Location Dimension Table
Contains branch/location information for each bank
Links to dim_bank via bank_key
*/

WITH branch_data AS (
    SELECT DISTINCT
        place_id as branch_id,
        branch_name,
        bank_name,
        
        -- Extract city from branch_name or use pattern matching
        CASE 
            WHEN UPPER(branch_name) LIKE '%CASABLANCA%' THEN 'Casablanca'
            WHEN UPPER(branch_name) LIKE '%RABAT%' THEN 'Rabat'
            WHEN UPPER(branch_name) LIKE '%FÈS%' OR UPPER(branch_name) LIKE '%FES%' THEN 'Fès'
            WHEN UPPER(branch_name) LIKE '%MARRAKECH%' OR UPPER(branch_name) LIKE '%MARRAKESH%' THEN 'Marrakech'
            WHEN UPPER(branch_name) LIKE '%AGADIR%' THEN 'Agadir'
            WHEN UPPER(branch_name) LIKE '%TANGIER%' OR UPPER(branch_name) LIKE '%TANGER%' THEN 'Tangier'
            WHEN UPPER(branch_name) LIKE '%MEKNÈS%' OR UPPER(branch_name) LIKE '%MEKNES%' THEN 'Meknès'
            WHEN UPPER(branch_name) LIKE '%OUJDA%' THEN 'Oujda'
            WHEN UPPER(branch_name) LIKE '%KENITRA%' THEN 'Kenitra'
            WHEN UPPER(branch_name) LIKE '%TETOUAN%' OR UPPER(branch_name) LIKE '%TÉTOUAN%' THEN 'Tetouan'
            WHEN UPPER(branch_name) LIKE '%SAFI%' THEN 'Safi'
            WHEN UPPER(branch_name) LIKE '%MOHAMMEDIA%' THEN 'Mohammedia'
            WHEN UPPER(branch_name) LIKE '%KHOURIBGA%' THEN 'Khouribga'
            WHEN UPPER(branch_name) LIKE '%BENI MELLAL%' THEN 'Beni Mellal'
            WHEN UPPER(branch_name) LIKE '%EL JADIDA%' THEN 'El Jadida'
            WHEN UPPER(branch_name) LIKE '%TAZA%' THEN 'Taza'
            WHEN UPPER(branch_name) LIKE '%NADOR%' THEN 'Nador'
            WHEN UPPER(branch_name) LIKE '%SETTAT%' THEN 'Settat'
            ELSE 'Other'
        END AS city,
        
        -- Regional classification
        CASE 
            WHEN UPPER(branch_name) LIKE '%CASABLANCA%' OR UPPER(branch_name) LIKE '%MOHAMMEDIA%' OR UPPER(branch_name) LIKE '%SETTAT%' THEN 'Casablanca-Settat'
            WHEN UPPER(branch_name) LIKE '%RABAT%' OR UPPER(branch_name) LIKE '%KENITRA%' THEN 'Rabat-Salé-Kénitra'
            WHEN UPPER(branch_name) LIKE '%FÈS%' OR UPPER(branch_name) LIKE '%FES%' OR UPPER(branch_name) LIKE '%MEKNÈS%' OR UPPER(branch_name) LIKE '%MEKNES%' THEN 'Fès-Meknès'
            WHEN UPPER(branch_name) LIKE '%MARRAKECH%' OR UPPER(branch_name) LIKE '%MARRAKESH%' THEN 'Marrakech-Safi'
            WHEN UPPER(branch_name) LIKE '%AGADIR%' THEN 'Souss-Massa'
            WHEN UPPER(branch_name) LIKE '%TANGIER%' OR UPPER(branch_name) LIKE '%TANGER%' OR UPPER(branch_name) LIKE '%TETOUAN%' OR UPPER(branch_name) LIKE '%TÉTOUAN%' THEN 'Tanger-Tétouan-Al Hoceïma'
            WHEN UPPER(branch_name) LIKE '%OUJDA%' OR UPPER(branch_name) LIKE '%NADOR%' THEN 'L\'Oriental'
            WHEN UPPER(branch_name) LIKE '%BENI MELLAL%' OR UPPER(branch_name) LIKE '%KHOURIBGA%' THEN 'Béni Mellal-Khénifra'
            WHEN UPPER(branch_name) LIKE '%SAFI%' THEN 'Marrakech-Safi'
            WHEN UPPER(branch_name) LIKE '%EL JADIDA%' THEN 'Casablanca-Settat'
            WHEN UPPER(branch_name) LIKE '%TAZA%' THEN 'Fès-Meknès'
            ELSE 'Other'
        END AS region,
        
        -- Branch type classification
        CASE 
            WHEN UPPER(branch_name) LIKE '%AGENCE%' OR UPPER(branch_name) LIKE '%AGENCY%' THEN 'Branch'
            WHEN UPPER(branch_name) LIKE '%SIEGE%' OR UPPER(branch_name) LIKE '%MAIN%' OR UPPER(branch_name) LIKE '%PRINCIPAL%' THEN 'Main'
            WHEN UPPER(branch_name) LIKE '%ATM%' OR UPPER(branch_name) LIKE '%GAB%' THEN 'ATM'
            WHEN UPPER(branch_name) LIKE '%CENTRE%' OR UPPER(branch_name) LIKE '%CENTER%' THEN 'Center'
            ELSE 'Branch'
        END AS branch_type
        
    FROM {{ ref('stg_bank_reviews') }}
    WHERE place_id IS NOT NULL AND branch_name IS NOT NULL
)

SELECT 
    ROW_NUMBER() OVER (ORDER BY bd.branch_name) as branch_key,
    bd.branch_id,
    bd.branch_name,
    
    -- Get bank_key from dim_bank
    db.bank_key,
    
    -- Address information (constructed from available data)
    bd.branch_name as full_address,
    bd.city,
    bd.region,
    NULL as postal_code, -- Not available in current data
    NULL as latitude,    -- Not available in current data  
    NULL as longitude,   -- Not available in current data
    NULL as phone_number, -- Not available in current data
    
    bd.branch_type,
    
    -- Services offered (inferred from bank type)
    CASE 
        WHEN bd.branch_type = 'ATM' THEN ARRAY['ATM', 'Cash Withdrawal']
        WHEN bd.branch_type = 'Main' THEN ARRAY['Banking Services', 'Customer Service', 'Loans', 'Accounts', 'International Transfer']
        ELSE ARRAY['Banking Services', 'Customer Service', 'Accounts']
    END as services_offered,
    
    NULL::JSONB as opening_hours, -- Not available in current data
    TRUE as is_active,
    CURRENT_TIMESTAMP as created_at,
    CURRENT_TIMESTAMP as updated_at
    
FROM branch_data bd
LEFT JOIN {{ ref('dim_bank') }} db ON bd.bank_name = db.bank_name
ORDER BY bd.branch_name 