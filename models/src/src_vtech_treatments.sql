{{ config( materialized = 'table') }}



with vtech_service as (
    SELECT 
        *
    FROM (
        SELECT 
            treatments.AccountCode as account_code,
            treatments.ServiceCode as service_code,
            treatments.Speciescode as species_code,
            treatments.Description as name,
            case 
                when treatments.Charge > 0 then treatments.Charge
                else 0.0
            end as price_per_unit,
            case 
                when treatments.Taxed == 'Y' then 1
                when treatments.Taxed == 'N' then 0
                else 0
            end as is_taxable,
            treatments.Sig as instructions,
            '2000-11-01' as created_at,
            treatments.LastUpdated as updated_at
        FROM {{ source('vtech', 'treatments') }} as treatments

    )
),
dbt_treatment_map as (
    SELECT 
        *
    FROM (
        SELECT 
            CAST( map.vtech_account_code as int) as vtech_account_code,
            map.nectar_category as category,
            map.nectar_sub_category as subcategory
        FROM {{ source('dbt', 'treatments_map') }} as map
    )

)


select 
    1 as nectar_clinic_id,
    'vtech' as nectar_clinic_code,
    treatments.account_code as account_code,
    treatments.service_code as service_code,
    treatments.species_code as species_code,
    treatments.name as name,
    treatments.is_taxable as is_taxable,
    treatments.price_per_unit as price_per_unit,
    map.category as cat,
    map.subcategory as subcat,
    treatments.created_at as created_at,
    treatments.updated_at as updated_at,
    -- Check map with this data
    '{}' as meta,
    'ITEMS' as unit,
    '' as lot_no,
    '' as species,
    '' as vaccine,
    '' as exp_date,
    '' as admin_fee,
    '' as revision_id,
    treatments.instructions as instructions,
    '' as manufacturer,
    '[]' as satisfy_reminder,
    '[]' as generate_reminders
from vtech_service as treatments
Join dbt_treatment_map as map
    on map.vtech_account_code = treatments.account_code