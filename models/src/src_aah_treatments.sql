{{ config( materialized = 'table') }}

with aah_service_history as (
    SELECT 
        *
    FROM (
        SELECT 
            treatments.Id as service_history_id,
            treatments.Date as created_at,
            treatments.CodeId as service_history_code_id,
            treatments.PatientId as aah_patient_id,
            treatments.TransactionId as aah_transaction_id,
            treatments.Description as aah_treatment_description,
            treatments.Updated as updated_at
        FROM {{ source('allied', 'treatments') }} as treatments

    )
),
aah_code as (
    SELECT 
        *
    FROM (
        SELECT 
            codes.Id as code_id,
            CAST(codes.CodeCategoryId as int) as aah_code_category_id,
            codes.Type as aah_code_type
        FROM {{ source('allied', 'codes') }} as codes

    )

),
aah_transactions as (
    SELECT 
        *
    FROM (
        SELECT 
            transactions.Id as aah_transaction_id,
            transactions.ClientId as aah_client_id,
            transactions.Quantity as aah_quantity,
            transactions.InvoiceId as aah_invoice_id,
            transactions.IsTaxable as aah_is_taxable,
            transactions.PatientId as aah_patient_id,
            transactions.IsPayment as aah_is_payment,
            transactions.LineAmount as aah_line_amount,
            transactions.LineNumber as ahh_line_number,
            transactions.Type as ahh_type,
            transactions.BitwerxType as ahh_bitwerx_type

        FROM {{ source('allied', 'transactions') }} as transactions

    )

),
dbt_treatment_map as (
    SELECT 
        *
    FROM (
        SELECT 
            CAST( map.aah_code_categories as int) as aah_code_categories,
            map.nectar_category as category,
            map.nectar_sub_category as subcategory
        FROM {{ source('dbt', 'treatments_map') }} as map
    )

)


select 
    2 as nectar_clinic_id,
    'aah' as nectar_clinic_code,
    treatment.service_history_id as aah_service_history_id,
    treatment.aah_patient_id as aah_patient_id,
    treatment.aah_transaction_id as aah_transaction_id,
    transactions.aah_client_id as aah_client_id,
    code.code_id as aah_code_id,
    transactions.aah_invoice_id as aah_invoice_id,
    code.aah_code_category_id as aah_code_category_id,
    transactions.ahh_type as aah_transaction_type,

    -- Nectar treatments
    treatment.aah_treatment_description as name,
    transactions.aah_line_amount as price_per_unit,
    map.category as cat,
    map.subcategory as subcat,
    treatment.created_at as created_at, -- Work with date format
    treatment.updated_at as updated_at, -- Work with date format 
    -- Check map with this data
    '0' as is_taxable,
    '0' as minimun_charge,
    'ITEMS' as unit,
    '' as lot_no,
    '' as species,
    '' as vaccine,
    '' as exp_date,
    '0' as admin_fee,
    '' as revision_id,
    '' as instructions,
    '' as manufacturer
from aah_service_history as treatment
join aah_code as code 
    on code.code_id = treatment.service_history_code_id
Join aah_transactions as transactions 
    on treatment.aah_transaction_id = transactions.aah_transaction_id
Join dbt_treatment_map as map
    on map.aah_code_categories = code.aah_code_category_id