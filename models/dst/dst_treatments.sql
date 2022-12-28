
-- we can add others treatments tables from others client  in this file

SELECT 
    name,
    is_taxable,
    cat,
    subcat,
    created_at,
    updated_at,
    price_per_unit,
    -- Check map with this data
    unit,
    lot_no,
    species,
    vaccine,
    exp_date,
    admin_fee,
    revision_id,
    instructions,
    manufacturer  
FROM  {{ ref('src_aah_treatments')}}
