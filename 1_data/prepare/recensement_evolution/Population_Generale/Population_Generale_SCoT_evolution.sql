{{ config(materialized='table', schema='prepare') }}

-- Note: This model assumes nom_scot column exists in the base table
-- If it doesn't exist, this model will fail and needs to be updated
with commune_data as (
    select * from {{ ref('Population_Generale_communes_evolution') }}
),

-- Get SCoT information and aggregate
scot_aggregation as (
    select 
        coalesce(nom_scot, 'UNKNOWN') as nom_scot,
        code_departement,
        code_region,
        "annee",
        -- Sum all population-related numeric columns based on actual column names
        sum("p_pop") as "p_pop"
    from commune_data
    where code_departement is not null
    group by 
        coalesce(nom_scot, 'UNKNOWN'),
        code_departement,
        code_region,
        "annee"
)

select 
    nom_scot as "CODGEO",
    code_departement,
    code_region,
    "annee",
    "p_pop"
from scot_aggregation
order by nom_scot, "annee"
