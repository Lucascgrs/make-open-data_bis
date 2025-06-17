{{ config(materialized='table', schema='prepare') }}

-- Note: This model assumes siren_epci column exists in the base table
-- If it doesn't exist, this model will fail and needs to be updated
with commune_data as (
    select * from {{ ref('Population_Generale_communes_evolution') }}
),

-- Get EPCI information and aggregate
epci_aggregation as (
    select 
        coalesce(siren_epci, 'UNKNOWN') as siren_epci,
        code_departement,
        code_region,
        "annee",
        -- Sum all population-related numeric columns based on actual column names
        sum("p_pop") as "p_pop"
    from commune_data
    where code_departement is not null
    group by 
        coalesce(siren_epci, 'UNKNOWN'),
        code_departement,
        code_region,
        "annee"
)

select 
    siren_epci as "CODGEO",
    code_departement,
    code_region,
    "annee",
    "p_pop"
from epci_aggregation
order by siren_epci, "annee"
