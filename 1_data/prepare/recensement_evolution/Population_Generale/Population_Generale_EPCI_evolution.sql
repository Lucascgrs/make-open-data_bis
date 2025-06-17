{{ config(materialized='table', schema='prepare') }}

with commune_data as (
    select * from {{ ref('Population_Generale_communes_evolution') }}
),

-- Get EPCI information and aggregate
epci_aggregation as (
    select 
        siren_epci,
        code_departement,
        nom_departement,
        code_region,
        nom_region,
        "annee",
        -- Sum all population-related numeric columns based on actual column names
        sum("p_pop") as "p_pop"
    from commune_data
    where siren_epci is not null
    group by 
        siren_epci,
        code_departement,
        nom_departement,
        code_region,
        nom_region,
        "annee"
)

select 
    siren_epci as "CODGEO",
    code_departement,
    nom_departement,
    code_region,
    nom_region,
    "annee",
    "p_pop"
from epci_aggregation
order by siren_epci, "annee"
