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
        -- Sum all population-related numeric columns
        sum("Population municipale") as "Population municipale",
        sum("Population comptée à part") as "Population comptée à part", 
        sum("Population totale") as "Population totale"
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
    "Population municipale",
    "Population comptée à part",
    "Population totale"
from epci_aggregation
order by siren_epci, "annee"
