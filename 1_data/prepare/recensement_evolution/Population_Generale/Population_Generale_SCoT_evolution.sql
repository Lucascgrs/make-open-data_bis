{{ config(materialized='table', schema='prepare') }}

with commune_data as (
    select * from {{ ref('Population_Generale_communes_evolution') }}
),

-- Get SCoT information and aggregate
scot_aggregation as (
    select 
        nom_scot,
        code_departement,
        nom_departement,
        code_region,
        nom_region,
        "annee",
        -- Sum all population-related numeric columns based on actual column names
        sum("p_pop") as "p_pop"
    from commune_data
    where nom_scot is not null
    group by 
        nom_scot,
        code_departement,
        nom_departement,
        code_region,
        nom_region,
        "annee"
)

select 
    nom_scot as "CODGEO",
    code_departement,
    nom_departement,
    code_region,
    nom_region,
    "annee",
    "p_pop"
from scot_aggregation
order by nom_scot, "annee"
