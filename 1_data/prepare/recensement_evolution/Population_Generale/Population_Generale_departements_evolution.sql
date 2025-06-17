{{ config(materialized='table', schema='prepare') }}

with commune_data as (
    select * from {{ ref('Population_Generale_communes_evolution') }}
),

-- Get numeric columns dynamically for aggregation
numeric_columns as (
    select 
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
    where code_departement is not null
    group by 
        code_departement,
        nom_departement,
        code_region,
        nom_region,
        "annee"
)

select 
    code_departement as "CODGEO",
    nom_departement,
    code_region,
    nom_region,
    "annee",
    "Population municipale",
    "Population comptée à part",
    "Population totale"
from numeric_columns
order by code_departement, "annee"
