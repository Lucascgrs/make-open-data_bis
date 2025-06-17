{{ config(materialized='table', schema='prepare') }}

<<<<<<< HEAD
with commune_data as (
    select * from {{ ref('Population_Generale_communes_evolution') }}
=======
with infos_communes as (
    select 
        ltrim(code_commune, '0') as code_commune,
        nom_commune,
        code_arrondissement,
        code_departement,
        code_region,
        nom_departement,
        nom_region,
        "SCoT" as nom_scot,
        "SIREN EPCI" as siren_epci
    from {{ source('prepare', 'infos_communes') }}
>>>>>>> edbf969 (add departement level)
),

-- Get numeric columns dynamically for aggregation
numeric_columns as (
    select 
        code_departement,
        nom_departement,
        code_region,
        nom_region,
        "annee",
        -- Sum all population-related numeric columns based on actual column names
        sum("p_pop") as "p_pop"
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
<<<<<<< HEAD
    code_departement as "CODGEO",
    nom_departement,
    code_region,
    nom_region,
    "annee",
    "p_pop"
from numeric_columns
order by code_departement, "annee"
=======
    e.*,
    i.nom_commune,
    i.code_departement,
    i.siren_epci,
    i.nom_departement,
    i.code_region,
    i.nom_region,
    i.nom_scot
from evolution_data e
left join infos_communes i on ltrim(e."CODGEO", '0') = i.code_commune
>>>>>>> edbf969 (add departement level)
