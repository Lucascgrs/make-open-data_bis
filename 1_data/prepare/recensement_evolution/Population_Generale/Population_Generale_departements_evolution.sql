{{ config(materialized='table', schema='prepare') }}

with infos_communes as (
    select 
        code_commune,
        code_departement,
        code_region,
        nom_departement,
        nom_region,
        "SCoT" as nom_scot,
        "SIREN EPCI" as siren_epci
    from {{ source('prepare', 'infos_communes') }}
),

evolution_data as (
    {{ generate_evolution_table(
        category='Population_Generale',
        start_year=2016,
        end_year=2021
    ) }}
)

-- Affichons d'abord un exemple des données pour voir la structure
select *
from evolution_data
limit 1