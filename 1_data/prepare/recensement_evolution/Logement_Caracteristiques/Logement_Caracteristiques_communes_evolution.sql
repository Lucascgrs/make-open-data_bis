{{ config(materialized='table', schema='prepare') }}

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
),

evolution_data as (
    {{ generate_evolution_table(
        category='Logement_Caracteristiques',
        start_year=2016,
        end_year=2021
    ) }}
)

select 
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