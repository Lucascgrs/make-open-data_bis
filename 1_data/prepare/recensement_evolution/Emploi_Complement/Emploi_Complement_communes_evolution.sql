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
        coalesce(shape_epci."SIREN_EPCI", scot_mapping."SIREN EPCI") as siren_epci,
        scot_mapping."SCoT" as nom_scot
    from {{ source('prepare', 'infos_communes') }} as ic
    left join {{ source('sources', 'shape_commune_2024') }} as shape_epci 
        on ic.code_commune = shape_epci."INSEE_COM"
    left join {{ source('sources', 'communes_to_scot') }} as scot_mapping 
        on ic.code_commune = LPAD(CAST(scot_mapping."INSEE commune" AS TEXT), 5, '0')
),

evolution_data as (
    {{ generate_evolution_table(
        category='Emploi_Complement',
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