{{ config(materialized='table', schema='prepare') }}

with cog_communes as (
    select
        ltrim(code, '0') as code_commune,
        nom as nom_commune,
        ltrim(departement, '0') as code_departement,
        ltrim(region, '0') as code_region,
        ltrim(arrondissement, '0') as arrondissement,
        replace(siren::text, '.0', '') as siren  -- Conversion du SIREN en text sans le cast en integer
    from {{ source('sources', 'cog_communes') }}
),

cog_departements as (
    select
        ltrim(code, '0') as code_departement,
        nom as nom_departement
    from {{ source('sources', 'cog_departements') }}
),

cog_regions as (
    select
        ltrim(code, '0') as code_region,
        nom as nom_region,
        "chefLieu" as CodeChefLieu
    from {{ source('sources', 'cog_regions') }}
),

communes_to_scot as (
    select 
        "SIREN EPCI"::text as siren_epci,  -- Conversion explicite en text
        "SCoT" as nom_scot,
        "Id SCoT" as code_scot
    from {{ source('sources', 'communes_to_scot') }}
),

evolution_data as (
    {{ generate_evolution_table(
        category='Population_Generale',
        start_year=2016,
        end_year=2021
    ) }}
)

select 
    e.*,
    c.nom_commune,
    c.code_departement,
    c.siren,
    d.nom_departement,
    c.code_region,
    r.nom_region,
    s.nom_scot,
    s.code_scot
from evolution_data e
left join cog_communes c on ltrim(e."CODGEO", '0') = c.code_commune
left join cog_departements d on c.code_departement = d.code_departement
left join cog_regions r on c.code_region = r.code_region
left join communes_to_scot s on c.siren = s.siren_epci  -- Jointure sur les SIREN en format texte