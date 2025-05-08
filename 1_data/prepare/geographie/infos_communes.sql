{{ config(materialized='table') }}

with filtre_cog_communes as (
    select * 
    from {{ source('sources', 'cog_communes') }} as cog_communes     
    where cog_communes.type in ('commune-actuelle', 'arrondissement-municipal')

), denormalise_cog as (
    select
        LPAD(CAST(filtre_cog_communes.code as TEXT), 5, '0') as code_commune,
        filtre_cog_communes.nom as nom_commune,
        filtre_cog_communes.arrondissement as code_arrondissement,
        filtre_cog_communes.departement as code_departement,
        filtre_cog_communes.region as code_region,
        filtre_cog_communes.population as population,
        filtre_cog_communes.zone as code_zone,
        
        cog_arrondissements.nom as nom_arrondissement,
        cog_departements.nom as nom_departement,
        cog_regions.nom as nom_region

    from filtre_cog_communes
    left join {{ source('sources', 'cog_arrondissements') }} as cog_arrondissements 
        on cog_arrondissements.code = filtre_cog_communes.arrondissement
    left join {{ source('sources', 'cog_departements') }} as cog_departements 
        on cog_departements.code = filtre_cog_communes.departement
    left join {{ source('sources', 'cog_regions') }} as cog_regions 
        on cog_regions.code = filtre_cog_communes.region  

), ign_shapes as (
    select "INSEE_COM" as code_commune, geometry as commune_contour 
    from {{ source('sources', 'shape_commune_2024') }}
    union
    select "INSEE_ARM" as code_commune, geometry as commune_contour
    from {{ source('sources', 'shape_arrondissement_municipal_2024') }}
    
), scot_data as (
    select distinct on (code_commune)
        LPAD(CAST("INSEE commune" AS TEXT), 5, '0') as code_commune,
        "SCoT" as nom_scot,
        "SIREN EPCI" as code_epci
    from {{ source('sources', 'communes_to_scot') }}
    order by code_commune, "SCoT"

)

select
    denormalise_cog.*,
    ign_shapes.commune_contour,
    scot_data.nom_scot,
    scot_data.code_epci
from denormalise_cog
left join ign_shapes on denormalise_cog.code_commune = ign_shapes.code_commune
left join scot_data on denormalise_cog.code_commune = scot_data.code_commune
