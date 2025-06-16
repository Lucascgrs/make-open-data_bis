{{ config(materialized='table', schema='prepare') }}

with cog_communes as (
    select
        code as code_commune,
        nom as nom_commune,
        departement as code_departement,
        region as code_region
    from {{ source('sources', 'cog_communes') }}
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
    c.code_region
from evolution_data e
left join cog_communes c on e."CODGEO" = c.code_commune