{{ config(materialized='table') }}


cog_communes as (
  select
    code as code_commune,
    nom as nom_commune,
    departement as code_departement,
    region as code_region
  from {{ source('sources', 'cog_communes') }}
)

-- Étape finale : jointures explicites, directement dans le SELECT
select
  *
from
  cog_communes
  
