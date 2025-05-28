{{ config(materialized='table') }}

-- Définir les tables intermédiaires
with aggregated as (
  {{ aggreger_colonnes_theme_geo('demographie', 'demographie_renomee', 'code_commune_insee') }}
),

infos_communes as (
  select
    code_commune,
    nom_commune,
    code_departement,
    nom_departement,
    code_region,
    nom_region,
    code_epci,
    nom_epci,
    code_scot,
    nom_scot
  from {{ ref('infos_communes') }}
)

-- Faire toutes les jointures à la fin, de manière explicite
select
  *
from
  aggregated
  
left join infos_communes
  on aggregated.code_commune_insee = infos_communes.code_commune
