{{ config(materialized='table') }}

{% set annees = [2016, 2017, 2018, 2019, 2020, 2021] %}

-- Bloc WITH
with

-- Référentiel des communes
cog_communes as (
  select
    code as code_commune,
    nom as nom_commune,
    departement as code_departement,
    region as code_region
  from {{ source('sources', 'cog_communes') }}
),

-- Tables annuelles transformées
{% for annee in annees %}
base_{{ annee }} as (
    {{ rename_columns_by_year(
        source('sources', 'base_cc_emploi_pop_active_' ~ annee),
        annee,
        ['CODGEO']
    ) }}
){% if not loop.last %},{% endif %}
{% endfor %},

-- Union de toutes les années
base_unifiee as (
    select * from base_2016
    {% for annee in annees[1:] %}
    union all
    select * from base_{{ annee }}
    {% endfor %}
)

-- Requête finale avec enrichissement géographique
select
    b.*,
    c.nom_commune,
    c.code_departement,
    c.code_region
from base_unifiee b

left join cog_communes c on b."CODGEO" = c.code_commune