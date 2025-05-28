{{ config(materialized='table') }}

{% set annees = [2016, 2017, 2018, 2019, 2020, 2021] %}

-- Un seul bloc WITH
with
cog_communes as (
  select
    code as code_commune,
    nom as nom_commune,
    departement as code_departement,
    region as code_region
  from {{ source('sources', 'cog_communes') }}
),

{% for annee in annees %}
base_{{ annee }} as (
{{ rename_columns_by_year(
        source('sources', 'base_cc_evol_struct_pop_' ~ annee),
        annee,
        ['code_commune']
    ) }}
){% if not loop.last %},{% endif %}
{% endfor %}

-- Requête principale
select * from base_2016
{% for annee in annees[1:] %}
union all
select * from base_{{ annee }}
{% endfor %}
