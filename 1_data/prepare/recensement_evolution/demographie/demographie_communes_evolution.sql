{{ config(materialized='table') }}

{% set annees = [2016, 2017, 2018, 2019, 2020, 2021] %}

-- On crée un seul WITH contenant tous les CTEs
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
-- Deux derniers chiffres de l'année (ex: 18 pour 2018)
{% set suffix = (annee|string)[2:] %}
{% set relation = source('sources', 'base_cc_evol_struct_pop_' ~ annee) %}

{% set cols = dbt_utils.get_filtered_columns_in_relation(
    relation=relation,
    include=['^.{{ '.' }}{{ suffix }}'],
    exclude=[]
) %}

base_{{ annee }} as (
    select
        code_commune,
        {% for col in cols %}
            {{ col }} as {{ col[3:] }}{% if not loop.last %},{% endif %}
        {% endfor %},
        {{ annee }} as annee
    from {{ relation }}
){% if not loop.last %},{% endif %}

{% endfor %}

-- Union finale
select * from base_2016
{% for annee in annees[1:] %}
union all
select * from base_{{ annee }}
{% endfor %}
