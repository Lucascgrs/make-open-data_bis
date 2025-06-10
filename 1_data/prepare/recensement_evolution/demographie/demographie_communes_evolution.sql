-- models/1_data/prepare/recensement_evolution/demographie_communes.sql
{{ config(materialized='table') }}

{# Paramètres #}
{% set annees = range(2016, 2022) %}      {# 2016 → 2021 #}

{# Récupère la table champs_categorises chargée depuis ton S3 #}
{% set champs_raw = dbt_utils.get_query_results(
    "select champ_insee, clef_json, base_source
       from {{ source('sources', 'champs_categorises') }}
      where categorie = 'demographie'"
) %}

{# Sépare les indicateurs par source #}
{% set rp_pop = [] %}
{% set rp_fam = [] %}

{% for row in champs_raw %}
    {% set record = {'champ_insee': row[0], 'clef_json': row[1]} %}
    {% if row[2] == 'rp_population' %}
        {% do rp_pop.append(record) %}
    {% elif row[2] == 'rp_familles_menages' %}
        {% do rp_fam.append(record) %}
    {% endif %}
{% endfor %}

with rp_population as (

    select
        lpad(cast(code_commune_insee as text), 5, '0') as code_commune,
        {{ generate_demographie_columns(rp_pop,  annees) }}
    from {{ source('sources', 'rp_population') }}

), rp_familles_menages as (

    select
        lpad(cast(code_commune_insee as text), 5, '0') as code_commune,
        {{ generate_demographie_columns(rp_fam,  annees) }}
    from {{ source('sources', 'rp_familles_menages') }}

)

select
    coalesce(rp_population.code_commune,
             rp_familles_menages.code_commune)                            as code_commune,
    {{ generate_demographie_columns(rp_pop + rp_fam, annees) }}
from rp_population
full outer join rp_familles_menages using (code_commune);
