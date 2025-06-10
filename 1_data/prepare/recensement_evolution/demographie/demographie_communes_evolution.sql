{{ config(materialized='table') }}

{% set annees = range(2016, 2022) %}        {# 2016‒2021 inclus #}

{% set champs_raw = dbt_utils.get_query_results("""
    SELECT champ_insee, clef_json, base_source
      FROM {{ source('sources', 'champs_categorises') }}
     WHERE categorie = 'demographie'
""") %}

{% set pop_champs = [] %}
{% set fam_champs = [] %}
{% for row in champs_raw %}
    {% set rec = {'champ_insee': row[0], 'clef_json': row[1]} %}
    {% if row[2] == 'rp_population' %}
        {% do pop_champs.append(rec) %}
    {% elif row[2] == 'rp_familles_menages' %}
        {% do fam_champs.append(rec) %}
    {% endif %}
{% endfor %}

with

{% for annee in annees %}
struct_pop_{{ annee }} as (
    select
        lpad(cast(codgeo as text), 5, '0')              AS code_commune,
        {{ generate_demographie_columns_year(
               pop_champs, annee, 'sp' ~ annee) }}
    from {{ source('sources', 'base_cc_evol_struct_pop_' ~ annee) }}  sp{{ annee }}
),
{% endfor %}

{% for annee in annees %}
fam_men_{{ annee }} as (
    select
        lpad(cast(codgeo as text), 5, '0')              AS code_commune,
        {{ generate_demographie_columns_year(
               fam_champs, annee, 'fm' ~ annee) }}
    from {{ source('sources', 'base_cc_coupl_fam_men_' ~ annee) }}     fm{{ annee }}
){% if not loop.last %},{% endif %}
{% endfor %}

, rp_population as (

    {# Démarre sur l’année la plus ancienne… #}
    select *
    from struct_pop_2016

    {# …et on ajoute chaque millésime suivant. #}
    {% for annee in annees if annee != 2016 %}
    full outer join struct_pop_{{ annee }} using (code_commune)
    {% endfor %}
)

, rp_familles_menages as (

    select *
    from fam_men_2016
    {% for annee in annees if annee != 2016 %}
    full outer join fam_men_{{ annee }} using (code_commune)
    {% endfor %}
)

select
    coalesce(rp_population.code_commune,
             rp_familles_menages.code_commune) AS code_commune,

    -- Colonnes Population 2016-2021
    {% for annee in annees %}
        {{ generate_demographie_columns_year(pop_champs, annee, 'rp_population') }},{% endfor %}

    -- Colonnes Familles/Ménages 2016-2021
    {% for annee in annees %}
        {{ generate_demographie_columns_year(fam_champs, annee, 'rp_familles_menages') }}{% if not loop.last %},{% endif %}
    {% endfor %}

from rp_population
full outer join rp_familles_menages using (code_commune);
