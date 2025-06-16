{{ config(
    materialized='table',
    schema='prepare'
) }}

{# Définition des années #}
{% set annees = [2016, 2017, 2018, 2019, 2020, 2021] %}

{# Récupération des bases sources uniques #}
{% set bases_query %}
    select distinct base_source
    from {{ ref('champs_disponibles_categorises') }}
    where categorie = 'population_generale'
{% endset %}

{% set bases = run_query(bases_query) %}
{% set bases_sources = [] %}
{% for base in bases %}
    {% do bases_sources.append(base[0]) %}
{% endfor %}

-- Début de la requête SQL
with 

{# Création des CTEs pour chaque base et année #}
{% for base in bases_sources %}
    {% for annee in annees %}
        {{ base }}_{{ annee }} as (
            {{ transform_column_names(
                source('sources', base ~ '_' ~ annee),
                annee,
                'population_generale'
            ) }}
        ){% if not loop.last %},{% endif %}
    {% endfor %}
    {% if not loop.last %},{% endif %}
{% endfor %},

-- Union des tables par base source
{% for base in bases_sources %}
    {{ base }}_unified as (
        select * from {{ base }}_{{ annees[0] }}
        {% for annee in annees[1:] %}
            union all
            select * from {{ base }}_{{ annee }}
        {% endfor %}
    ){% if not loop.last %},{% endif %}
{% endfor %}

-- Requête finale
select 
    coalesce({% for base in bases_sources %}{{ base }}_unified.CODGEO{% if not loop.last %}, {% endif %}{% endfor %}) as CODGEO,
    {% for base in bases_sources %}
        {{ base }}_unified.*{% if not loop.last %},{% endif %}
    {% endfor %}
from {% for base in bases_sources %}
    {{ base }}_unified
    {% if not loop.last %}
    full outer join
    {% endif %}
{% endfor %}
{% if bases_sources|length > 1 %}
    on {% for base in bases_sources %}
        {% if not loop.first %}
            {{ bases_sources[0] }}_unified.CODGEO = {{ base }}_unified.CODGEO
            {% if not loop.last %}and{% endif %}
        {% endif %}
    {% endfor %}
{% endif %}