{{ config(
    materialized='table',
    schema='prepare'
) }}

{# Définition des années #}
{% set annees = [2016, 2017, 2018, 2019, 2020, 2021] %}

{# Récupération des bases sources uniques #}
{% set bases_query %}
    select distinct base_source
    from {{ source('sources', 'champs_disponibles_sources') }}
    where categorie = 'population_generale'
{% endset %}

{% set bases = run_query(bases_query) %}
{% set bases_sources = [] %}
{% for base in bases %}
    {% do bases_sources.append(base[0]) %}
{% endfor %}

{# On construit la liste plate de tous les noms de CTEs #}
{% set cte_names = [] %}
{% for base in bases_sources %}
    {% for annee in annees %}
        {% do cte_names.append(base ~ '_' ~ annee) %}
    {% endfor %}
{% endfor %}

with
{# Génération des CTEs proprement #}
{% for cte in cte_names %}
    {{ cte }} as (
        {{ transform_column_names(
            source('sources', cte),
            cte.split('_')[-1],
            'population_generale'
        ) }}
    ){% if not loop.last %},{% endif %}
{% endfor %}

{% for base in bases_sources %}
,{{ base }}_unified as (
    select * from {{ base }}_{{ annees[0] }}
    {% for annee in annees[1:] %}
        union all
        select * from {{ base }}_{{ annee }}
    {% endfor %}
)
{% endfor %}

-- Requête finale
select 
    coalesce({% for base in bases_sources %}{{ base }}_unified.CODGEO{% if not loop.last %}, {% endif %}{% endfor %}) as CODGEO,
    {% for base in bases_sources %}
        {{ base }}_unified.*{% if not loop.last %},{% endif %}
    {% endfor %}
from
{% for base in bases_sources %}
    {{ base }}_unified
    {% if not loop.last %}
    full outer join
    {% endif %}
{% endfor %}
{% if bases_sources|length > 1 %}
    on
    {% for base in bases_sources %}
        {% if not loop.first %}
            {{ bases_sources[0] }}_unified.CODGEO = {{ base }}_unified.CODGEO
            {% if not loop.last %} and {% endif %}
        {% endif %}
    {% endfor %}
{% endif %}