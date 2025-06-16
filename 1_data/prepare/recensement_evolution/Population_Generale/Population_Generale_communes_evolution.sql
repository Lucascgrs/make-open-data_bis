{{ config(
    materialized='table',
    schema='prepare'
) }}

{# Définition des années #}
{% set annees = [2016, 2017, 2018, 2019, 2020, 2021] %}
{% do log("Années à traiter : " ~ annees, info=True) %}

{# Récupération des bases sources uniques #}
{% do print("🔍 Récupération des bases sources...") %}
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
{% do log("Bases sources trouvées : " ~ bases_sources | join(", "), info=True) %}

{# On construit la liste plate de tous les noms de CTEs #}
{% do print("📝 Construction de la liste des CTEs...") %}
{% set cte_names = [] %}
{% for base in bases_sources %}
    {% for annee in annees %}
        {% do cte_names.append(base ~ '_' ~ annee) %}
        {% do log("Ajout CTE : " ~ base ~ '_' ~ annee, info=True) %}
    {% endfor %}
{% endfor %}
{% do print("✅ " ~ cte_names | length ~ " CTEs générées") %}

{% do print("🔨 Début de la génération SQL...") %}
with
{# Génération des CTEs proprement #}
{% for cte in cte_names %}
    {% do log("Génération CTE : " ~ cte, info=True) %}
    {{ cte }} as (
        {{ transform_column_names(
            source('sources', cte),
            cte.split('_')[-1],
            'population_generale'
        ) }}
    ){% if not loop.last %},{% endif %}
{% endfor %}

{# Unions des tables par base source #}
{% do print("🔄 Génération des UNIONs...") %}
{% for base in bases_sources %}
    {% do log("Union pour la base : " ~ base, info=True) %}
,{{ base }}_unified as (
    select * from {{ base }}_{{ annees[0] }}
    {% for annee in annees[1:] %}
        union all
        select * from {{ base }}_{{ annee }}
    {% endfor %}
)
{% endfor %}

{% do print("🎯 Génération de la requête finale...") %}
-- Requête finale
select 
    {% do log("Génération du COALESCE pour CODGEO", info=True) %}
    coalesce({% for base in bases_sources %}{{ base }}_unified.CODGEO{% if not loop.last %}, {% endif %}{% endfor %}) as CODGEO,
    {% for base in bases_sources %}
        {% do log("Ajout des colonnes pour : " ~ base, info=True) %}
        {{ base }}_unified.*{% if not loop.last %},{% endif %}
    {% endfor %}
from
{% for base in bases_sources %}
    {{ base }}_unified
    {% if not loop.last %}
    {% do log("Ajout FULL OUTER JOIN pour : " ~ base, info=True) %}
    full outer join
    {% endif %}
{% endfor %}
{% if bases_sources|length > 1 %}
    on
    {% do log("Génération des conditions de jointure", info=True) %}
    {% for base in bases_sources %}
        {% if not loop.first %}
            {{ bases_sources[0] }}_unified.CODGEO = {{ base }}_unified.CODGEO
            {% if not loop.last %} and {% endif %}
        {% endif %}
    {% endfor %}
{% endif %}

{% do print("✨ Génération SQL terminée") %}