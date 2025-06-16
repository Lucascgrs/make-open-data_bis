{{ config(
    materialized='table',
    schema='prepare'
) }}

{# Définition des années #}
{% set annees = [2016, 2017, 2018, 2019, 2020, 2021] %}

{% do print("🔍 Récupération des bases sources") %}
{% set bases_query %}
    select distinct base_table_source
    from {{ source('sources', 'champs_disponibles_sources') }}
    where categorie = 'Population_Generale'
{% endset %}

{% set bases = run_query(bases_query) %}
{% set bases_sources = [] %}
{% for base in bases %}
    {% do bases_sources.append(base[0]) %}
{% endfor %}
{% do log("Bases sources trouvées : " ~ bases_sources | join(", "), info=True) %}

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
{% for cte in cte_names %}
    {% do log("Génération CTE : " ~ cte, info=True) %}
    {{ cte }} as (
        {{ transform_column_names(
            source('sources', cte),
            cte.split('_')[-1],
            'Population_Generale'
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

{# Générer dynamiquement la liste des colonnes renommées pour le SELECT final #}
{% set all_columns = [] %}
{% for base in bases_sources %}
    {% set suffix = (annees[0]|string)[2:] %}
    {% set mapping_query %}
        select distinct champ_insee_transfo, clef_json_transfo
        from {{ source('sources', 'champs_disponibles_sources') }}
        where categorie = 'Population_Generale'
    {% endset %}
    {% set mapping = run_query(mapping_query) %}
    {% set mapping_dict = {} %}
    {% for m in mapping %}
        {% do mapping_dict.update({m[0]: m[1]}) %}
    {% endfor %}
    {% set columns = adapter.get_columns_in_relation(source('sources', base ~ '_' ~ annees[0])) %}
    {% for col in columns %}
        {% set colname = col.name %}
        {% if colname == 'CODGEO' %}
            {# on ne met pas CODGEO ici, il est déjà dans le coalesce #}
        {% elif colname.startswith('P' ~ suffix ~ '_') or colname.startswith('C' ~ suffix ~ '_') %}
            {% set base_name = colname[0] ~ '_' ~ colname.split('_')[1:] | join('_') %}
            {% if base_name in mapping_dict %}
                {% do all_columns.append('"' ~ base ~ '_unified"."'+ mapping_dict[base_name] + '"') %}
            {% endif %}
        {% endif %}
    {% endfor %}
{% endfor %}

-- Requête finale
select
    coalesce(
        {% for base in bases_sources %}
            "{{ base }}_unified"."CODGEO"{% if not loop.last %}, {% endif %}
        {% endfor %}
    ) as "CODGEO",
    {{ all_columns | join(', ') }},
    {% for base in bases_sources %}
        "{{ base }}_unified"."annee"{% if not loop.last %}, {% endif %}
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

{% do print("✨ Génération SQL terminée") %}