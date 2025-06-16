{# Configuration du modèle #}
{{ config(materialized='table', schema='prepare') }}

{% set years = [2016, 2017, 2018, 2019, 2020, 2021] %}

{# Récupération des tables sources de la catégorie Population Générale #}
{% set source_tables_query %}
    select distinct base_table_source from {{ source('sources', 'champs_disponibles_sources') }} where categorie = 'Population_Generale'
{% endset %}

{# Exécution de la requête et stockage des noms de tables sources #}
{% set source_tables = run_query(source_tables_query) %}
{% set source_table_names = [] %}
{% for table in source_tables %}
    {% do source_table_names.append(table[0]) %}
{% endfor %}

{# Génération des noms de CTE pour chaque combinaison table/année #}
{% set cte_identifiers = [] %}
{% for table_name in source_table_names %}
    {% for year in years %}
        {% do cte_identifiers.append(table_name ~ '_' ~ year) %}
    {% endfor %}
{% endfor %}

{# Génération des CTEs pour transformation des colonnes #}
with
{% for cte_name in cte_identifiers %}
    {{ cte_name }} as (
        {{ transform_column_names(
            source('sources', cte_name),
            cte_name.split('_')[-1],
            'Population_Generale'
        ) }}
    ){% if not loop.last %},{% endif %}
{% endfor %}

{# Union des données par table source à travers les années #}
{% for table_name in source_table_names %}
,{{ table_name }}_unified as (
    select * from {{ table_name }}_{{ years[0] }}
    {% for year in years[1:] %}
        union all
        select * from {{ table_name }}_{{ year }}
    {% endfor %}
)
{% endfor %}

{# Préparation des colonnes pour la requête finale #}
{% set final_columns = [] %}
{% for table_name in source_table_names %}
    {% set year_suffix = (years[0]|string)[2:] %}
    {% set column_mapping_query %}
        select distinct champ_insee_transfo, clef_json_transfo
        from {{ source('sources', 'champs_disponibles_sources') }}
        where categorie = 'Population_Generale'
    {% endset %}
    
    {% set mapping_results = run_query(column_mapping_query) %}
    {% set column_mapping = {} %}
    {% for mapping in mapping_results %}
        {% do column_mapping.update({mapping[0]: mapping[1]}) %}
    {% endfor %}
    
    {% set table_columns = adapter.get_columns_in_relation(source('sources', table_name ~ '_' ~ years[0])) %}
    {% for column in table_columns %}
        {% set column_name = column.name %}
        {% if column_name != 'CODGEO' %}
            {% if column_name.startswith('P' ~ year_suffix ~ '_') or column_name.startswith('C' ~ year_suffix ~ '_') %}
                {% set base_column_name = column_name[0] ~ '_' ~ column_name.split('_')[1:] | join('_') %}
                {% if base_column_name in column_mapping %}
                    {% do final_columns.append('\"' ~ table_name ~ '_unified\".\"'+ column_mapping[base_column_name] + '\"') %}
                {% endif %}
            {% endif %}
        {% endif %}
    {% endfor %}
{% endfor %}

{# Requête finale combinant toutes les données #}
select
    coalesce(
        {% for table_name in source_table_names %}
            "{{ table_name }}_unified"."CODGEO"{% if not loop.last %}, {% endif %}
        {% endfor %}
    ) as "CODGEO",
    {{ final_columns | join(', ') }},
    {% for table_name in source_table_names %}
        "{{ table_name }}_unified"."annee"{% if not loop.last %}, {% endif %}
    {% endfor %}

from

{% for table_name in source_table_names %}
    {{ table_name }}_unified
    {% if not loop.last %}
    full outer join
    {% endif %}
{% endfor %}
{% if source_table_names|length > 1 %}
    on
    {% for table_name in source_table_names %}
        {% if not loop.first %}
            {{ source_table_names[0] }}_unified.CODGEO = {{ table_name }}_unified.CODGEO
            {% if not loop.last %} and {% endif %}
        {% endif %}
    {% endfor %}
{% endif %}