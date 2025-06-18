{% macro generate_evolution_table(category, start_year, end_year) %}
    {# Génération de la liste des années #}
    {% set years = [] %}
    {% for year in range(start_year, end_year + 1) %}
        {% do years.append(year) %}
    {% endfor %}

    {# Récupération de toutes les colonnes possibles pour cette catégorie #}
    {% set all_columns_query %}
        select distinct clef_json_transfo
        from {{ source('sources', 'champs_disponibles_sources') }}
        where categorie = '{{ category }}'
        order by clef_json_transfo
    {% endset %}

    {% set all_columns_result = run_query(all_columns_query) %}
    {% set all_possible_columns = [] %}
    {% for col in all_columns_result %}
        {% do all_possible_columns.append(col[0]) %}
    {% endfor %}

    {# Récupération des tables sources pour la catégorie donnée #}
    {% set source_tables_query %}
        select distinct base_table_source 
        from {{ source('sources', 'champs_disponibles_sources') }} 
        where categorie = '{{ category }}'
    {% endset %}

    {% set source_tables = run_query(source_tables_query) %}
    {% set source_table_names = [] %}
    {% for table in source_tables %}
        {% do source_table_names.append(table[0]) %}
    {% endfor %}

    {# Génération des CTEs avec transformation de colonnes #}
    with
    {% for table_name in source_table_names %}
        {% for year in years %}
            {{ table_name }}_{{ year }} as (
                {{ transform_column_names_robust(
                    source('sources', table_name ~ '_' ~ year),
                    year,
                    category,
                    all_possible_columns
                ) }}
            ),
        {% endfor %}
    {% endfor %}

    {# Union des données par table source #}
    {% for table_name in source_table_names %}
    {{ table_name }}_unified as (
        {% for year in years %}
            {% if not loop.first %}union all{% endif %}
            select 
                "CODGEO",
                {% for col in all_possible_columns %}
                    "{{ col }}",
                {% endfor %}
                annee
            from {{ table_name }}_{{ year }}
        {% endfor %}
    ){% if not loop.last %},{% endif %}
    {% endfor %}

    {# Requête finale avec jointure des tables unifiées #}
    select
        {% for table_name in source_table_names %}
            {% if loop.first %}
                {{ table_name }}_unified."CODGEO",
            {% endif %}
        {% endfor %}
        {% for col in all_possible_columns %}
            {% for table_name in source_table_names %}
                {% if loop.first %}
                    coalesce(
                {% endif %}
                {{ table_name }}_unified."{{ col }}"
                {% if not loop.last %}, {% else %}) as "{{ col }}"{% if not loop.last %},{% endif %}{% endif %}
            {% endfor %}
        {% endfor %}
        {% if all_possible_columns|length > 0 %},{% endif %}
        {% for table_name in source_table_names %}
            {% if loop.first %}
                {{ table_name }}_unified.annee
            {% endif %}
        {% endfor %}

    from {{ source_table_names[0] }}_unified
    {% for table_name in source_table_names[1:] %}
        full outer join {{ table_name }}_unified
            on {{ source_table_names[0] }}_unified."CODGEO" = {{ table_name }}_unified."CODGEO"
            and {{ source_table_names[0] }}_unified.annee = {{ table_name }}_unified.annee
    {% endfor %}

{% endmacro %}