{% macro generate_evolution_table(category, start_year, end_year) %}
    {# Génération de la liste des années #}
    {% set years = [] %}
    {% for year in range(start_year, end_year + 1) %}
        {% do years.append(year) %}
    {% endfor %}

    {# Récupération des tables sources valides pour la catégorie donnée #}
    {% set source_tables_query %}
        select distinct base_table_source 
        from {{ source('sources', 'champs_disponibles_sources') }} 
        where categorie = '{{ category }}'
        and base_table_source is not null
        and base_table_source != ''
        and base_table_source != 'None'
    {% endset %}

    {% set source_tables = run_query(source_tables_query) %}
    {% set source_table_names = [] %}
    {% for table in source_tables %}
        {% if table[0] and table[0] != 'None' and table[0] != '' %}
            {% do source_table_names.append(table[0]) %}
        {% endif %}
    {% endfor %}

    {# Vérifier qu'on a au moins une table source #}
    {% if source_table_names|length == 0 %}
        {# Retourner une table vide avec la structure attendue #}
        select 
            null::text as "CODGEO",
            null::integer as annee,
            null::text as nom_commune,
            null::text as code_departement,
            null::text as siren_epci,
            null::text as nom_departement,
            null::text as code_region,
            null::text as nom_region,
            null::text as nom_scot
        where false
    {% else %}

    {# Récupération de toutes les colonnes possibles pour cette catégorie #}
    {% set all_columns_query %}
        select distinct clef_json_transfo
        from {{ source('sources', 'champs_disponibles_sources') }}
        where categorie = '{{ category }}'
        and base_table_source is not null
        and base_table_source != ''
        and base_table_source != 'None'
        order by clef_json_transfo
    {% endset %}

    {% set all_columns_result = run_query(all_columns_query) %}
    {% set all_possible_columns = [] %}
    {% for col in all_columns_result %}
        {% do all_possible_columns.append(col[0]) %}
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
        {{ source_table_names[0] }}_unified."CODGEO",
        {% for col in all_possible_columns %}
            {% if source_table_names|length == 1 %}
                {{ source_table_names[0] }}_unified."{{ col }}"
            {% else %}
                coalesce(
                    {% for table_name in source_table_names %}
                        {{ table_name }}_unified."{{ col }}"{% if not loop.last %}, {% endif %}
                    {% endfor %}
                ) as "{{ col }}"
            {% endif %}{% if not loop.last %},{% endif %}
        {% endfor %}
        {% if all_possible_columns|length > 0 %},{% endif %}
        {{ source_table_names[0] }}_unified.annee

    from {{ source_table_names[0] }}_unified
    {% for table_name in source_table_names[1:] %}
        full outer join {{ table_name }}_unified
            on {{ source_table_names[0] }}_unified."CODGEO" = {{ table_name }}_unified."CODGEO"
            and {{ source_table_names[0] }}_unified.annee = {{ table_name }}_unified.annee
    {% endfor %}

    {% endif %}

{% endmacro %}