{% macro safe_generate_evolution_table(category, start_year, end_year) %}
    {# Génération de la liste des années #}
    {% set years = [] %}
    {% for year in range(start_year, end_year + 1) %}
        {% do years.append(year) %}
    {% endfor %}

    {# Try to get source tables, but handle case where they don't exist #}
    {% set source_tables_query %}
        select distinct base_table_source 
        from {{ source('sources', 'champs_disponibles_sources') }} 
        where categorie = '{{ category }}'
    {% endset %}

    {# Check if the metadata table exists and has data for this category #}
    {% if execute %}
        {% set source_tables_result = run_query(source_tables_query) %}
        {% if source_tables_result.rows | length > 0 %}
            {% set source_table_names = [] %}
            {% for table in source_tables_result %}
                {% do source_table_names.append(table[0]) %}
            {% endfor %}
            
            {# Generate the normal evolution table #}
            {{ generate_evolution_table(category, start_year, end_year) }}
        {% else %}
            {# Category not found in metadata, create empty placeholder #}
            select 
                null as "CODGEO",
                null as "annee"
            where 1=0  -- Empty result set
        {% endif %}
    {% else %}
        {# During parsing phase, create a placeholder #}
        select 
            null as "CODGEO",
            null as "annee"
        where 1=0
    {% endif %}

{% endmacro %}
