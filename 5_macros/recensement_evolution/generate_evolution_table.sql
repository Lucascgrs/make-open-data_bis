{% macro generate_evolution_table(category, start_year, end_year) %}
    {# Simplified category to table mapping #}
    {% set category_table_mapping = {
        'Categories_Socioprofessionnelles': 'base_cc_carac_emploi',
        'Chomage': 'base_cc_carac_emploi',
        'Education_Formation': 'base_cc_carac_emploi',
        'Emploi_Complement': 'base_cc_carac_emploi',
        'Emplois': 'base_cc_carac_emploi',
        'Population_Active': 'base_cc_carac_emploi',
        'Logement_Caracteristiques': 'base_cc_logement',
        'Logement_Occupation': 'base_cc_logement',
        'Logement_Parc': 'base_cc_logement',
        'Logement_Type': 'base_cc_logement',
        'Menage_Famille': 'base_cc_logement',
        'Population_Age': 'base_cc_caracteristiques_population',
        'Population_Generale': 'base_cc_caracteristiques_population',
        'Population_Sexe': 'base_cc_caracteristiques_population',
        'Revenus': 'base_cc_filosofi'
    } %}
    
    {% if category in category_table_mapping %}
        {% set forced_table = category_table_mapping[category] %}
    {% else %}
        {# Auto-detect table for unknown categories #}
        {% set source_tables_query %}
            select distinct base_table_source 
            from {{ source('sources', 'champs_disponibles_sources') }} 
            where categorie = '{{ category }}'
            and base_table_source is not null
            and base_table_source != ''
            and base_table_source != 'None'
            order by base_table_source
            limit 1
        {% endset %}
        
        {% set source_tables = run_query(source_tables_query) %}
        {% if source_tables and source_tables.rows|length > 0 %}
            {% set forced_table = source_tables.rows[0][0] %}
        {% else %}
            {% set forced_table = null %}
        {% endif %}
    {% endif %}

    {{ generate_evolution_table_implementation(category, start_year, end_year, forced_table) }}
{% endmacro %}

{% macro generate_evolution_table_implementation(category, start_year, end_year, forced_table) %}
    {% if not forced_table %}
        {# Return empty table if no source table found #}
        select 
            null::text as "CODGEO",
            null::integer as annee
        where false
    {% else %}
        {# Generate list of years #}
        {% set years = [] %}
        {% for year in range(start_year, end_year + 1) %}
            {% do years.append(year) %}
        {% endfor %}

        {# Get all possible columns for this category and table #}
        {% set all_columns_query %}
            select distinct clef_json_transfo
            from {{ source('sources', 'champs_disponibles_sources') }}
            where categorie = '{{ category }}'
            and base_table_source = '{{ forced_table }}'
            order by clef_json_transfo
        {% endset %}

        {% set all_columns_result = run_query(all_columns_query) %}
        {% set all_possible_columns = [] %}
        {% for col in all_columns_result %}
            {% do all_possible_columns.append(col[0]) %}
        {% endfor %}

        {# Generate CTEs with column transformation #}
        with
        {% for year in years %}
            {{ forced_table }}_{{ year }} as (
                {{ transform_column_names_robust(
                    source('sources', forced_table ~ '_' ~ year),
                    year,
                    category,
                    all_possible_columns
                ) }}
            ){% if not loop.last %},{% endif %}
        {% endfor %}

        {# Union all years #}
        {% for year in years %}
            {% if not loop.first %}union all{% endif %}
            select 
                "CODGEO",
                {% for col in all_possible_columns -%}
                    "{{ col }}"{% if not loop.last %},{% endif %}
                {% endfor -%}
                {% if all_possible_columns|length > 0 %},{% endif %}
                annee
            from {{ forced_table }}_{{ year }}
        {% endfor %}
    {% endif %}
{% endmacro %}