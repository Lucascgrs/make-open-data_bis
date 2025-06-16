{% macro transform_column_names(relation, year, category='Population_Generale') %}
    {% set year_suffix = (year|string)[2:] %}

    {# Récupération du mapping des noms de colonnes #}
    {% set column_mapping_query %}
        select distinct 
            champ_insee_transfo,
            clef_json_transfo
        from {{ source('sources', 'champs_disponibles_sources') }}
        where categorie = '{{ category }}'
    {% endset %}

    {# Création du dictionnaire de mapping #}
    {% set mapping_results = run_query(column_mapping_query) %}
    {% set column_name_mapping = {} %}
    {% for result in mapping_results %}
        {% do column_name_mapping.update({result[0]: result[1]}) %}
    {% endfor %}

    {# Transformation des noms de colonnes #}
    {% set columns = adapter.get_columns_in_relation(relation) %}
    {% set transformed_columns = [] %}

    {% for column in columns %}
        {% set original_column_name = column.name %}
        
        {# Traitement spécial pour CODGEO #}
        {% if original_column_name == 'CODGEO' %}
            {% do transformed_columns.append('\"' ~ original_column_name ~ '\" as \"' ~ original_column_name ~ '\"') %}
        
        {# Traitement des colonnes de population et autres métriques #}
        {% elif original_column_name.startswith('P' ~ year_suffix ~ '_') or original_column_name.startswith('C' ~ year_suffix ~ '_') %}
            {% set base_column_name = original_column_name[0] ~ '_' ~ original_column_name.split('_')[1:] | join('_') %}
            {% if base_column_name in column_name_mapping %}
                {% do transformed_columns.append('\"' ~ original_column_name ~ '\" as \"' ~ column_name_mapping[base_column_name] ~ '\"') %}
            {% endif %}
        {% endif %}
    {% endfor %}

    {# Génération de la requête finale #}
    select
        {{ transformed_columns | join(',\n        ') }},
        {{ year }} as annee
    from {{ relation }}
{% endmacro %}