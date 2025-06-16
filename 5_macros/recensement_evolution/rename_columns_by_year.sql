{% macro transform_column_names(relation, annee, categorie='population_generale') %}
    {% set suffix = (annee|string)[2:] %}
    
    {% set mapping_query %}
        select distinct 
            champ_insee_transfo,
            clef_json_transfo
        from {{ ref('champs_disponibles_categorises') }}
        where categorie = '{{ categorie }}'
    {% endset %}
    
    {% set mapping = run_query(mapping_query) %}
    {% set mapping_dict = {} %}
    {% for m in mapping %}
        {% do mapping_dict.update({m[0]: m[1]}) %}
    {% endfor %}

    {% set columns = adapter.get_columns_in_relation(relation) %}
    {% set selected_columns = [] %}

    {% for col in columns %}
        {% set colname = col.name %}
        
        {% if colname == 'CODGEO' %}
            {% do selected_columns.append('\"' ~ colname ~ '\" as \"' ~ colname ~ '\"') %}
        
        {% elif colname.startswith('P' ~ suffix ~ '_') or colname.startswith('C' ~ suffix ~ '_') %}
            {% set base_name = colname[0] ~ '_' ~ colname.split('_')[1:] | join('_') %}
            
            {# Si le nom de base existe dans notre mapping #}
            {% if base_name in mapping_dict %}
                {% do selected_columns.append('\"' ~ colname ~ '\" as \"' ~ mapping_dict[base_name] ~ '\"') %}
            {% endif %}
        {% endif %}
    {% endfor %}

    select
        {{ selected_columns | join(',\n        ') }},
        {{ annee }} as annee
    from {{ relation }}
{% endmacro %}