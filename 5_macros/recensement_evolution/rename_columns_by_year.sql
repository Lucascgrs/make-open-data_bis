{% macro transform_column_names(relation, annee, categorie='Population_Generale') %}
    {% set suffix = (annee|string)[2:] %}

    {% do log("=== [transform_column_names] Start for relation: " ~ relation ~ ", annee: " ~ annee ~ ", categorie: " ~ categorie, info=True) %}

    {% set mapping_query %}
        select distinct 
            champ_insee_transfo,
            clef_json_transfo
        from {{ source('sources', 'champs_disponibles_sources') }}
        where categorie = '{{ categorie }}'
    {% endset %}

    {% set mapping = run_query(mapping_query) %}
    {% set mapping_dict = {} %}
    {% for m in mapping %}
        {% do mapping_dict.update({m[0]: m[1]}) %}
    {% endfor %}
    {% do log("Mapping dict: " ~ mapping_dict, info=True) %}

    {% set columns = adapter.get_columns_in_relation(relation) %}
    {% set selected_columns = [] %}
    {% set mapped_names = [] %}
    {% for col in columns %}
        {% set colname = col.name %}
        {% do log("Traitement colonne: " ~ colname, info=True) %}
        
        {% if colname == 'CODGEO' %}
            {% do selected_columns.append('\"' ~ colname ~ '\" as \"' ~ colname ~ '\"') %}
            {% do mapped_names.append(colname) %}
            {% do log("Ajout colonne CODGEO", info=True) %}
        
        {% elif colname.startswith('P' ~ suffix ~ '_') or colname.startswith('C' ~ suffix ~ '_') %}
            {% set base_name = colname[0] ~ '_' ~ colname.split('_')[1:] | join('_') %}
            {% do log("Colonne potentielle à mapper: " ~ colname ~ " | base_name: " ~ base_name, info=True) %}
            {% if base_name in mapping_dict %}
                {% do selected_columns.append('\"' ~ colname ~ '\" as \"' ~ mapping_dict[base_name] ~ '\"') %}
                {% do mapped_names.append(mapping_dict[base_name]) %}
                {% do log("Mapping trouvé: " ~ base_name ~ " → " ~ mapping_dict[base_name], info=True) %}
            {% else %}
                {% do log("Aucun mapping trouvé pour: " ~ base_name, info=True) %}
            {% endif %}
        {% endif %}
    {% endfor %}

    {% do log("Colonnes sélectionnées (SQL): " ~ selected_columns, info=True) %}
    {% do log("Noms finaux de colonnes (pour CTE): " ~ mapped_names, info=True) %}

    select
        {{ selected_columns | join(',\n        ') }},
        {{ annee }} as annee
    from {{ relation }}
    {% do log("=== [transform_column_names] Fin macro pour: " ~ relation, info=True) %}
{% endmacro %}