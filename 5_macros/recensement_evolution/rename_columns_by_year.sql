{% macro rename_columns_by_year(relation, annee, colonnes_fix = ['code_commune']) %}
    {% set suffix = (annee|string)[2:] %}

    {% set all_columns = adapter.get_columns_in_relation(relation) %}

    {% set selected_columns = [] %}

    {% for col in all_columns %}
        {% set colname = col.name %}

        {% if colname in colonnes_fix %}
            {% do selected_columns.append('"' ~ colname ~ '" as "' ~ colname ~ '"') %}

        {% elif colname.startswith('P' ~ suffix ~ '_') 
            or colname.startswith('C' ~ suffix ~ '_') %}

            {% set new_name = colname[0] ~ colname[4:] %}
            {% do selected_columns.append('"' ~ colname ~ '" as ' ~ new_name) %}

        {% endif %}
    {% endfor %}

    select
        {{ selected_columns | join(',\n        ') }},
        {{ annee }} as annee
    from {{ relation }}
{% endmacro %}