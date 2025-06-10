{% macro demographie_col_expr(champ_insee, clef_json, annee, alias) %}
    {#
        Construit, par ex. :
        {{ demographie_col_expr('POP_P', 'pop_p', 2018, 'sp2018') }}
        → sp2018.P18_POP AS pop_p_2018
    #}
    {% set parts  = champ_insee.split('_') %}
    {% set suffix = parts[0] %}
    {% set prefix = parts[1] | upper %}
    {{ alias }}.{{ prefix }}{{ (annee|string)[-2:] }}_{{ suffix }}
        AS {{ clef_json }}_{{ annee }}
{% endmacro %}

{% macro generate_demographie_columns_year(champs, annee, alias) %}
    {# Champs d’un **seul** millésime, avec l’alias de table donné #}
    {% set cols = [] %}
    {% for c in champs %}
        {% do cols.append(
            demographie_col_expr(c.champ_insee, c.clef_json, annee, alias)
        ) %}
    {% endfor %}
    {{ cols | join(',\n        ') }}
{% endmacro %}

{% macro list_alias_columns(champs, annees, table_alias) %}
    {# Retourne table_alias.clef_json_annee pour chaque couple #}
    {% set cols = [] %}
    {% for c in champs %}
        {% for annee in annees %}
            {% do cols.append(table_alias ~ '.' ~ c.clef_json ~ '_' ~ annee) %}
        {% endfor %}
    {% endfor %}
    {{ cols | join(',\n    ') }}
{% endmacro %}
