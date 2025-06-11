{% macro demographie_col_expr(champ_insee, clef_json, annee, alias) %}

    {% set parts  = champ_insee.split('_') %}
    {% set yy     = (annee|string)[-2:] %}

    {% if parts|length == 2 and parts[1]|length == 1 %}
        {# Exemple : POP_P  →  P16_POP #}
        {% set col = 'P' ~ yy ~ '_' ~ parts[0] %}
    {% else %}
        {# Exemple : POP01P_IRAN1  →  P16__POP01P_IRAN1 #}
        {% set col = 'P' ~ yy ~ '__' ~ champ_insee %}
    {% endif %}

    {{ alias }}.{{ col }} AS {{ clef_json }}_{{ annee }}
{% endmacro %}

{% macro generate_demographie_columns_year(champs, annee, alias) %}
    {% set cols = [] %}
    {% for c in champs %}
        {% do cols.append(
            demographie_col_expr(c.champ_insee, c.clef_json, annee, alias)
        ) %}
    {% endfor %}
    {{ cols | join(',\n        ') }}
{% endmacro %}
