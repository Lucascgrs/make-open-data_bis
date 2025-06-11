{% macro demographie_col_expr(champ_insee, clef_json, annee, alias) %}

    {% set yy = (annee|string)[-2:] %}

    {% if champ_insee.endswith('_P') or
          champ_insee.endswith('_C') or
          champ_insee.endswith('_F') or
          (champ_insee.endswith('_' ~ champ_insee[-1]) and
           champ_insee[-1]|length == 1 and
           champ_insee[-2] == '_' ) %}

        {% set letter = champ_insee[-1]|upper %}
        {% set body   = champ_insee[:-2] %}   {# on coupe "_X" #}
    {% else %}
        {% set letter = 'P' %}
        {% set body   = champ_insee %}
    {% endif %}

    {% set col = letter ~ yy ~ '_' ~ body %}

    {{ alias }}."{{ col }}" AS {{ clef_json }}_{{ annee }}
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
