{% macro demographie_col_expr(champ_insee, clef_json, annee) %}
    {#
        Transforme, par ex. :
          champ_insee = 'POP_P', clef_json = 'pop_p', annee = 2018
        en :
          P18_POP as pop_p_2018
    #}
    {% set parts  = champ_insee.split('_') %}
    {% set suffix = parts[0] %}
    {% set prefix = parts[1] | upper %}

    {{ prefix }}{{ (annee|string)[-2:] }}_{{ suffix }}
        as {{ clef_json }}_{{ annee }}
{% endmacro %}

{% macro generate_demographie_columns(champs, annees) %}
    {# 
        champs  : liste de dicts     [{'champ_insee': ..., 'clef_json': ...}, …]
        annees  : iterable           [2016, 2017, …, 2021]
    #}
    {%- set cols = [] -%}
    {%- for row in champs -%}
        {%- for annee in annees -%}
            {%- do cols.append(demographie_col_expr(row.champ_insee, row.clef_json, annee)) -%}
        {%- endfor -%}
    {%- endfor -%}
    {{ cols | join(',\n        ') }}
{% endmacro %}
