{% macro safe_demographie_col_expr(champ_insee, clef_json, annee, alias) %}

    {% set yy = (annee|string)[-2:] %}

    {% if champ_insee.endswith('_P') or champ_insee.endswith('_C') or champ_insee.endswith('_F') %}
        {% set letter = champ_insee[-1]|upper %}
        {% set body   = champ_insee[:-2] %}
    {% else %}
        {% set letter = 'P' %}
        {% set body   = champ_insee %}
    {% endif %}

    {% set col_name = letter ~ yy ~ '_' ~ body %}

    {% set tbl %}
        {% if alias.startswith('sp') %}
            base_cc_evol_struct_pop_{{ annee }}
        {% else %}
            base_cc_coupl_fam_men_{{ annee }}
        {% endif %}
    {% endset %}
    {% set src = source('sources', tbl.strip()) %}
    {% set schema = src.schema %}
    {% set table  = src.identifier %}

    {% if execute %}
        {% set q_exists %}
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = '{{ schema }}'
              AND table_name   = '{{ table }}'
              AND column_name  = '{{ col_name }}'
            LIMIT 1
        {% endset %}
        {% set exists = run_query(q_exists).rows | length > 0 %}
    {% else %}
        {% set exists = true %}
    {% endif %}

    {% if not exists %}
        {{ log('⚠️  Colonne absente (' ~ alias ~ '.' ~ col_name ~ ') : remplissage NULL', info=True) }}
    {% endif %}

    {% if exists %}
        {{ alias }}."{{ col_name }}" AS "{{ annee }}_{{ clef_json }}"
    {% else %}
        NULL AS "{{ annee }}_{{ clef_json }}"
    {% endif %}
{% endmacro %}


{% macro generate_demographie_columns_year(champs, annee, alias) %}
    {% set cols = [] %}
    {% for c in champs %}
        {% do cols.append(
            safe_demographie_col_expr(c.champ_insee, c.clef_json, annee, alias)
        ) %}
    {% endfor %}
    {{ cols | join(',\n        ') }}

{% endmacro %}
