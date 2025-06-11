{{ config(materialized='table') }}

{% set annees = range(2016, 2022) %}  {# 2016 → 2021 inclus #}

{% if execute %}
    {% set cols_by_alias = {} %}

    {% for annee in annees %}
        {% set alias  = 'sp' ~ annee %}
        {% set src    = source('sources', 'base_cc_evol_struct_pop_' ~ annee) %}
        {% set schema = src.schema %}
        {% set table  = src.identifier %}

        {% set sql_cols %}
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = '{{ schema }}'
              AND table_name   = '{{ table }}'
        {% endset %}
        {% set table_cols = run_query(sql_cols).rows | map(attribute=0) | list %}
        {{ log('→ columns trouvées pour ' ~ alias ~ ' : ' ~ (table_cols | length), info=True) }}
        {% do cols_by_alias.update({ alias: table_cols }) %}

        {% set alias  = 'fm' ~ annee %}
        {% set src    = source('sources', 'base_cc_coupl_fam_men_' ~ annee) %}
        {% set schema = src.schema %}
        {% set table  = src.identifier %}

        {% set sql_cols %}
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = '{{ schema }}'
              AND table_name   = '{{ table }}'
        {% endset %}
        {% set table_cols = run_query(sql_cols).rows | map(attribute=0) | list %}
        {{ log('→ columns trouvées pour ' ~ alias ~ ' : ' ~ (table_cols | length), info=True) }}
        {% do cols_by_alias.update({ alias: table_cols }) %}
    {% endfor %}
{% else %}
    {% set cols_by_alias = {} %}
{% endif %}

{% set pop_champs = [] %}
{% set fam_champs = [] %}
{% set seen_pop = [] %}
{% set seen_fam = [] %}
{% for row in champs_raw %}
    {% set clef   = row[1] %}
    {% set record = {'champ_insee': row[0], 'clef_json': clef} %}

    {% if row[2] == 'rp_population' %}
        {% if clef not in seen_pop %}
            {% do pop_champs.append(record) %}
            {% do seen_pop.append(clef) %}
        {% endif %}

    {% elif row[2] == 'rp_familles_menages' %}
        {% if clef not in seen_fam %}
            {% do fam_champs.append(record) %}
            {% do seen_fam.append(clef) %}
        {% endif %}
    {% endif %}
{% endfor %}

{{ log('→ pop_champs : ' ~ (pop_champs | length) ~ '  |  fam_champs : ' ~ (fam_champs | length), info=True) }}

with
{% for annee in annees %}
struct_pop_{{ annee }} as (
    select
        lpad(cast("CODGEO" as text), 5, '0')              AS code_commune,
        {{ generate_demographie_columns_year(
               pop_champs, annee, 'sp' ~ annee) }}
    from {{ source('sources', 'base_cc_evol_struct_pop_' ~ annee) }}  sp{{ annee }}
),
{% endfor %}

{% for annee in annees %}
fam_men_{{ annee }} as (
    select
        lpad(cast("CODGEO" as text), 5, '0')              AS code_commune,
        {{ generate_demographie_columns_year(
               fam_champs, annee, 'fm' ~ annee) }}
    from {{ source('sources', 'base_cc_coupl_fam_men_' ~ annee) }}     fm{{ annee }}
){% if not loop.last %},{% endif %}
{% endfor %}

, rp_population as (
    select * from struct_pop_2016
    {% for annee in annees if annee != 2016 %}
        full outer join struct_pop_{{ annee }} using (code_commune)
    {% endfor %}
)

, rp_familles_menages as (
    select * from fam_men_2016
    {% for annee in annees if annee != 2016 %}
        full outer join fam_men_{{ annee }} using (code_commune)
    {% endfor %}
)

select
    coalesce(rp_population.code_commune,
             rp_familles_menages.code_commune) AS code_commune,

    -- Colonnes Population 2016-2021
    {% for annee in annees %}
        {{ generate_demographie_columns_year(pop_champs, annee, 'rp_population') }},{% endfor %}

    -- Colonnes Familles/Ménages 2016-2021
    {% for annee in annees %}
        {{ generate_demographie_columns_year(fam_champs, annee, 'rp_familles_menages') }}{% if not loop.last %},{% endif %}
    {% endfor %}

from rp_population
full outer join rp_familles_menages using (code_commune)