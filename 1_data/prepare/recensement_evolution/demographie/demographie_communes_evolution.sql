{{ config(materialized='table') }}

{% set annees = [2016, 2017, 2018, 2019, 2020, 2021] %}

-- On crée un CTE par année
with
{% for annee in annees %}
-- Deux derniers chiffres de l'année (ex: 18 pour 2018)
{% set suffix = (annee|string)|slice(-2, 2) %}

base_{{ annee }} as (

    {% set cols = dbt_utils.get_filtered_columns_in_source(
        'sources',
        'base_cc_evol_struct_pop_' ~ annee,
        "substring(column_name from 2 for 2) = '" ~ suffix ~ "' or column_name = 'code_commune'"
    ) %}

    select
        {% for col in cols %}
            {% if col != 'code_commune' %}
                {{ col }} as {{ col[3:] }}{% if not loop.last %},{% endif %}
            {% else %}
                {{ col }}{% if not loop.last %},{% endif %}
            {% endif %}
        {% endfor %},
        {{ annee }} as annee
    from {{ source('sources', 'base_cc_evol_struct_pop_' ~ annee) }}
){% if not loop.last %},{% endif %}

{% endfor %}

-- Union finale
select * from base_2016
{% for annee in annees[1:] %}
union all
select * from base_{{ annee }}
{% endfor %}
