{% macro generate_category_files(category, base_table_prefix) %}
  {# This macro generates the schema.yml and communes evolution file for a category #}
  
  {# Schema.yml content #}
  {% set schema_content %}
version: 2

sources:
  - name: sources
    schema: sources
    tables:
      - name: {{ base_table_prefix }}_2016
      - name: {{ base_table_prefix }}_2017
      - name: {{ base_table_prefix }}_2018
      - name: {{ base_table_prefix }}_2019
      - name: {{ base_table_prefix }}_2020
      - name: {{ base_table_prefix }}_2021
      - name: champs_disponibles_sources

  - name: prepare
    schema: prepare
    tables:
      - name: infos_communes

models:
  - name: {{ category }}_communes_evolution
    description: "Évolution {{ category }} au niveau communal"
  - name: {{ category }}_departements_evolution
    description: "Évolution {{ category }} au niveau départemental"
  - name: {{ category }}_EPCI_evolution
    description: "Évolution {{ category }} au niveau EPCI"
  - name: {{ category }}_SCoT_evolution
    description: "Évolution {{ category }} au niveau SCoT"
  {% endset %}

  {# Communes evolution SQL content #}
  {% set communes_sql_content %}
{{ "{{" }} config(materialized='table', schema='prepare') {{ "}}" }}

with infos_communes as (
    select 
        ltrim(code_commune, '0') as code_commune,
        nom_commune,
        code_arrondissement,
        code_departement,
        code_region,
        nom_departement,
        nom_region,
        "SCoT" as nom_scot,
        "SIREN EPCI" as siren_epci
    from {{ "{{" }} source('prepare', 'infos_communes') {{ "}}" }}
),

evolution_data as (
    {{ "{{" }} safe_generate_evolution_table(
        category='{{ category }}',
        start_year=2016,
        end_year=2021
    ) {{ "}}" }}
)

select 
    e.*,
    i.nom_commune,
    i.code_departement,
    i.siren_epci,
    i.nom_departement,
    i.code_region,
    i.nom_region,
    i.nom_scot
from evolution_data e
left join infos_communes i on ltrim(e."CODGEO", '0') = i.code_commune
  {% endset %}

  {{ log("Schema content for " ~ category ~ ":", info=true) }}
  {{ log(schema_content, info=true) }}
  {{ log("SQL content for " ~ category ~ ":", info=true) }}
  {{ log(communes_sql_content, info=true) }}

{% endmacro %}
