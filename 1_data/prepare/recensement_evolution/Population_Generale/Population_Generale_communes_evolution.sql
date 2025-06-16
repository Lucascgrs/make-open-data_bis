{{ config(
    materialized='table',
    schema='prepare'
) }}

{{ generate_evolution_table(
    category='Population_Generale',
    start_year=2016,
    end_year=2021
) }}