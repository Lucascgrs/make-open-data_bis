{{ config(materialized='table', schema='prepare') }}

with commune_data as (
    select * from {{ ref('Population_Generale_communes_evolution') }}
),

-- Get numeric columns dynamically for aggregation
numeric_columns as (
    select 
        code_departement,
        code_region,
        "annee",
        -- Sum all population-related numeric columns based on actual column names
        sum("p_pop") as "p_pop"
    from commune_data
    where code_departement is not null
    group by 
        code_departement,
        code_region,
        "annee"
)

select 
    code_departement as "CODGEO",
    code_region,
    "annee",
    "p_pop"
from numeric_columns
order by code_departement, "annee"
