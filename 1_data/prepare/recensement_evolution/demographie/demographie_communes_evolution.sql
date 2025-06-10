{{ config(materialized='table') }}

with
rp_population as (        -- base rp_population
    select
        lpad(cast(code_commune_insee as text), 5, '0') as code_commune,
        P15_POP        as pop_p_2015,
        P16_POP        as pop_p_2016,
        …
),                        -- 1 040 colonnes au total
rp_familles_menages as (  -- base rp_familles_menages
    select
        lpad(cast(code_commune_insee as text), 5, '0') as code_commune,
        C15_MEN        as menages_c_2015,
        C16_MEN        as menages_c_2016,
        …
)                         -- 254 colonnes au total

select
    coalesce(rp_population.code_commune,
             rp_familles_menages.code_commune)    as code_commune,
    -- 1 294 colonnes listées explicitement, ex. :
    rp_population.pop_p_2015,
    rp_population.pop_p_2016,
    …
    rp_familles_menages.menages_c_2021
from rp_population
full outer join rp_familles_menages using (code_commune);
