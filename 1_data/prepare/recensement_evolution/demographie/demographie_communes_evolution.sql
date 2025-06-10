{{ config(materialized='table') }}

with rp_population as (           -- données “population”
    select
        lpad(cast(code_commune_insee as text), 5, '0') as code_commune,
        -- … 728 colonnes :  P15_POP  as pop_p_2015,  P16_POP  as pop_p_2016, …
), rp_familles_menages as (       -- données “familles & ménages”
    select
        lpad(cast(code_commune_insee as text), 5, '0') as code_commune,
        -- … 566 colonnes :  C15_MEN  as menages_c_2015,  C16_MEN  as menages_c_2016, …
), demographie_data as (
    -- fusion des deux sources sur code_commune
), denomalise_cog as (
    -- jointure avec le modèle de référence géographique
), laposte_gps as ( … ), ign_shapes as ( … )

select
    denomalise_cog.*,
    laposte_gps.commune_latitude,
    laposte_gps.commune_longitude,
    st_setsrid(st_makepoint(laposte_gps.commune_latitude,
                            laposte_gps.commune_longitude), 4326) as commune_centre_geopoint,
    ign_shapes.commune_contour
from denomalise_cog
left join laposte_gps  using (code_commune)
left join ign_shapes   using (code_commune);
