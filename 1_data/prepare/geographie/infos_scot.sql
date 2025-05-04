-- Table: infos_scot
-- Reports basic infos on SCOT (Schéma de Cohérence Territoriale)
-- scot_code, siren, scot_name, population, contour, number_of_communes

{{ config(materialized='table') }}

WITH scot_data as (
    select
        commune_scot."Id SCoT" as scot_code, -- identifiant unique du SCOT
        commune_scot."SCoT" as scot_name,    -- nom du SCOT
        commune_scot."SIREN EPCI" as siren,
        LPAD(CAST(commune_scot."INSEE commune" AS TEXT), 5, '0') as code_commune
    from {{ source('sources', 'communes_to_scot') }} as commune_scot
),

scot_communes as (
    select 
        sd.scot_code,
        sd.siren,
        sd.scot_name,
        sd.code_commune,
        ic.population,
        ic.commune_contour,
        ic.code_departement,
        ic.nom_departement,
        ic.code_region,
        ic.nom_region
    from scot_data sd
    left join {{ ref('infos_communes') }} ic
        on sd.code_commune = ic.code_commune
)

select
    scot_code,
    siren,
    scot_name,
    -- Région (on suppose qu'un SCOT n'est que sur une seule région)
    MIN(code_region) as code_region,
    MIN(nom_region) as nom_region,
    -- Département principal :
    -- Si un seul département, on prend ce département, sinon celui avec le plus d'habitants
    (
        CASE 
            WHEN COUNT(DISTINCT code_departement) = 1 THEN MIN(code_departement)
            ELSE (
                SELECT code_departement FROM (
                    SELECT code_departement, SUM(CAST(population AS NUMERIC)) as pop
                    FROM scot_communes sc2
                    WHERE sc2.scot_code = scot_communes.scot_code
                    GROUP BY code_departement
                    ORDER BY pop DESC NULLS LAST
                    LIMIT 1
                ) t
            )
        END
    ) as code_departement,
    (
        CASE 
            WHEN COUNT(DISTINCT nom_departement) = 1 THEN MIN(nom_departement)
            ELSE (
                SELECT nom_departement FROM (
                    SELECT nom_departement, SUM(CAST(population AS NUMERIC)) as pop
                    FROM scot_communes sc2
                    WHERE sc2.scot_code = scot_communes.scot_code
                    GROUP BY nom_departement
                    ORDER BY pop DESC NULLS LAST
                    LIMIT 1
                ) t
            )
        END
    ) as main_departement,
    -- Booléen multi_dpt
    (COUNT(DISTINCT code_departement) > 1) as multi_dpt,
    SUM(CAST(population AS NUMERIC)) as population,
    ST_Union(commune_contour) as contour,
    COUNT(code_commune) as number_of_communes
from scot_communes
where code_commune is not null
GROUP BY scot_code, siren, scot_name