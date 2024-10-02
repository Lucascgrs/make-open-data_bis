-- Filtres nature transaction et nature bien :
-- Il convient aussi de garder que les vente (explure les VEFA et les échanges)
-- Et que les transactions qui concernent au moins un appartement et les maisons

-- Filtres sur les surfaces et trautement des pièces :
-- Il convient de garder que les transactions qui concernent des biens de plus de 9m2 
-- Le nombre de pièces est souvent mal renseigné, il convient de le corriger en fonction de la surface

-- Filtres sur les prix :
-- Il convient de garder que les transactions dont le prix au metre carré n'est pas 50% de plus que ses 10 plus proches voisins

-- Données par mutation : 
-- Les données DVF sont initilement présentées sous forme d'une ligne par mutation (transaction)
-- Une mutation peut concerner plusieurs biens
-- Le prix est le prix total de la mutation, il apparait sur les biens concernés

{% if target.name == 'production' %}
    {% set millesimes = get_millesimes_dvf_from_sources('prod') %}
{% else %}
    {% set millesimes = get_millesimes_dvf_from_sources('dev') %}
{% endif %}

{{
    config(
        materialized='table',
        schema='intermediaires',
        post_hook=[
            "CREATE INDEX IF NOT EXISTS geopoint_index ON {{ this }} USING GIST(geopoint);",
        ]
    )
}}

{% do log(millesimes, info=True) %}

WITH source_dvf AS (
    {% for millesime in millesimes %}
        {% do log("Processing millesime: " ~ millesime, info=True) %}
        SELECT * FROM {{ source('sources', millesime) }}
        {% if not loop.last %}
            UNION ALL
        {% endif %}
    {% endfor %}
),
filtrer_dvf AS (
    {{ filtrer_dvf(source_dvf) }}
),
aggreger_dvf AS (
    {{ aggreger_dvf(filtrer_dvf) }}
),
bien_principal_dvf AS (
    {{ selectionner_bien_principal_dvf(filtrer_dvf) }}
)
SELECT
    bien_principal_dvf.id_mutation,
    bien_principal_dvf.valeur_fonciere,
    bien_principal_dvf.nature_mutation,
    bien_principal_dvf.longitude,
    bien_principal_dvf.latitude,
    aggreger_dvf.total_pieces,
    aggreger_dvf.total_surface_bati,
    aggreger_dvf.total_surface_terrain,
    aggreger_dvf.type_locaux,
    aggreger_dvf.id_parcelles,
    coalesce(bien_principal_dvf.type_local, 'Inconnu') as type_local,
    bien_principal_dvf.valeur_fonciere / aggreger_dvf.total_surface_bati as prix_m2,
    bien_principal_dvf.valeur_fonciere / bien_principal_dvf.surface_reelle_bati as prix_m2_bien_principal,
    ST_SetSRID(ST_MakePoint(bien_principal_dvf.latitude, bien_principal_dvf.longitude), 4326) as geopoint,
    bien_principal_dvf.code_postal,
    bien_principal_dvf.code_commune,
    infos_communes.nom_commune,
    infos_communes.code_arrondissement,
    infos_communes.code_departement,
    infos_communes.code_region,
    infos_communes.nom_arrondissement,
    infos_communes.nom_departement,
    infos_communes.nom_region
FROM
    bien_principal_dvf
JOIN
    aggreger_dvf ON aggreger_dvf.id_mutation = bien_principal_dvf.id_mutation
LEFT JOIN
    {{ ref('infos_communes') }} as infos_communes on infos_communes.code_commune = bien_principal_dvf.code_commune