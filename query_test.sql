/* SUMMARY */
-- name: Query_polizas_gs
-- created_date: 2025-01-01
-- description: consulta con todas las altas generadas por Galicia +
-- references: Ticket Jira #1234


/* RELATED PROGRAMS */
-- - Query_polizas_gs.sql
-- - Query_polizas_gs.sql


/* SOURCES */
-- - prod_bronze.engage.cust_gestiones
-- - prod_bronze.mktinfo.cotizacion_vt7
-- - prod_gold_summarized.migs_upgraded.certificados
-- - prod_bronze.vt7.qv_vt7_estructuragestion
-- - prod_gold_summarized.migs_upgraded.emision
-- - hive_metastore.default.hsbc_presupuestos


/* PRODUCTS */
-- - name: hive_metastore.default.hsbc_altas
--   type: table
--   description: tabla con todas las altas de Galicia +
--   process: create or replace

-- - name: hive_metastore.default.hsbc_bajas
--   type: view
--   description: tabla con todas las altas de Galicia +
--   process: create or replace

-- - name: hive_metastore.default.hsbc_bajas
--   type: view
--   description: tabla con todas las altas de Galicia +
--   process: create or replace

-- - name: hive_metastore.default.hsbc_bajas
--   type: view
--   description: tabla con todas las altas de Galicia +
--   process: create or replace

-- - name: hive_metastore.default.hsbc_bajas
--   type: view
--   description: tabla con todas las altas de Galicia +
--   process: create or replace

-- - name: hive_metastore.default.hsbc_bajas
--   type: view
--   description: tabla con todas las altas de Galicia +
--   process: create or replace



/* HISTORICAL VERSIONS */
-- - date: 2025-01-01
--   user: Emiliano Rigueiro
--   description: created query

-- - date: 2025-02-01
--   user: Emiliano Rigueiro
--   description: se eliminó uno de los ids considerados para campañas


/* PROCESS COMMENTS */
-- STEP 1: Altas sin campañas (estructura de gestión)
-- STEP 2: Altas provenientes de campañas
-- STEP 3: Unificación de universos para tener una única tabla de altas (con y sin campañas) y flag de campaña
-- STEP 4: Enriquecimiento con certificados
-- STEP 5: Enriquecimiento con emisiones
-- STEP 6: Enriquecimiento con estructura de gestión
-- STEP 7: Corrección de punto de venta
-- STEP 8: Unión con presupuesto
-- STEP 9: Resultado final


CREATE OR REPLACE TABLE hive_metastore.default.hsbc_altas AS

WITH tmp_1 AS
(
SELECT 
    0           AS num_solicitud,
    NPOLICY     AS num_pol,
    NCERTIF     AS ncertifpol,
    NPRODUCT    AS id_producto,
    NBRANCH     AS id_branch,
    ID_CANAL    AS id_canal,
    ID_SUBCANAL AS id_subcanal,
    0           AS id_campania,
    VENDEDOR    AS vendedor,
    0           AS flg_campania

FROM prod_bronze.vt7.qv_vt7_estructuragestion

WHERE ID_ESTRUC_GESTION in (85,86)   -- LC: filtro por estructura de gestión
AND NCERTIF = 0                      -- LC: solo primera póliza (alta)
),

tmp_2 AS 
(
SELECT * 
FROM prod_bronze.engage.cust_gestiones

WHERE campana in (220,221,222,225,224)   -- LC: campañas HSBC
AND ES_VENTA = 'SI'                      -- LC: solo ventas
),

tmp_3 AS
(
SELECT 
    gest.NRO_SOLICITUD AS num_solicitud,            
    coti.NPOLICYPOL    AS num_pol,
    coti.NCERTIFPOL    AS ncertifpol,
    CAST(gest.PRODUCTO AS INT) AS id_producto,
    coti.NBRANCH       AS id_branch,
    gest.canal         AS id_canal,
    gest.subcanal      AS id_subcanal,
    gest.CAMPANA       AS id_campania,
    gest.AGENTE        AS vendedor,
    1                  AS flg_campania

FROM tmp_2 AS gest 

LEFT JOIN prod_bronze.mktinfo.cotizacion_vt7 AS coti 
    ON gest.NRO_SOLICITUD = COTI.NPOLICYSOL 
   AND gest.PRODUCTO = coti.NPRODUCT
),

tmp_3_5 AS
(
SELECT 
    tmp_3.num_solicitud,                                   
    cert.NPOLICY       AS num_pol,
    cert.NCERTIF       AS ncertifpol,
    CAST(cert.NPRODUCT AS INT) AS id_producto,
    cert.NBRANCH       AS id_branch,
    NULL               AS id_canal,
    NULL               AS id_subcanal,
    tmp_3.id_campania,
    tmp_3.vendedor,
    1                  AS flg_campania   -- LC: flag campañas

FROM tmp_3 

LEFT JOIN (
    SELECT * 
    FROM prod_gold_summarized.migs_upgraded.certificados 
    WHERE flg_traspaso IS NULL
) AS cert 

ON tmp_3.num_solicitud = cert.NPROPONUM 
AND tmp_3.id_producto = cert.NPRODUCT
),

tmp_3_6 AS
(
SELECT
    tmp_3_5.*,
    estr.id_canal,
    estr.id_subcanal   -- LC: completamos canal/subcanal

FROM tmp_3_5 

LEFT JOIN prod_bronze.vt7.qv_vt7_estructuragestion AS estr 
    ON tmp_3_5.num_pol = estr.NPOLICY 
   AND tmp_3_5.id_producto = estr.NPRODUCT
),

tmp_4 AS
(
SELECT * FROM tmp_1
UNION ALL
SELECT * FROM tmp_3_6
),

tmp_5 AS 
(
SELECT
    NPOLICY,
    NPROPONUM,
    UPDATED_AT,
    NBRANCH,
    NPRODUCT,
    NCERTIF,
    FACTURACION AS desc_facturacion,
    DISSUEDAT   AS fec_emi, 
    TIENESINIESTRO AS tiene_siniestro,
    RAMO        AS desc_ramo,
    MOTIVO_ANULACION_PRINCIPAL AS mot_anulacion,
    PUNTO_VENTA,
    ESTADO_POLIZA
  
FROM prod_gold_summarized.migs_upgraded.certificados 

WHERE NCERTIF = 0
AND flg_traspaso IS NULL   -- LC: excluye traspasos
),

tmp_6 AS
(
SELECT
    tmp_4.*,
    CASE WHEN id_producto = 5000 THEN 1 ELSE cert.NCERTIF END AS NCERTIF, -- LC: ajuste SIP
    cert.*

FROM tmp_4 

LEFT JOIN tmp_5 AS cert 
ON tmp_4.num_pol = cert.NPOLICY 
),

tmp_7 AS
(
SELECT
    *,
    ROW_NUMBER() OVER(PARTITION BY npolicy ORDER BY DISSUEDAT ASC) AS rw
FROM prod_gold_summarized.migs_upgraded.emision
),

tmp_8 AS
(
SELECT * 
FROM tmp_7
WHERE NTYPE in (1,7,9,10,33)   -- LC: tipos válidos
AND rw = 1                     -- LC: última emisión
),

tmp_9 AS
(
SELECT
    tmp_6.*,
    emision.premio,
    emision.prima,
    emision.primapura

FROM tmp_6 

LEFT JOIN tmp_8 AS emision 
ON tmp_6.num_pol = emision.NPOLICY
),

tmp_10 AS
(
SELECT
    *,
    CASE 
        WHEN punto_venta = 'CVT - Vta Tel GS' THEN 'Telemarketing'
        ELSE punto_venta 
    END AS punto_venta_fix   -- LC: normalización canal
FROM tmp_9
),

tmp_12 AS
(
SELECT * FROM tmp_10

UNION ALL

SELECT
    NULL,NULL,id_producto,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
    punto_venta,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
    cantidad_presupuesto,
    premio_promedio_presupuesto,
    premio_total_presupuesto

FROM hive_metastore.default.hsbc_presupuestos
)

-- STEP 9: Resultado final
SELECT * FROM tmp_12