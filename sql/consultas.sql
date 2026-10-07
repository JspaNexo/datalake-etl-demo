-- Ejecutar cada consulta por separado en SQLPad, conexion "Gold - Ventas".

-- 1. Resultado Gold del CSV.
SELECT * FROM gold.ventas_diarias ORDER BY fecha, ciudad, producto;

-- 2. Indicadores del CSV.
SELECT COALESCE(SUM(ventas), 0) AS ventas, COALESCE(SUM(unidades), 0) AS unidades, COALESCE(SUM(ingresos), 0) AS ingresos_bs
FROM gold.ventas_diarias;

-- 3. Ventas por ciudad.
SELECT ciudad, COALESCE(SUM(ingresos), 0) AS ingresos_bs
FROM gold.ventas_diarias GROUP BY ciudad ORDER BY ingresos_bs DESC;

-- 4. Ingresos por producto.
SELECT producto, COALESCE(SUM(unidades), 0) AS unidades, COALESCE(SUM(ingresos), 0) AS ingresos_bs
FROM gold.ventas_diarias GROUP BY producto ORDER BY ingresos_bs DESC;

-- 5. Control de calidad del CSV.
SELECT filas_bronze, filas_silver, filas_rechazadas,
       filas_bronze = filas_silver + filas_rechazadas AS filas_conciliadas,
       generado_utc
FROM gold.calidad;

-- 6. Resultados de la fuente relacional (misma conexion "Gold - Ventas").
SELECT * FROM gold.ventas_db_diarias ORDER BY fecha, ciudad, producto;

-- 7. Comparar ambas fuentes; ventas_db cuenta detalles de ventas confirmadas.
SELECT 'CSV' AS fuente, COALESCE(SUM(ventas), 0) AS registros, COALESCE(SUM(unidades), 0) AS unidades, COALESCE(SUM(ingresos), 0) AS ingresos_bs
FROM gold.ventas_diarias
UNION ALL
SELECT 'PostgreSQL', COALESCE(SUM(ventas), 0), COALESCE(SUM(unidades), 0), COALESCE(SUM(ingresos), 0)
FROM gold.ventas_db_diarias;

-- 8. Calidad del origen relacional.
SELECT * FROM gold.calidad_db;


-- 9. Version vigente por fuente y ubicacion de su manifiesto en MinIO.
SELECT dataset, execution_id, revision, manifest->>'key' AS manifiesto,
       manifest->>'sha256' AS sha256_manifiesto, published_utc
FROM etl_control.publicaciones
WHERE manifest IS NOT NULL
ORDER BY dataset;

-- 10. Historial de capturas: registrar una ejecucion no implica haberla publicado.
SELECT dataset, execution_id, revision, created_utc,
       bronze->'artifact'->>'key' AS archivo_bronze
FROM etl_control.ejecuciones
ORDER BY revision DESC;
