-- Ejecutar cada consulta por separado en SQLPad, conexion "Gold - Ventas".

-- 1. Resultado gold: 7 grupos.
SELECT * FROM gold.ventas_diarias ORDER BY fecha, ciudad, producto;

-- 2. Indicadores: 8 ventas, 22 unidades, Bs 356.00.
SELECT SUM(ventas) AS ventas, SUM(unidades) AS unidades, SUM(ingresos) AS ingresos_bs
FROM gold.ventas_diarias;

-- 3. Ventas por ciudad: La Paz 111.00, Cochabamba 115.00, Santa Cruz 130.00.
SELECT ciudad, SUM(ingresos) AS ingresos_bs
FROM gold.ventas_diarias GROUP BY ciudad ORDER BY ingresos_bs DESC;

-- 4. Producto con mayores ingresos: cafe, Bs 250.00.
SELECT producto, SUM(unidades) AS unidades, SUM(ingresos) AS ingresos_bs
FROM gold.ventas_diarias GROUP BY producto ORDER BY ingresos_bs DESC;

-- 5. Control de calidad: 14 entradas = 8 validas + 6 rechazadas.
SELECT filas_bronze, filas_silver, filas_rechazadas,
       filas_bronze = filas_silver + filas_rechazadas AS filas_conciliadas,
       generado_utc
FROM gold.calidad;
