-- SQLPad: elegir la conexion "Origen - Operacion".
SELECT v.venta_id, v.fecha, cl.nombre AS cliente, c.nombre AS ciudad, v.estado,
       p.nombre AS producto, d.cantidad, d.precio_unitario
FROM operacion.ventas v
JOIN operacion.clientes cl ON cl.cliente_id = v.cliente_id
JOIN operacion.ciudades c ON c.ciudad_id = cl.ciudad_id
JOIN operacion.detalles_venta d ON d.venta_id = v.venta_id
JOIN operacion.productos p ON p.producto_id = d.producto_id
ORDER BY v.venta_id, d.detalle_id;

-- Simular una nueva venta: ejecutar este bloque completo como una consulta.
-- El sensor DB detecta el cambio y actualiza Gold tras dos lecturas estables.
WITH nueva_venta AS (
    INSERT INTO operacion.ventas (fecha, cliente_id, estado)
    VALUES ((CURRENT_TIMESTAMP AT TIME ZONE 'America/La_Paz')::date, 1, 'CONFIRMADA')
    RETURNING venta_id
)
INSERT INTO operacion.detalles_venta (venta_id, producto_id, cantidad, precio_unitario)
SELECT venta_id, 1, 2, 25.00 FROM nueva_venta
RETURNING detalle_id, venta_id;
