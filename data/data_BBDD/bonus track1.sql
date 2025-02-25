-- Bonus track 1
 
--1. Cuantos hoteles tiene la base de datos
SELECT COUNT(*) FROM "hoteles";
 
--1.2. Cuantos hoteles únicos hay en la base de datos
SELECT COUNT(DISTINCT "nombre_hotel") FROM "hoteles";

--2. Cuantas reservas se han hecho
SELECT COUNT(*) FROM "reservas";

--3. Identifica los 10 clientes que más se han gastado
SELECT c."id_cliente", CONCAT(c."nombre", ' ' ,c."apellido") as "nombre_cliente", SUM(r.precio_noche) AS "total_gastado"
FROM "reservas" AS "r"
JOIN "clientes" AS "c" ON r."id_cliente" = c."id_cliente"
GROUP BY c."id_cliente", c."nombre", c."apellido"
ORDER BY "total_gastado" DESC
LIMIT 10;
 
--4. Identifica el hotel de la competencia y el hotel de nuestra marca que más han recaudado para esas fechas
WITH "ingresos_por_hotel" AS (
    SELECT h."id_hotel", h."nombre_hotel", h."competencia", SUM(r."precio_noche") AS "total_recaudado"
    FROM "reservas" r
    JOIN "hoteles" h ON r."id_hotel" = h."id_hotel"
    WHERE r."fecha_reserva" BETWEEN '2025-02-01' AND '2025-02-22' 
    GROUP BY h."id_hotel", h."nombre_hotel", h."competencia"
)
SELECT * FROM (
    (SELECT * FROM ingresos_por_hotel WHERE competencia = FALSE ORDER BY total_recaudado DESC LIMIT 1)
    UNION ALL
    (SELECT * FROM ingresos_por_hotel WHERE competencia = TRUE ORDER BY total_recaudado DESC LIMIT 1)
) AS hoteles_top;
 

--5. Identifica cuantos eventos hay.
SELECT COUNT(*) FROM "eventos";
 
--6. Identifica el día que más reservas se han hecho para nuestro hoteles
SELECT r."fecha_reserva", COUNT(*) AS "total_reservas"
FROM "reservas" r
JOIN "hoteles" h ON r."id_hotel" = h."id_hotel"
WHERE h."competencia" = FALSE  -- Solo hoteles propios
GROUP BY r."fecha_reserva"
ORDER BY "total_reservas" DESC
LIMIT 1;








