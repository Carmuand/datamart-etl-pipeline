-- ============================================================================
-- PRUEBA DE DESEMPEÑO - DATAMART S.A.S.
-- ENTREGABLE: CONSULTAS SQL DE VALIDACIÓN (SECCIÓN 7) - OPTIMIZADO
-- ============================================================================

-- 1. Evolución mensual de las ventas netas (Revenue Ventas - Revenue Devoluciones)
-- Corregido mediante CTEs independientes para evitar duplicación por producto cartesiano en JOINs múltiples
WITH monthly_sales AS (
    SELECT date_id, SUM(revenue_bruto) AS rev_ventas
    FROM fact_sales
    GROUP BY date_id
),
monthly_returns AS (
    SELECT date_id, SUM(revenue_bruto) AS rev_devoluciones
    FROM fact_returns
    GROUP BY date_id
)
SELECT 
    d.year,
    d.month,
    d.month_name,
    COALESCE(SUM(s.rev_ventas), 0) AS revenue_ventas_global,
    COALESCE(SUM(r.rev_devoluciones), 0) AS revenue_devoluciones_global,
    (COALESCE(SUM(s.rev_ventas), 0) - COALESCE(SUM(r.rev_devoluciones), 0)) AS ventas_netas_consolidadas
FROM dim_date d
LEFT JOIN monthly_sales s ON d.date_id = s.date_id
LEFT JOIN monthly_returns r ON d.date_id = r.date_id
GROUP BY d.year, d.month, d.month_name
ORDER BY d.year ASC, d.month ASC;


-- 2. Categorías de producto con mayor revenue bruto (Corregida con limpieza de llaves)
SELECT 
    TRIM(p.category) AS category,
    SUM(s.revenue_bruto) AS revenue_bruto_ventas,
    ROUND(
        (COALESCE(SUM(r.revenue_bruto), 0) / NULLIF(SUM(s.revenue_bruto), 0)) * 100, 2
    ) AS porcentaje_devolucion_sobre_revenue
FROM dim_product p
JOIN fact_sales s ON TRIM(p.product_code) = TRIM(s.product_code)
LEFT JOIN fact_returns r ON TRIM(p.product_code) = TRIM(r.product_code)
GROUP BY TRIM(p.category)
ORDER BY revenue_bruto_ventas DESC;


-- 3. Top 10 productos con mayor revenue neto y Top 10 con mayor tasa de devolución
-- Subconsulta A: Top 10 Revenue Neto General
SELECT 
    p.product_code,
    p.description_canonical,
    (COALESCE(SUM(s.revenue_bruto), 0) - COALESCE(SUM(r.revenue_bruto), 0)) AS revenue_neto_total
FROM dim_product p
LEFT JOIN fact_sales s ON p.product_code = s.product_code
LEFT JOIN fact_returns r ON p.product_code = r.product_code
GROUP BY p.product_code, p.description_canonical
ORDER BY revenue_neto_total DESC
LIMIT 10;

-- Subconsulta B: Top 10 Mayor tasa de devolución (Basado en cantidades físicas de ítems)
SELECT 
    p.product_code,
    p.description_canonical,
    ROUND(
        (COALESCE(SUM(r.quantity), 0) * 1.0 / NULLIF(COALESCE(SUM(s.quantity), 0), 0)) * 100, 2
    ) AS tasa_devolucion_por_unidades
FROM dim_product p
JOIN fact_sales s ON p.product_code = s.product_code
LEFT JOIN fact_returns r ON p.product_code = r.product_code
GROUP BY p.product_code, p.description_canonical
HAVING SUM(s.quantity) > 10 -- Filtro de control para evitar sesgos por compras atípicas bajas
ORDER BY tasa_devolucion_por_unidades DESC
LIMIT 10;


-- 4. Concentración de transacciones por país y variación del ticket promedio de venta
-- Corregido para calcular el ticket promedio real por factura (Suma total / Órdenes distintas)
SELECT 
    c.country_name,
    COUNT(DISTINCT s.invoice_no) AS total_ordenes_de_venta,
    ROUND(SUM(s.revenue_bruto) / NULLIF(COUNT(DISTINCT s.invoice_no), 0), 2) AS ticket_promedio_venta
FROM fact_sales s
JOIN dim_country c ON s.country_id = c.country_id
GROUP BY c.country_name
ORDER BY total_ordenes_de_venta DESC;


-- 5. Diferencia en el comportamiento de compra entre clientes identificados vs transacciones sin Customer ID (Anónimos)
SELECT 
    c.customer_type,
    COUNT(DISTINCT s.invoice_no) AS numero_total_de_facturas,
    ROUND(AVG(s.quantity), 2) AS promedio_unidades_por_item,
    ROUND(SUM(s.revenue_bruto) / NULLIF(COUNT(DISTINCT s.invoice_no), 0), 2) AS gasto_promedio_por_factura
FROM fact_sales s
JOIN dim_customer c ON s.customer_id = c.customer_id
GROUP BY c.customer_type;


-- 6. Calidad de Catálogo: Productos sin descripción consistente y conteo total de códigos únicos
-- Parte A: Códigos que registran múltiples textos descriptivos distintos en la fuente de datos
SELECT 
    product_code,
    COUNT(DISTINCT description_canonical) AS variaciones_de_descripcion
FROM dim_product
GROUP BY product_code
HAVING COUNT(DISTINCT description_canonical) > 1;

-- Parte B: Métrica de control del volumen total del inventario operacional de DataMart
SELECT COUNT(DISTINCT product_code) AS total_codigos_unicos_sistema_maestro FROM dim_product;