-- Databricks notebook source
CREATE OR REPLACE TABLE workspace.default.gold_executive_kpis AS
SELECT
  COUNT(*) AS unidades,
  COUNT(DISTINCT store) AS tiendas,
  COUNT(DISTINCT date) AS dias,
  ROUND(AVG(TRY_CAST(tiempo_espera_segundos AS DOUBLE)) / 60.0, 2) AS espera_prom_min,
  ROUND(SUM(TRY_CAST(total AS DOUBLE)), 2) AS venta_lineas
FROM workspace.default.silver_ventas;
