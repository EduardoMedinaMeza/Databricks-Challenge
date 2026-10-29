-- Databricks notebook source
OPTIMIZE workspace.default.bronze_ventas ZORDER BY (store, date);
ANALYZE TABLE workspace.default.bronze_ventas COMPUTE STATISTICS FOR ALL COLUMNS;

OPTIMIZE workspace.default.silver_ventas ZORDER BY (store, date);
ANALYZE TABLE workspace.default.silver_ventas COMPUTE STATISTICS FOR ALL COLUMNS;
