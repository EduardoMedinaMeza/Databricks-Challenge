# Databricks notebook source
# Compacta archivos Delta y recoge stats para que el SQL filtre más rápido.

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE workspace.default.bronze_ventas
# MAGIC ZORDER BY (store, date);
# MAGIC
# MAGIC ANALYZE TABLE workspace.default.bronze_ventas
# MAGIC COMPUTE STATISTICS FOR ALL COLUMNS;
