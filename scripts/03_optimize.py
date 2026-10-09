# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# Compacta archivos Delta y recoge stats para que el SQL filtre más rápido.

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE workspace.default.bronze_ventas
# MAGIC ZORDER BY (store, date);
# MAGIC
# MAGIC ANALYZE TABLE workspace.default.bronze_ventas
# MAGIC COMPUTE STATISTICS FOR ALL COLUMNS;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workspace.default.bronze_ventas LIMIT 20

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS filas
# MAGIC FROM workspace.default.silver_ventas

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT MIN(to_date(date, 'M/d/yyyy')) AS primera,
# MAGIC        MAX(to_date(date, 'M/d/yyyy')) AS ultima,
# MAGIC        COUNT(DISTINCT to_date(date, 'M/d/yyyy')) AS dias
# MAGIC FROM workspace.default.bronze_ventas
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #PABLO 
# MAGIC #CREACION DE TABLA SILVER CON FORMATO DE HORA Y DIA CORRECTOS
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE workspace.default.silver_ventas AS
# MAGIC WITH t AS (
# MAGIC   SELECT b.* EXCEPT (date, begin_time, end_time),
# MAGIC          CAST(try_to_timestamp(b.date, 'M/d/yyyy') AS DATE) AS date,
# MAGIC          try_to_timestamp(concat(b.date, ' ', b.begin_time), 'M/d/yyyy h:mm a') AS begin_time,
# MAGIC          try_to_timestamp(concat(b.date, ' ', b.end_time), 'M/d/yyyy h:mm a') AS end_time
# MAGIC   FROM workspace.default.bronze_ventas b
# MAGIC )
# MAGIC SELECT * EXCEPT (end_time),
# MAGIC        CASE WHEN timestampdiff(MINUTE, begin_time, end_time) < -720
# MAGIC             THEN end_time + INTERVAL 1 DAY
# MAGIC             ELSE end_time END AS end_time
# MAGIC FROM t

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT pop,
# MAGIC        COUNT(*) AS filas,
# MAGIC        COUNT_IF(end_time < begin_time) AS negativos,
# MAGIC        ROUND(100 * COUNT_IF(end_time < begin_time) / COUNT(*), 1) AS pct_negativos
# MAGIC FROM workspace.default.silver_ventas
# MAGIC GROUP BY pop
# MAGIC ORDER BY negativos DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT date,
# MAGIC        COUNT(*) AS filas,
# MAGIC        COUNT(DISTINCT store) AS tiendas,
# MAGIC        COUNT(DISTINCT store, begin_time) AS ordenes_aprox,
# MAGIC        percentile_approx(timestampdiff(MINUTE, begin_time, end_time), 0.5) AS mediana_min
# MAGIC FROM workspace.default.silver_ventas
# MAGIC WHERE pop NOT IN ('Online: Zipline', 'Online: GrubHub', 'Online', 'Online: UberEats', 'Online: DoorDash')
# MAGIC   AND end_time < begin_time
# MAGIC GROUP BY date
# MAGIC ORDER BY filas DESC
# MAGIC LIMIT 500

# COMMAND ----------

'Online: Zipline', 'Online: GrubHub', 'Online', 'Online: UberEats', 'Online: DoorDash'

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #ANGELO

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workspace.default.silver_ventas LIMIT 20

# COMMAND ----------

import pyspark.pandas as pd

# COMMAND ----------

df = pd.read_table("silver_ventas")

# COMMAND ----------

print(df.dtypes)

# COMMAND ----------

df.info()

# COMMAND ----------

columns = ['tender_type', 'transaction_type', 'pop', 'store', 'menu_item', 'order_voided', 'item_voided']

# Build a dict of unique values per column (as plain Python lists)
unique_dict = {col: df[col].unique().to_list() for col in columns}

# Pad lists to the same length so they fit into a DataFrame
max_len = max(len(v) for v in unique_dict.values())
unique_dict = {col: vals + ["-"] * (max_len - len(vals)) for col, vals in unique_dict.items()}

uniques_df = pd.DataFrame(unique_dict)
uniques_df

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT tender_type,
# MAGIC        COUNT(*) AS total,
# MAGIC        ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct_uso
# MAGIC FROM workspace.default.silver_ventas
# MAGIC GROUP BY tender_type
# MAGIC ORDER BY pct_uso DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 

# COMMAND ----------

# MAGIC %md
# MAGIC Adrián - % de valores nulos en columnas
# MAGIC

# COMMAND ----------

import pandas as pd

(df[['store','pos_code','check_number','menu_item','quantity','menu_price','total','item_discount','total_discount','tax','item_voided','order_voided','transaction_type','tender_type','report_group','pop','_n_cols','_source_file','date','begin_time','end_time']].isnull().mean() * 100).round(2)