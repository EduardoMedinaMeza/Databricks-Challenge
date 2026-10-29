# Databricks notebook source
# MAGIC %md
# MAGIC # EDA — Taco Bueno (silver)
# MAGIC
# MAGIC Corre esto **en Databricks** (no usa el SQL Warehouse free que se saturó).
# MAGIC Lee `workspace.default.silver_ventas`. Solo agregados y `LIMIT`; no hagas `SELECT *` de 100 M filas.
# MAGIC
# MAGIC **Qué es este dataset:** líneas de ticket POS. 598 CSV, 3 encabezados. Bronze ≈ 85.2 M filas (una línea POS). Silver ≈ **100.3 M** (una fila = una unidad). Compañeros quitaron `menu_item_ext_ref`, `total_rounding`, `time_card`. Se añadió `tiempo_espera_segundos` = `end_time - begin_time`.
# MAGIC
# MAGIC **Ojo:** ~7.7 M líneas tenían `quantity = 0` (voids) y en silver cuentan como 1. Filtra `item_voided` / `order_voided` si mides venta real.

# COMMAND ----------

SILVER = "workspace.default.silver_ventas"
GOLD_CANDIDATOS = [
    "workspace.default.gold_ventas",
    "workspace.default.gold",
    "workspace.default.gold_taco_bueno",
]

spark = globals().get("spark")
if spark is None:
    from pathlib import Path
    import webbrowser

    html = Path(__file__).resolve().parent / "eda" / "eda_taco_bueno.html"
    print("Esto no corre con python3 local: necesita el cluster de Databricks.")
    print("Abriendo el EDA en HTML:", html)
    if html.exists():
        webbrowser.open(html.as_uri())
    raise SystemExit(0)

df = spark.table(SILVER)
n = df.count()
print("filas:", n)
print("columnas:", df.columns)
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Completitud
# MAGIC `report_group` y `tender_type` no venían en 168/598 CSV. `tiempo_espera_segundos` sale NULL si no parseó la hora.

# COMMAND ----------

from pyspark.sql import functions as F

nulos_row = df.agg(
    *[
        F.count(F.when(F.col(c).isNull() | (F.trim(F.col(c).cast("string")) == ""), True)).alias(c)
        for c in df.columns
    ]
).collect()[0]
filas = [(c, int(nulos_row[c]), round(100 * int(nulos_row[c]) / n, 2)) for c in df.columns]
display(spark.createDataFrame(filas, ["columna", "nulos", "pct"]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Quantity
# MAGIC Esperado: solo `1` (ya se partieron los combos).

# COMMAND ----------

display(df.groupBy("quantity").count().orderBy("quantity"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Tiempo de espera (minutos)
# MAGIC `begin_time` / `end_time` vienen como `yyyy-MM-dd HH:mm:ss`.

# COMMAND ----------

espera = df.filter(F.col("tiempo_espera_segundos").isNotNull()).withColumn(
    "espera_min", F.col("tiempo_espera_segundos") / 60.0
)
display(espera.select("espera_min").summary())
print("negativos (posible error):", espera.filter(F.col("espera_min") < 0).count())
print("más de 2 h (outliers):", espera.filter(F.col("espera_min") > 120).count())

# COMMAND ----------

display(
    espera.filter(F.col("espera_min").between(0, 60))
    .groupBy((F.floor(F.col("espera_min"))).alias("minuto"))
    .count()
    .orderBy("minuto")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Tiendas y tiempo

# COMMAND ----------

display(
    df.groupBy("store")
    .agg(F.count("*").alias("unidades"))
    .orderBy(F.desc("unidades"))
)

# COMMAND ----------

display(
    df.groupBy("date")
    .agg(F.count("*").alias("unidades"))
    .orderBy("date")
)

# COMMAND ----------

display(
    espera.filter(F.col("espera_min").between(0, 120))
    .groupBy("store")
    .agg(
        F.count("*").alias("n"),
        F.avg("espera_min").alias("espera_prom_min"),
        F.expr("percentile_approx(espera_min, 0.5)").alias("espera_p50"),
        F.expr("percentile_approx(espera_min, 0.9)").alias("espera_p90"),
    )
    .orderBy(F.desc("n"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Productos y report_group
# MAGIC `report_group` = categoría de menú (tacos, combos, bebidas…). NULL en archivos viejos.

# COMMAND ----------

display(
    df.groupBy("menu_item")
    .agg(F.count("*").alias("unidades"))
    .orderBy(F.desc("unidades"))
    .limit(30)
)

# COMMAND ----------

if "report_group" in df.columns:
    display(
        df.groupBy("report_group")
        .agg(F.count("*").alias("unidades"))
        .orderBy(F.desc("unidades"))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Pago, voids y dinero
# MAGIC `tender_type` es del ticket, no del ítem: está repetido en cada línea del mismo `check_number`. No sumes el pago por fila.

# COMMAND ----------

if "tender_type" in df.columns:
    display(
        df.groupBy("tender_type")
        .agg(F.count("*").alias("lineas"))
        .orderBy(F.desc("lineas"))
    )

# COMMAND ----------

void_cols = [c for c in ("item_voided", "order_voided") if c in df.columns]
if void_cols:
    display(df.groupBy(*void_cols).count())

# COMMAND ----------

money = [c for c in ("menu_price", "total", "item_discount", "total_discount", "tax") if c in df.columns]
display(df.select(*[F.col(c).cast("double").alias(c) for c in money]).summary())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Tickets (check)
# MAGIC Unidades por ticket; no confundir con tender.

# COMMAND ----------

tickets = df.groupBy("store", "check_number", "date").agg(
    F.count("*").alias("unidades"),
    F.sum(F.col("total").cast("double")).alias("suma_total_lineas"),
)
display(tickets.select("unidades", "suma_total_lineas").summary())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. ¿Hay gold?

# COMMAND ----------

tnames = {t.name for t in spark.catalog.listTables("workspace.default")}
print("tablas en workspace.default:", sorted(tnames))
gold = next((g for g in GOLD_CANDIDATOS if g.split(".")[-1] in tnames), None)
if gold:
    g = spark.table(gold)
    print("gold:", gold, "filas:", g.count(), "cols:", g.columns)
    g.printSchema()
    display(g.limit(20))
else:
    print("no encontré gold; el EDA de arriba es silver. Si gold tiene otro nombre, cámbialo en GOLD_CANDIDATOS.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Notas para el reporte
# MAGIC
# MAGIC 1. Unidad de análisis: **1 fila = 1 unidad vendida** (salvo voids con qty 0 que se volvieron 1).
# MAGIC 2. KPI natural: **tiempo de espera** por tienda / hora / categoría.
# MAGIC 3. Mix: `menu_item` + `report_group`; pago: `tender_type` **a nivel check**, no línea.
# MAGIC 4. No uses bronze para el EDA final: columnas crudas y qty mezclada (1 vs 2).
# MAGIC 5. Sample data de Catalog ≠ tabla completa (85 M / 100 M).
