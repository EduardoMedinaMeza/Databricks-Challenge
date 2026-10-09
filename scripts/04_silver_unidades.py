# Databricks notebook source
# Parte quantity>1 EN la silver del equipo. No toca bronze, no DROPea, no pisa el esquema.
# Delta deja la versión anterior por si hay que volver atrás.

from pyspark.sql.functions import col, explode, least, lit, sequence, when

# Nombre de la silver que ya crearon tus compañeros (cámbialo si es otro):
SILVER = "workspace.default.silver_ventas"
MAX_QTY = 50

if not spark.catalog.tableExists(SILVER):
    spark.sql("SHOW TABLES IN workspace.default").show(200, truncate=False)
    raise ValueError(
        f"{SILVER} no existe. Cambia SILVER por el nombre exacto de la tabla del equipo."
    )

prev = spark.sql(f"DESCRIBE HISTORY {SILVER} LIMIT 1").collect()
prev_version = prev[0]["version"] if prev else None

src = spark.table(SILVER)
if "quantity" not in src.columns:
    raise ValueError(f"{SILVER} no tiene columna quantity; no se modificó nada")

qty_type = dict(src.dtypes)["quantity"]
qty = col("quantity").cast("double").cast("int")
qty_n = when(qty.isNull() | (qty < 1), lit(1)).otherwise(least(qty, lit(MAX_QTY)))

out = (
    src.withColumn("_qty_n", qty_n)
    .withColumn("_u", explode(sequence(lit(1), col("_qty_n"))))
    .drop("_u", "_qty_n")
    .withColumn("quantity", lit(1).cast(qty_type))
)

(
    out.coalesce(8)
    .write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "false")
    .saveAsTable(SILVER)
)

print(f"actualizada {SILVER}: mismas columnas, una fila por unidad")
if prev_version is not None:
    print(
        f"si hay que deshacer:\nRESTORE TABLE {SILVER} TO VERSION AS OF {prev_version}"
    )
