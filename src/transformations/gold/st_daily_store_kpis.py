# Databricks notebook source
# Gold: una fila por tienda y día (unidades, voids, espera).

from pyspark.sql import functions as F

SILVER = "workspace.default.silver_ventas"
GOLD = "workspace.default.gold_daily_store_kpis"

src = spark.table(SILVER)
void = F.coalesce(F.col("item_voided").cast("string"), F.lit("false"))
has_espera = "tiempo_espera_segundos" in src.columns
espera = F.col("tiempo_espera_segundos") if has_espera else F.lit(None).cast("double")
p50 = (
    F.expr("percentile_approx(tiempo_espera_segundos, 0.5)")
    if has_espera
    else F.lit(None).cast("double")
)

gold = (
    src.withColumn("_void", void.isin("true", "1", "Y", "yes"))
    .groupBy("store", "date")
    .agg(
        F.count("*").alias("unidades"),
        F.sum(F.when(F.col("_void"), 1).otherwise(0)).alias("voids"),
        F.avg(espera).alias("espera_prom_seg"),
        p50.alias("espera_p50_seg"),
        F.sum(F.col("total").cast("double")).alias("suma_total"),
    )
)

(
    gold.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(GOLD)
)
print(f"lista {GOLD}")
