# Databricks notebook source
# Agrega tiempo_espera_segundos = end_time - begin_time en la silver del equipo.
# No DROPea columnas; Delta deja la versión anterior.

from pyspark.sql.functions import col, expr, unix_timestamp, when

SILVER = "workspace.default.silver_ventas"
BEGIN = "begin_time"
END = "end_time"
NUEVA = "tiempo_espera_segundos"


def parse_hora(colname: str):
    # try_to_timestamp: Photon no admite to_timestamp en coalesce (lanza en el 1er formato).
    return expr(
        f"""
        coalesce(
          try_to_timestamp(trim(cast({colname} AS string)), 'yyyy-MM-dd HH:mm:ss'),
          try_to_timestamp(trim(cast({colname} AS string)), "yyyy-MM-dd'T'HH:mm:ss"),
          try_to_timestamp(trim(cast({colname} AS string)), 'yyyy-MM-dd HH:mm:ss.SSS'),
          try_to_timestamp(trim(cast({colname} AS string)), 'M/d/yyyy H:mm:ss'),
          try_to_timestamp(trim(cast({colname} AS string)), 'MM/dd/yyyy HH:mm:ss'),
          try_to_timestamp(trim(cast({colname} AS string)), 'HH:mm:ss'),
          try_to_timestamp(trim(cast({colname} AS string)), 'H:mm:ss'),
          try_to_timestamp(trim(cast({colname} AS string)), 'HH:mm'),
          try_to_timestamp(trim(cast({colname} AS string)))
        )
        """
    )


if not spark.catalog.tableExists(SILVER):
    spark.sql("SHOW TABLES IN workspace.default").show(200, truncate=False)
    raise ValueError(f"{SILVER} no existe. Cambia SILVER por el nombre real.")

src = spark.table(SILVER)
faltan = [c for c in (BEGIN, END) if c not in src.columns]
if faltan:
    raise ValueError(f"{SILVER} no tiene {faltan}. Columnas: {src.columns}")

prev = spark.sql(f"DESCRIBE HISTORY {SILVER} LIMIT 1").collect()
prev_version = prev[0]["version"] if prev else None

inicio = parse_hora(BEGIN)
fin = parse_hora(END)
seg = unix_timestamp(fin) - unix_timestamp(inicio)
solo_hora = ~col(BEGIN).cast("string").rlike(r"\d{4}-\d{2}-\d{2}")
seg = when(solo_hora & (seg < 0), seg + 24 * 3600).otherwise(seg)

out = src.drop(NUEVA) if NUEVA in src.columns else src
out = out.withColumn(NUEVA, seg.cast("int"))

(
    out.coalesce(8)
    .write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER)
)

print(f"columna {NUEVA} en {SILVER}")
spark.sql(
    f"""
    SELECT {BEGIN}, {END}, {NUEVA}
    FROM {SILVER}
    WHERE {NUEVA} IS NOT NULL
    LIMIT 10
    """
).show(truncate=False)
if prev_version is not None:
    print(f"para revertir:\nRESTORE TABLE {SILVER} TO VERSION AS OF {prev_version}")
