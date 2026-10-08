# Databricks notebook source
# 3 encabezados CSV → 1 tabla Delta. Una sola acción de escritura.

import csv
import glob
import re
from io import StringIO

from pyspark.sql.functions import input_file_name, lit
from pyspark.sql.types import StringType, StructField, StructType

CSV_DIR = "/Volumes/workspace/default/raw/csv"
TABLE = "workspace.default.bronze_ventas"

CANON = [
    "date",
    "begin_time",
    "end_time",
    "store",
    "pos_code",
    "check_number",
    "menu_item",
    "menu_item_ext_ref",
    "quantity",
    "menu_price",
    "total",
    "item_discount",
    "total_discount",
    "tax",
    "total_rounding",
    "item_voided",
    "order_voided",
    "transaction_type",
    "tender_type",
    "report_group",
    "pop",
    "time_card",
    "_n_cols",
    "_source_file",
]


def sniff_sep(header: str) -> str:
    counts = {sep: header.count(sep) for sep in [",", ";", "\t", "|"]}
    return max(counts, key=counts.get)


def norm_col(name: str) -> str:
    return re.sub(r"[^0-9a-zA-Z]+", "_", name).strip("_").lower() or "col"


def unique_names(cols):
    seen, out = {}, []
    for c in cols:
        n = seen.get(c, 0)
        out.append(c if n == 0 else f"{c}_{n}")
        seen[c] = n + 1
    return out


def parse_header(header: str):
    sep = sniff_sep(header)
    raw = next(csv.reader(StringIO(header), delimiter=sep))
    names = tuple(unique_names([norm_col(c.strip()) for c in raw]))
    return sep, names


spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

familias = {}
for path in glob.glob(f"{CSV_DIR}/**/*.csv", recursive=True):
    with open(path, encoding="utf-8", errors="replace") as f:
        header = f.readline().rstrip("\r\n")
    familias.setdefault(parse_header(header), []).append(path)

if not familias:
    raise FileNotFoundError(f"no hay CSV en {CSV_DIR}; corre primero scripts/01_extract_csv")

dfs = []
for (sep, names), paths in familias.items():
    schema = StructType([StructField(n, StringType(), True) for n in names])
    df_g = (
        spark.read.format("csv")
        .schema(schema)
        .option("header", "true")
        .option("sep", sep)
        .option("mode", "PERMISSIVE")
        .option("encoding", "UTF-8")
        .load(paths)
        .withColumn("_n_cols", lit(len(names)))
        .withColumn("_source_file", input_file_name())
    )
    dfs.append(df_g)
    print(f"familia {len(names)} cols × {len(paths)} archivos")

bronze = dfs[0]
for other in dfs[1:]:
    bronze = bronze.unionByName(other, allowMissingColumns=True)

ordered = [c for c in CANON if c in bronze.columns]
extras = [c for c in bronze.columns if c not in CANON]
bronze = bronze.select(*ordered, *extras)

(
    bronze.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TABLE)
)

print(f"lista {TABLE}")
spark.table(TABLE).printSchema()
