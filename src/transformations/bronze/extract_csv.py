# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# Extrae los CSV del zip solo si la carpeta aún no tiene archivos.

import zipfile
from pathlib import Path

ZIP_PATH = "/Volumes/workspace/default/raw/Taco Bueno - Pricing.zip"
CSV_DIR = "/Volumes/workspace/default/raw/csv"

destino = Path(CSV_DIR)
existentes = list(destino.rglob("*.csv")) if destino.exists() else []

if existentes:
    print(f"ya hay {len(existentes)} csv en {CSV_DIR}; se omite la extracción")
else:
    destino.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(ZIP_PATH) as z:
        miembros = [n for n in z.namelist() if n.endswith(".csv") and "__MACOSX" not in n]
        z.extractall(CSV_DIR, members=miembros)
        print(f"extraídos {len(miembros)} csv → {CSV_DIR}")