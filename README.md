# Databricks-Challenge (Sun Holdings / Taco Bueno)

Pipeline POS en Unity Catalog. Orquestación en `resources/`; tablas y EDA en `src/transformations/`.

```
.
├── resources/
│   ├── jobs/
│   │   └── daily_orchestration_job.yml
│   └── pipelines/
│       └── lakeflow_pipeline.yml
└── src/
    └── transformations/
        ├── bronze/
        │   ├── extract_csv.py
        │   ├── st_ventas_bronze.py
        │   └── ingest_orchestration.ipynb
        ├── silver/
        │   ├── st_ventas_unidades.py
        │   ├── st_tiempo_espera.py
        │   ├── optimize_ventas.sql
        │   ├── Adrian_silver_ventas.ipynb
        │   └── exploratory_optimize_silver.ipynb
        └── gold/
            ├── st_daily_store_kpis.py
            ├── mv_executive_kpis.sql
            ├── EDA.ipynb
            ├── eda_silver.py
            └── eda/                    # Quarto (reporte local)
                ├── eda_taco_bueno.qmd
                ├── eda_gold.qmd
                └── download_gold_zip.py
```

| Carpeta | Qué guarda |
|---|---|
| `resources/jobs/` | Job Databricks: extract → bronze → silver → gold |
| `resources/pipelines/` | Config Lakeflow / DLT |
| `src/transformations/bronze/` | Extract del zip + `bronze_ventas` |
| `src/transformations/silver/` | Grano 1 fila = 1 unidad, espera, notebooks del equipo |
| `src/transformations/gold/` | KPIs diarios/tienda, KPIs ejecutivos, EDA |

Tablas en `workspace.default`: `bronze_ventas`, `silver_ventas`, `gold_daily_store_kpis`, `gold_executive_kpis`.
