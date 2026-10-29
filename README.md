# Databricks-Challenge (Sun Holdings / Taco Bueno)

Pipeline POS en Unity Catalog. Orquestación en `resources/`, lógica de tablas en `src/transformations/`.

```
.
├── resources/
│   ├── jobs/
│   │   └── daily_orchestration_job.yml
│   └── pipelines/
│       └── lakeflow_pipeline.yml
├── src/
│   └── transformations/
│       ├── bronze/
│       │   ├── extract_csv.py
│       │   └── st_ventas_bronze.py
│       ├── silver/
│       │   ├── st_ventas_unidades.py
│       │   └── st_tiempo_espera.py
│       └── gold/
│           ├── st_daily_store_kpis.py
│           └── mv_executive_kpis.sql
└── notebooks/          # exploración / EDA (no es el pipeline)
```

- `resources/jobs/` — job diario bronze → silver → gold
- `resources/pipelines/` — config Lakeflow/DLT
- `src/transformations/bronze/` — extract + tabla raw
- `src/transformations/silver/` — grano unidad + tiempo de espera
- `src/transformations/gold/` — KPIs por tienda/día y resumen ejecutivo

Tablas: `bronze_ventas`, `silver_ventas`, `gold_daily_store_kpis`, `gold_executive_kpis` en `workspace.default`.
