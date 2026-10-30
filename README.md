# Databricks-Challenge (Sun Holdings / Taco Bueno)

Pipeline POS en Unity Catalog. Orquestación en `resources/`; tablas en `src/transformations/` y validación en `src/validations/`.

## Pipeline `taco_bueno_daily`

Job de Databricks que corre todo el flujo medallion: CSV → bronze → silver → gold, con materialized view y validación de calidad al final. La definición vive en `resources/jobs/daily_orchestration_job.yml` y está desplegada en el workspace como el job **`taco_bueno_daily`**.

```
extract_csv ──► bronze_ventas ──► silver_ventas ──► silver_unidades ──► silver_limpieza
                                                                          │
                                                            silver_optimize ◄┘
                                                                          │
                                                               gold_ventas ◄┘
                                                                │       │
                    ┌───────────────────────────────────────────┘       └──────────┐
                    ▼                                                               ▼
        gold_mv_dia_tienda                                                validacion
        (MV ventas/tienda/día)                                     (chequeos de calidad, sin reintentos)
```

| Tarea | Notebook | Qué hace |
|---|---|---|
| `extract_csv` | `src/transformations/01_bronze/01_01_extract_csv.py` | Extrae los CSV del zip a la zona de trabajo |
| `bronze_ventas` | `src/transformations/01_bronze/01_02_st_ventas_bronze.py` | Ingiere a `bronze_ventas` |
| `silver_ventas` | `src/transformations/02_silver/02_01_silver_ventas.ipynb` | Limpieza principal a `silver_ventas` |
| `silver_unidades` | `src/transformations/02_silver/02_02_ventas_unidades.py` | Grano 1 fila = 1 unidad |
| `silver_limpieza` | `src/transformations/02_silver/02_03_silver_limpieza.ipynb` | Limpieza fina de silver |
| `silver_optimize` | `src/transformations/02_silver/02_04_optimize.ipynb` | `OPTIMIZE` + stats de Delta |
| `gold_ventas` | `src/transformations/03_gold/03_01_gold_ventas.ipynb` | Construye `gold_ventas` |
| `gold_mv_dia_tienda` | `src/transformations/03_gold/03_02_MV_Dia_Tienda.ipynb` | `CREATE OR REPLACE MATERIALIZED VIEW mv_ventas_tienda_dia` |
| `validacion` | `src/validations/01_01_validation.ipynb` | Validaciones de calidad (`validacion_resultados`) |

**Detalles del job**

* Horario: diario 6:00 AM (`America/Mexico_City`), **en pausa** — el extracto es histórico, así que se corre manualmente con **Run now** desde la UI de Jobs.
* Reintentos: 1 (cada 60 s) en todas las tareas, excepto `validacion` (0: si los datos no cuadran, reintentar no arregla nada).
* Correos de fallo al equipo (`@tec.mx`).

**Cómo correrla:** Jobs UI → job `taco_bueno_daily` → *Run now* (en el workspace: <https://dbc-b9b79dc2-7f0a.cloud.databricks.com/?o=7474660220074313#job/728417848934577>).

**Tablas en `workspace.default`**: `bronze_ventas`, `silver_ventas`, `gold_ventas`, `mv_ventas_tienda_dia` (materialized view), `exports` (respaldo Parquet) y `validacion_resultados`.

## Estructura del repo

```
.
├── resources/
│   ├── jobs/
│   │   └── daily_orchestration_job.yml   # definición del job taco_bueno_daily
│   └── pipelines/
│       └── NOTE.md
├── scripts/
│   └── 06 - Limpieza Final (Gold).ipynb  # limpieza puntual de gold (manual)
└── src/
    ├── transformations/
    │   ├── 01_bronze/   # extract + ingesta a bronze
    │   ├── 02_silver/   # limpieza, grano por unidad y optimize
    │   └── 03_gold/     # gold, MV y export
    └── validations/
        └── 01_01_validation.ipynb
```

| Carpeta | Qué guarda |
|---|---|
| `resources/jobs/` | Definición YAML del job `taco_bueno_daily` |
| `src/transformations/01_bronze/` | Extract del zip + `bronze_ventas` |
| `src/transformations/02_silver/` | Grano 1 fila = 1 unidad, limpieza y `OPTIMIZE` |
| `src/transformations/03_gold/` | `gold_ventas`, materialized view y export Parquet |
| `src/validations/` | Validaciones de calidad al final del pipeline |
