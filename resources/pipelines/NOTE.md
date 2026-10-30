# Lakeflow pipeline: evaluated, not used

This folder holds pipeline configuration. We evaluated moving the bronze → silver → gold transformations into a Lakeflow Spark Declarative Pipeline, and decided to orchestrate them as notebook tasks in a Lakeflow Job instead (`resources/jobs/daily_orchestration_job.yml`).

## Why not a declarative pipeline

- **Reproducibility first.** The goal of this phase was to reproduce, exactly, the tables the analysis was built on, which were cleaned and validated on 9 October 2026. Those steps already existed as notebooks, and we checked them version by version against the Delta history of each table. Rewriting them as streaming tables and materialized views days before the deadline added risk without changing the data.
- **Not every step is declarative.** Part of the cleaning is imperative: the Python split of multi-unit rows into one row per unit, CHECK constraints added with `ALTER TABLE`, and table maintenance with `OPTIMIZE`. A declarative pipeline only accepts dataset definitions.
- **The data is a fixed historical extract.** It covers January 2025 to September 2026, and no new files arrive. Streaming ingestion and continuous mode add nothing here, and continuous mode keeps compute running.
- **Free Edition limits.** One active pipeline per type, and a daily compute quota shared by the whole workspace.

## Where Lakeflow pipelines are still used

The gold materialized view `mv_ventas_tienda_dia` (sales by store and day, with expectations) is created from a SQL warehouse. Its create and refresh operations run on a serverless Lakeflow pipeline behind the scenes, and that backing pipeline can be inspected in Catalog Explorer.

## Next steps

- Bronze as a streaming table with Auto Loader, so each new extract is ingested exactly once without reprocessing the history.
- Silver and gold as materialized views with expectations inside a single pipeline, triggered by the job.