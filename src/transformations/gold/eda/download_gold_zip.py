# Baja GOLD_TABLE a ~/Downloads/<tabla>.zip
# Uso (desde eda/, venv activo):
#   python download_gold_zip.py
#   python download_gold_zip.py --force   # si pasa de 2M filas

import argparse
import csv
import os
import zipfile
from pathlib import Path

from databricks import sql
from dotenv import load_dotenv

_env = next(
    (p / ".env" for p in Path(__file__).resolve().parents if (p / ".env").exists()),
    Path(".env"),
)
load_dotenv(_env)

MAX_FILAS = 2_000_000
LOTE = 20_000


def connect():
    host = os.environ["DATABRICKS_HOST"].replace("https://", "").split("/")[0]
    path = os.environ["DATABRICKS_HTTP_PATH"]
    token = os.environ.get("DATABRICKS_TOKEN", "")
    if not token:
        raise SystemExit("DATABRICKS_TOKEN vacío en .env")
    if "sql-warehouses" in path or path.startswith("http"):
        raise SystemExit(
            "DATABRICKS_HTTP_PATH debe ser /sql/1.0/warehouses/xxxx "
            "(Connection details del warehouse, no la URL del navegador)"
        )
    return sql.connect(server_hostname=host, http_path=path, access_token=token)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()

    gold = os.environ.get("GOLD_TABLE", "workspace.default.gold_ventas")
    dest_dir = Path.home() / "Downloads"
    dest_dir.mkdir(parents=True, exist_ok=True)
    stem = gold.split(".")[-1]
    csv_path = dest_dir / f"{stem}.csv"
    zip_path = dest_dir / f"{stem}.zip"

    conn = connect()
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {gold}")
        n = int(cur.fetchone()[0])
        print(f"{gold}: {n} filas")
        if n > MAX_FILAS and not args.force:
            raise SystemExit(
                f"son {n} filas; no bajo eso a la PC. "
                "Si gold es chica y quieres forzar: python download_gold_zip.py --force"
            )

        cur.execute(f"SELECT * FROM {gold}")
        cols = [c[0] for c in cur.description]
        with csv_path.open("w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(cols)
            written = 0
            while True:
                batch = cur.fetchmany(LOTE)
                if not batch:
                    break
                w.writerows(batch)
                written += len(batch)
                print(f"  {written}/{n}", flush=True)

    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as z:
        z.write(csv_path, arcname=csv_path.name)
    csv_path.unlink()
    print(f"listo: {zip_path}")


if __name__ == "__main__":
    main()
