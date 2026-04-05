"""
GSC Sync — Puxa dados do Google Search Console e salva no PostgreSQL analytics.

Uso:
  1. Crie um Service Account no Google Cloud Console
  2. Ative a API do Search Console
  3. Adicione o email do SA como usuário no Search Console (permissão leitura)
  4. Baixe o JSON de credenciais e defina GSC_CREDENTIALS_JSON ou GSC_CREDENTIALS_FILE
  5. Execute: python gsc_sync.py [--days 30] [--site https://www.papelariaunicornio.com.br]

Variáveis de ambiente:
  GSC_CREDENTIALS_FILE  — caminho para o JSON do service account
  GSC_CREDENTIALS_JSON  — JSON inline (alternativa ao file, útil em containers)
  GSC_SITE_URL          — URL da propriedade no GSC (default: https://www.papelariaunicornio.com.br)
  PG_HOST, PG_PORT, PG_USER, PG_PASS, PG_DB — conexão PostgreSQL
"""

import os
import sys
import json
import argparse
from datetime import date, timedelta
from typing import Optional

import psycopg2
from psycopg2.extras import execute_values
from google.oauth2 import service_account
from googleapiclient.discovery import build

# ─── Config ───
PG = dict(
    host=os.getenv("PG_HOST", "postgres-analytics-kk4wgo8s8gk0wckswok4oc4o"),
    port=int(os.getenv("PG_PORT", 5432)),
    user=os.getenv("PG_USER", "analytics"),
    password=os.getenv("PG_PASS", "analytics_pass_pu2024"),
    dbname=os.getenv("PG_DB", "analytics"),
)

SITE_URL = os.getenv("GSC_SITE_URL", "https://www.papelariaunicornio.com.br")
SCOPES = ["https://www.googleapis.com/auth/webmasters.readonly"]


def get_gsc_service():
    """Authenticate and return GSC service."""
    creds_file = os.getenv("GSC_CREDENTIALS_FILE")
    creds_json = os.getenv("GSC_CREDENTIALS_JSON")

    if creds_json:
        info = json.loads(creds_json)
        credentials = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
    elif creds_file:
        credentials = service_account.Credentials.from_service_account_file(creds_file, scopes=SCOPES)
    else:
        print("ERRO: Defina GSC_CREDENTIALS_FILE ou GSC_CREDENTIALS_JSON")
        sys.exit(1)

    return build("searchconsole", "v1", credentials=credentials)


def fetch_gsc_data(service, start_date: str, end_date: str, dimensions: list, row_limit: int = 25000):
    """Fetch data from GSC API with pagination."""
    all_rows = []
    start_row = 0

    while True:
        request = {
            "startDate": start_date,
            "endDate": end_date,
            "dimensions": dimensions,
            "rowLimit": row_limit,
            "startRow": start_row,
        }
        response = service.searchanalytics().query(siteUrl=SITE_URL, body=request).execute()
        rows = response.get("rows", [])
        if not rows:
            break
        all_rows.extend(rows)
        start_row += len(rows)
        if len(rows) < row_limit:
            break
        print(f"  ... {len(all_rows)} rows fetched so far")

    return all_rows


def sync_queries(conn, service, start_date: str, end_date: str):
    """Sync query-level data."""
    print(f"Fetching queries: {start_date} → {end_date}")
    rows = fetch_gsc_data(service, start_date, end_date, ["date", "query", "page", "country", "device"])
    print(f"  {len(rows)} rows from API")

    if not rows:
        return 0

    values = []
    for r in rows:
        keys = r["keys"]
        values.append((
            keys[0],          # date
            keys[1],          # query
            keys[2],          # page
            keys[3],          # country
            keys[4],          # device
            r["clicks"],
            r["impressions"],
            round(r["ctr"], 4),
            round(r["position"], 2),
        ))

    with conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO gsc_queries (data, query, page, country, device, clicks, impressions, ctr, position, synced_at)
            VALUES %s
            ON CONFLICT (data, query, page, country, device)
            DO UPDATE SET
                clicks = EXCLUDED.clicks,
                impressions = EXCLUDED.impressions,
                ctr = EXCLUDED.ctr,
                position = EXCLUDED.position,
                synced_at = NOW()
        """, values, page_size=500)
    conn.commit()
    print(f"  {len(values)} rows upserted into gsc_queries")
    return len(values)


def sync_pages(conn, service, start_date: str, end_date: str):
    """Sync page-level aggregated data."""
    print(f"Fetching pages: {start_date} → {end_date}")
    rows = fetch_gsc_data(service, start_date, end_date, ["date", "page"])
    print(f"  {len(rows)} rows from API")

    if not rows:
        return 0

    values = []
    for r in rows:
        keys = r["keys"]
        values.append((
            keys[0],          # date
            keys[1],          # page
            r["clicks"],
            r["impressions"],
            round(r["ctr"], 4),
            round(r["position"], 2),
        ))

    with conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO gsc_pages (data, page, clicks, impressions, ctr, position, synced_at)
            VALUES %s
            ON CONFLICT (data, page)
            DO UPDATE SET
                clicks = EXCLUDED.clicks,
                impressions = EXCLUDED.impressions,
                ctr = EXCLUDED.ctr,
                position = EXCLUDED.position,
                synced_at = NOW()
        """, values, page_size=500)
    conn.commit()
    print(f"  {len(values)} rows upserted into gsc_pages")
    return len(values)


def main():
    parser = argparse.ArgumentParser(description="Sync Google Search Console → PostgreSQL")
    parser.add_argument("--days", type=int, default=7, help="Dias para trás (default: 7)")
    parser.add_argument("--site", type=str, default=SITE_URL, help="URL da propriedade no GSC")
    parser.add_argument("--start", type=str, help="Data início (YYYY-MM-DD), sobrescreve --days")
    parser.add_argument("--end", type=str, help="Data fim (YYYY-MM-DD), default: ontem")
    args = parser.parse_args()

    global SITE_URL
    SITE_URL = args.site

    end_date = args.end or (date.today() - timedelta(days=1)).isoformat()
    if args.start:
        start_date = args.start
    else:
        start_date = (date.fromisoformat(end_date) - timedelta(days=args.days - 1)).isoformat()

    print(f"=== GSC Sync: {SITE_URL} ===")
    print(f"Período: {start_date} → {end_date}")

    service = get_gsc_service()
    conn = psycopg2.connect(**PG)

    try:
        total_q = sync_queries(conn, service, start_date, end_date)
        total_p = sync_pages(conn, service, start_date, end_date)
        print(f"\nDone! {total_q} queries + {total_p} pages synced.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
