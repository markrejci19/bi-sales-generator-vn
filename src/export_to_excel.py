import argparse
import math
import os
from dataclasses import dataclass
from typing import List, Optional

import pandas as pd
import psycopg2
from dotenv import load_dotenv


@dataclass
class DbConfig:
    host: str = 'localhost'
    port: int = 5432
    db: str = 'bi_courses'
    user: str = 'postgres'
    password: str = '1'


DEFAULT_TABLES = [
    'dates', 'customers', 'customer_child', 'products',
    'employees', 'stores', 'promotions', 'orders',
    'order_items', 'KPI_Target_Monthly'
]


def load_db_from_env() -> DbConfig:
    load_dotenv()
    return DbConfig(
        host=os.getenv('PG_HOST', 'localhost'),
        port=int(os.getenv('PG_PORT', 5432)),
        db=os.getenv('PG_DB', 'bi_courses'),
        user=os.getenv('PG_USER', 'postgres'),
        password=os.getenv('PG_PASSWORD', '1'),
    )


def get_conn(dbc: DbConfig):
    return psycopg2.connect(host=dbc.host, port=dbc.port, dbname=dbc.db, user=dbc.user, password=dbc.password)


def canonical_table_name(conn, table: str) -> Optional[str]:
    """Return the actual table name from catalog (case as stored) or None if not found."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT table_name FROM information_schema.tables
            WHERE table_schema='public' AND lower(table_name)=lower(%s)
            LIMIT 1
            """,
            (table,),
        )
        row = cur.fetchone()
        return row[0] if row else None


def table_exists(conn, table: str) -> bool:
    return canonical_table_name(conn, table) is not None


def get_row_count(conn, table: str) -> int:
    with conn.cursor() as cur:
        cur.execute(f'SELECT COUNT(*) FROM "{table}"')
        return int(cur.fetchone()[0])


def export_table_to_excel(conn, table: str, out_dir: str, max_rows_per_file: int = 1_000_000):
    """Export a single table to one or more Excel files, splitting to respect Excel's ~1,048,576 row limit per sheet.
    Files are named <table>.xlsx or <table>_partN.xlsx if split is needed.
    """
    canon = canonical_table_name(conn, table)
    if not canon:
        print(f"Bỏ qua bảng {table}: không tồn tại.")
        return

    total = get_row_count(conn, canon)
    if total == 0:
        # Create an empty file with header for consistency
        df_empty = pd.read_sql_query(f'SELECT * FROM "{canon}" LIMIT 0', conn)
        target = os.path.join(out_dir, f"{table}.xlsx")
        with pd.ExcelWriter(target, engine='openpyxl') as writer:
            df_empty.to_excel(writer, index=False, sheet_name=(table or canon)[:31] or 'Sheet1')
        print(f"Đã xuất {table} (trống) -> {target}")
        return

    parts = max(1, math.ceil(total / max_rows_per_file))
    for i in range(parts):
        offset = i * max_rows_per_file
        limit = min(max_rows_per_file, total - offset)
        # Always quote the canonical table name
        query = f'SELECT * FROM "{canon}" OFFSET {offset} LIMIT {limit}'
        df = pd.read_sql_query(query, conn)
        if parts == 1:
            filename = f"{table}.xlsx"
        else:
            filename = f"{table}_part{i+1}.xlsx"
        target = os.path.join(out_dir, filename)
        # Write a single sheet named after the table (trim to 31 chars)
        with pd.ExcelWriter(target, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name=table[:31] or 'Sheet1')
        print(f"Đã xuất {table} ({offset+1}-{offset+len(df)}/{total}) -> {target}")


def export_tables_to_excel(dbc: DbConfig, out_dir: str, tables: Optional[List[str]] = None, max_rows_per_file: int = 1_000_000):
    os.makedirs(out_dir, exist_ok=True)
    tables = tables or DEFAULT_TABLES
    with get_conn(dbc) as conn:
        for t in tables:
            try:
                export_table_to_excel(conn, t, out_dir, max_rows_per_file=max_rows_per_file)
            except Exception as e:
                print(f"Lỗi khi xuất {t}: {e}")


def parse_args():
    p = argparse.ArgumentParser(description='Export Postgres tables to Excel files (.xlsx) with Vietnamese support')
    p.add_argument('--out-dir', type=str, default=os.path.join('.', 'exports', 'xlsx'), help='Output directory for .xlsx files')
    p.add_argument('--tables', type=str, nargs='*', help='Specific tables to export (default: all supported)')
    p.add_argument('--max-rows-per-file', type=int, default=1_000_000, help='Max rows per Excel file before splitting')
    p.add_argument('--pg-host', type=str)
    p.add_argument('--pg-port', type=int)
    p.add_argument('--pg-db', type=str)
    p.add_argument('--pg-user', type=str)
    p.add_argument('--pg-password', type=str)
    return p.parse_args()


def main():
    dbc = load_db_from_env()
    args = parse_args()
    if args.pg_host: dbc.host = args.pg_host
    if args.pg_port: dbc.port = args.pg_port
    if args.pg_db: dbc.db = args.pg_db
    if args.pg_user: dbc.user = args.pg_user
    if args.pg_password: dbc.password = args.pg_password

    export_tables_to_excel(dbc, args.out_dir, tables=args.tables, max_rows_per_file=args.max_rows_per_file)


if __name__ == '__main__':
    main()
