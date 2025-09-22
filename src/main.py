import argparse
import os
from generate_data import load_config_from_env, generate_and_load, Config, DbConfig


def parse_args():
    p = argparse.ArgumentParser(description='Generate Vietnamese Mother & Baby sales dataset and load to Postgres')
    p.add_argument('--customers', type=int, help='Exact number of customers')
    p.add_argument('--customers-min', type=int, help='Minimum customers (used when --customers not provided)')
    p.add_argument('--customers-max', type=int, help='Maximum customers (used when --customers not provided)')
    p.add_argument('--products', type=int)
    p.add_argument('--employees', type=int)
    p.add_argument('--stores', type=int)
    p.add_argument('--promotions', type=int)
    p.add_argument('--years', type=int)
    p.add_argument('--min-rows', type=int)
    p.add_argument('--max-rows', type=int)
    p.add_argument('--export-csv', type=str, help='Folder to export CSVs in addition to DB insert')

    p.add_argument('--pg-host', type=str)
    p.add_argument('--pg-port', type=int)
    p.add_argument('--pg-db', type=str)
    p.add_argument('--pg-user', type=str)
    p.add_argument('--pg-password', type=str)
    return p.parse_args()


def main():
    cfg, dbc = load_config_from_env()
    args = parse_args()

    # Override config if args provided
    if args.customers is not None:
        cfg.customers = args.customers
    else:
        if args.customers_min is not None and args.customers_max is not None and args.customers_max >= args.customers_min:
            import random
            cfg.customers = random.randint(args.customers_min, args.customers_max)
    if args.products is not None: cfg.products = args.products
    if args.employees is not None: cfg.employees = args.employees
    if args.stores is not None: cfg.stores = args.stores
    if args.promotions is not None: cfg.promotions = args.promotions
    if args.years is not None: cfg.years = args.years
    if args.min_rows is not None: cfg.min_rows = args.min_rows
    if args.max_rows is not None: cfg.max_rows = args.max_rows
    if args.export_csv is not None: cfg.export_csv_dir = args.export_csv

    if args.pg_host is not None: dbc.host = args.pg_host
    if args.pg_port is not None: dbc.port = args.pg_port
    if args.pg_db is not None: dbc.db = args.pg_db
    if args.pg_user is not None: dbc.user = args.pg_user
    if args.pg_password is not None: dbc.password = args.pg_password

    generate_and_load(cfg, dbc)


if __name__ == '__main__':
    main()
