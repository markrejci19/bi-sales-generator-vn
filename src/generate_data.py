import os
import random
import math
from datetime import date, datetime, timedelta
from dataclasses import dataclass
from typing import List, Optional, Tuple, Any

import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta
from faker import Faker
from faker.providers import person, phone_number, address, internet
import psycopg2
from psycopg2.extras import execute_values
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from dotenv import load_dotenv

fake = Faker('vi_VN')
fake.add_provider(person)
fake.add_provider(phone_number)
fake.add_provider(address)
fake.add_provider(internet)

VN_CITIES = [
    ("Hà Nội", "Hà Nội"), ("Hồ Chí Minh", "Hồ Chí Minh"), ("Đà Nẵng", "Đà Nẵng"),
    ("Hải Phòng", "Hải Phòng"), ("Cần Thơ", "Cần Thơ"), ("Biên Hòa", "Đồng Nai"),
    ("Nha Trang", "Khánh Hòa"), ("Huế", "Thừa Thiên Huế"), ("Vinh", "Nghệ An"),
    ("Buôn Ma Thuột", "Đắk Lắk"), ("Thủ Dầu Một", "Bình Dương"), ("Vũng Tàu", "Bà Rịa - Vũng Tàu"),
]

PRODUCT_CATEGORIES = [
    ("Sữa bột", ["Dielac", "Friso", "Aptamil", "Nan", "Glico"]),
    ("Tã/bỉm", ["Pampers", "Moony", "Merries", "Huggies", "Bobby"]),
    ("Đồ ăn dặm", ["Heinz", "Gerber", "Nestlé", "Ella's"]),
    ("Bình sữa & núm ti", ["Pigeon", "Chicco", "Philips Avent", "Comotomo"]),
    ("Xe đẩy & ghế ngồi", ["Aprica", "Joie", "Graco", "Combi"]),
    ("Đồ vệ sinh", ["Kodomo", "Dnee", "Johnson's", "Lactacyd"]),
    ("Quần áo sơ sinh", ["Carters", "Gap", "H&M", "Ninomaxx"]),
]

UNITS = ["hộp", "bịch", "gói", "chai", "cái", "bộ"]

EMP_ROLES = ["Nhân viên bán hàng", "Thu ngân", "Quản lý cửa hàng", "Tư vấn viên"]

PROMO_TYPES = ["Percent", "Amount", "Bundle"]

@dataclass
class Config:
    customers: int = 100
    products: int = 180
    employees: int = 40
    stores: int = 10
    promotions: int = 15
    years: int = 3
    min_rows: int = 1000
    max_rows: int = 5000
    export_csv_dir: Optional[str] = None

@dataclass
class DbConfig:
    host: str
    port: int
    db: str
    user: str
    password: str


def load_config_from_env() -> Tuple[Config, DbConfig]:
    load_dotenv()
    cfg = Config(
        customers=int(os.getenv('CUSTOMERS', 100)),
        products=int(os.getenv('PRODUCTS', 180)),
        employees=int(os.getenv('EMPLOYEES', 40)),
        stores=int(os.getenv('STORES', 10)),
        promotions=int(os.getenv('PROMOTIONS', 15)),
        years=int(os.getenv('YEARS', 3)),
        min_rows=int(os.getenv('MIN_ROWS', 1000)),
        max_rows=int(os.getenv('MAX_ROWS', 5000)),
        export_csv_dir=os.getenv('EXPORT_CSV_DIR')
    )
    dbc = DbConfig(
        host=os.getenv('PG_HOST', 'localhost'),
        port=int(os.getenv('PG_PORT', 5432)),
        db=os.getenv('PG_DB', 'bi_courses'),
        user=os.getenv('PG_USER', 'postgres'),
        password=os.getenv('PG_PASSWORD', '1')
    )
    return cfg, dbc


def ensure_database(db: DbConfig):
    """Create database if not exists."""
    conn = psycopg2.connect(host=db.host, port=db.port, dbname='postgres', user=db.user, password=db.password)
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM pg_database WHERE datname=%s", (db.db,))
    exists = cur.fetchone() is not None
    if not exists:
        cur.execute(f'CREATE DATABASE "{db.db}"')
    cur.close()
    conn.close()


def get_conn(db: DbConfig):
    return psycopg2.connect(host=db.host, port=db.port, dbname=db.db, user=db.user, password=db.password)


def run_sql(conn, sql: str):
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()


def build_date_dim(years: int) -> pd.DataFrame:
    end = date.today()
    start = end - relativedelta(years=years)
    days = (end - start).days
    rows = []
    for i in range(days + 1):
        d = start + timedelta(days=i)
        rows.append({
            'date_key': int(d.strftime('%Y%m%d')),
            'full_date': d,
            'day': d.day,
            'week': int(d.strftime('%U')),
            'month': d.month,
            'month_name_vi': d.strftime('%m'),
            'quarter': (d.month - 1) // 3 + 1,
            'year': d.year,
            'is_weekend': d.weekday() >= 5
        })
    df = pd.DataFrame(rows)
    # Map month names to Vietnamese
    month_map = {
        '01': 'Tháng 1','02': 'Tháng 2','03': 'Tháng 3','04': 'Tháng 4','05': 'Tháng 5','06': 'Tháng 6',
        '07': 'Tháng 7','08': 'Tháng 8','09': 'Tháng 9','10': 'Tháng 10','11': 'Tháng 11','12': 'Tháng 12'
    }
    df['month_name_vi'] = df['month_name_vi'].map(month_map)
    return df


def build_customer_dim(n: int) -> pd.DataFrame:
    rows = []
    for i in range(n):
        city, province = random.choice(VN_CITIES)
        gender = random.choice(["Nam", "Nữ"])
        birth = fake.date_of_birth(minimum_age=18, maximum_age=45)
        rows.append({
            'customer_id': f'CUST-{i+1:04d}',
            'ho_ten': fake.name(),
            'gioi_tinh': gender,
            'ngay_sinh': birth,
            'so_dien_thoai': fake.phone_number(),
            'email': fake.free_email(),
            'dia_chi': fake.street_address(),
            'thanh_pho': city,
            'tinh_thanh': province
        })
    return pd.DataFrame(rows)


def build_customer_users(cust_df: pd.DataFrame) -> pd.DataFrame:
    """For each customer, create 0-5 product users (children) with gender and DOB."""
    rows = []
    for idx, _ in cust_df.iterrows():
        customer_key = int(idx) + 1  # matches SERIAL order of inserts
        count = random.randint(0, 5)
        for _ in range(count):
            gender = random.choice(["Nam", "Nữ"])
            # Child age: 0-10 years old
            years = random.randint(0, 10)
            # ensure valid date within last 'years'
            start = date.today() - relativedelta(years=years, days=random.randint(0, 364))
            dob = start
            rows.append({
                'customer_key': customer_key,
                'gioi_tinh': gender,
                'ngay_sinh': dob
            })
    return pd.DataFrame(rows)


def build_product_dim(n: int) -> pd.DataFrame:
    rows = []
    pid = 1
    while len(rows) < n:
        cat, brands = random.choice(PRODUCT_CATEGORIES)
        brand = random.choice(brands)
        unit = random.choice(UNITS)
        # Construct Vietnamese product names for mother & baby
        base_names = [
            "Sữa bột", "Tã dán", "Tã quần", "Bột ăn dặm", "Bánh ăn dặm", "Bình sữa", "Núm ti",
            "Xe đẩy", "Ghế ăn", "Ghế ngồi ô tô", "Sữa tắm gội", "Khăn ướt", "Bông tăm",
            "Quần áo sơ sinh", "Bộ quần áo"
        ]
        name = random.choice(base_names)
        size = random.choice(["400g", "800g", "1.2kg", "S", "M", "L", "XL", "XXL", "120ml", "240ml", "1 chiếc", "1 bộ"])
        product_name = f"{name} {brand} {size}"
        list_price = round(random.uniform(35000, 3500000), 0)
        rows.append({
            'product_id': f'PRD-{pid:04d}',
            'ten_san_pham': product_name,
            'danh_muc': cat,
            'thuong_hieu': brand,
            'don_vi': unit,
            'gia_niem_yet': list_price
        })
        pid += 1
    return pd.DataFrame(rows)


def build_employee_dim(n: int, stores: List[str]) -> pd.DataFrame:
    rows = []
    for i in range(n):
        rows.append({
            'employee_id': f'EMP-{i+1:04d}',
            'ho_ten': fake.name(),
            'chuc_danh': random.choice(EMP_ROLES),
            'cua_hang_mac_dinh': random.choice(stores)
        })
    return pd.DataFrame(rows)


def build_store_dim(n: int) -> pd.DataFrame:
    rows = []
    for i in range(n):
        city, province = random.choice(VN_CITIES)
        rows.append({
            'store_id': f'STO-{i+1:03d}',
            'ten_cua_hang': f"Cửa hàng Mẹ&Bé {i+1}",
            'dia_chi': fake.street_address(),
            'thanh_pho': city,
            'tinh_thanh': province
        })
    return pd.DataFrame(rows)


def build_promotion_dim(n: int, date_df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    dates = date_df['full_date'].tolist()
    for i in range(n):
        start = random.choice(dates[:-30])
        end = start + timedelta(days=random.randint(5, 30))
        ptype = random.choice(PROMO_TYPES)
        if ptype == 'Percent':
            value = round(random.uniform(5, 40), 2)
            name = f"Giảm {value}% toàn bộ danh mục"
        elif ptype == 'Amount':
            value = round(random.uniform(10000, 200000), 0)
            name = f"Giảm {int(value):,}đ cho đơn hàng".replace(',', '.')
        else:
            value = 1.0
            name = "Mua 2 tặng 1"
        rows.append({
            'promotion_id': f'PRO-{i+1:03d}',
            'ten_chuong_trinh': name,
            'loai': ptype,
            'gia_tri': value,
            'start_date': start,
            'end_date': end
        })
    return pd.DataFrame(rows)


def weighted_price(base: float) -> float:
    # Simulate price variability around list price
    return round(base * random.uniform(0.85, 1.05), 0)


def apply_promotion(price: float, qty: int, promo_row: Optional[pd.Series]) -> Tuple[float, float]:
    if promo_row is None:
        return 0.0, price * qty
    ptype = promo_row['loai']
    value = float(promo_row['gia_tri'])
    subtotal = price * qty
    if ptype == 'Percent':
        discount = subtotal * (value / 100.0)
    elif ptype == 'Amount':
        discount = min(value, subtotal * 0.6)  # cap to avoid negative
    else:  # Bundle simple model: buy 3 pay 2 equivalent
        free_units = qty // 3
        discount = free_units * price
    revenue = max(subtotal - discount, 0.0)
    return round(discount, 0), round(revenue, 0)


def build_sales_fact(
    min_rows: int,
    max_rows: int,
    date_df: pd.DataFrame,
    cust_df: pd.DataFrame,
    prod_df: pd.DataFrame,
    emp_df: pd.DataFrame,
    store_df: pd.DataFrame,
    promo_df: pd.DataFrame
) -> pd.DataFrame:
    n = random.randint(min_rows, max_rows)
    date_keys = date_df['date_key'].tolist()
    # index promotions by date for simple matching
    promo_by_date = {}
    for _, r in promo_df.iterrows():
        d = r['start_date']
        while d <= r['end_date']:
            promo_by_date.setdefault(int(d.strftime('%Y%m%d')), []).append(r)
            d += timedelta(days=1)
    rows = []
    for _ in range(n):
        dkey = random.choice(date_keys)
        p_opts = promo_by_date.get(dkey, [])
        promo_row = random.choice(p_opts) if p_opts and random.random() < 0.35 else None
        prod = prod_df.sample(1).iloc[0]
        price = weighted_price(float(prod['gia_niem_yet']))
        qty = random.choices([1,2,3,4,5,6], weights=[45,25,15,8,5,2])[0]
        discount, revenue = apply_promotion(price, qty, promo_row)
        rows.append({
            'date_key': dkey,
            'customer_key': int(cust_df.sample(1).index[0]) + 1 if not cust_df.empty else None,
            'product_key': int(prod_df.index[prod_df['product_id'] == prod['product_id']][0]) + 1,
            'employee_key': int(emp_df.sample(1).index[0]) + 1 if not emp_df.empty else None,
            'store_key': int(store_df.sample(1).index[0]) + 1 if not store_df.empty else None,
            'promotion_key': int(promo_df.index[promo_df['promotion_id'] == promo_row['promotion_id']][0]) + 1 if promo_row is not None else None,
            'so_luong': qty,
            'don_gia': price,
            'chiet_khau': discount,
            'doanh_thu': revenue
        })
    return pd.DataFrame(rows)


def create_schema(conn):
    from pathlib import Path
    schema_path = Path(__file__).with_name('schema.sql')
    sql = schema_path.read_text(encoding='utf-8')
    run_sql(conn, sql)


def truncate_tables(conn):
    run_sql(conn, "TRUNCATE TABLE Sales_Fact RESTART IDENTITY CASCADE;")
    run_sql(conn, "TRUNCATE TABLE KPI_Summary RESTART IDENTITY CASCADE;")
    run_sql(conn, "TRUNCATE TABLE Promotion_Dim RESTART IDENTITY CASCADE;")
    run_sql(conn, "TRUNCATE TABLE Store_Dim RESTART IDENTITY CASCADE;")
    run_sql(conn, "TRUNCATE TABLE Employee_Dim RESTART IDENTITY CASCADE;")
    run_sql(conn, "TRUNCATE TABLE Product_Dim RESTART IDENTITY CASCADE;")
    run_sql(conn, "TRUNCATE TABLE Customer_User RESTART IDENTITY CASCADE;")
    run_sql(conn, "TRUNCATE TABLE Customer_Dim RESTART IDENTITY CASCADE;")
    run_sql(conn, "TRUNCATE TABLE Date_Dim RESTART IDENTITY CASCADE;")


def insert_dim(conn, table: str, df: pd.DataFrame, add_serial_key: bool = False):
    def pyify(x: Any) -> Any:
        if pd.isna(x):
            return None
        if isinstance(x, (np.integer,)):
            return int(x)
        if isinstance(x, (np.floating,)):
            return float(x)
        if isinstance(x, (np.bool_,)):
            return bool(x)
        return x

    cols = list(df.columns)
    placeholders = ','.join(['%s']*len(cols))
    colnames = ','.join(cols)
    values = [tuple(pyify(x) for x in row) for row in df.itertuples(index=False, name=None)]
    with conn.cursor() as cur:
        cur.executemany(f"INSERT INTO {table} ({colnames}) VALUES ({placeholders})", values)
    conn.commit()


def insert_fact(conn, df: pd.DataFrame):
    def pyify(x: Any) -> Any:
        if x is None:
            return None
        # Handle pandas/NumPy NaN
        try:
            if pd.isna(x):
                return None
        except Exception:
            pass
        if isinstance(x, (np.integer,)):
            return int(x)
        if isinstance(x, (np.floating,)):
            return float(x)
        if isinstance(x, (np.bool_,)):
            return bool(x)
        return x

    cols = list(df.columns)
    colnames = ','.join(cols)
    values = [tuple(pyify(x) for x in row) for row in df.itertuples(index=False, name=None)]
    with conn.cursor() as cur:
        execute_values(cur,
                       f"INSERT INTO Sales_Fact ({colnames}) VALUES %s",
                       values,
                       page_size=5000)
    conn.commit()


def generate_and_load(cfg: Config, dbc: DbConfig):
    print("[1/6] Đảm bảo database tồn tại…")
    ensure_database(dbc)
    with get_conn(dbc) as conn:
        print("[2/6] Tạo schema…")
        create_schema(conn)
        print("[3/6] Làm sạch dữ liệu cũ…")
        truncate_tables(conn)

        print("[4/6] Tạo dữ liệu dimension…")
        date_df = build_date_dim(cfg.years)
        store_df = build_store_dim(cfg.stores)
        emp_df = build_employee_dim(cfg.employees, store_df['ten_cua_hang'].tolist())
        cust_df = build_customer_dim(cfg.customers)
        prod_df = build_product_dim(cfg.products)
        promo_df = build_promotion_dim(cfg.promotions, date_df)
        child_df = build_customer_users(cust_df)

        print("Chèn Date_Dim…")
        insert_dim(conn, 'Date_Dim', date_df[['date_key','full_date','day','week','month','month_name_vi','quarter','year','is_weekend']])
        print("Chèn Store_Dim…")
        insert_dim(conn, 'Store_Dim', store_df[['store_id','ten_cua_hang','dia_chi','thanh_pho','tinh_thanh']])
        print("Chèn Employee_Dim…")
        insert_dim(conn, 'Employee_Dim', emp_df[['employee_id','ho_ten','chuc_danh','cua_hang_mac_dinh']])
        print("Chèn Customer_Dim…")
        insert_dim(conn, 'Customer_Dim', cust_df[['customer_id','ho_ten','gioi_tinh','ngay_sinh','so_dien_thoai','email','dia_chi','thanh_pho','tinh_thanh']])
        if not child_df.empty:
            print("Chèn Customer_User…")
            insert_dim(conn, 'Customer_User', child_df[['customer_key','gioi_tinh','ngay_sinh']])
        print("Chèn Product_Dim…")
        insert_dim(conn, 'Product_Dim', prod_df[['product_id','ten_san_pham','danh_muc','thuong_hieu','don_vi','gia_niem_yet']])
        print("Chèn Promotion_Dim…")
        insert_dim(conn, 'Promotion_Dim', promo_df[['promotion_id','ten_chuong_trinh','loai','gia_tri','start_date','end_date']])

        print("[5/6] Tạo dữ liệu Sales_Fact…")
        sales_df = build_sales_fact(cfg.min_rows, cfg.max_rows, date_df, cust_df, prod_df, emp_df, store_df, promo_df)

        # Optional CSV export
        if cfg.export_csv_dir:
            os.makedirs(cfg.export_csv_dir, exist_ok=True)
            date_df.to_csv(os.path.join(cfg.export_csv_dir, 'Date_Dim.csv'), index=False)
            store_df.to_csv(os.path.join(cfg.export_csv_dir, 'Store_Dim.csv'), index=False)
            emp_df.to_csv(os.path.join(cfg.export_csv_dir, 'Employee_Dim.csv'), index=False)
            cust_df.to_csv(os.path.join(cfg.export_csv_dir, 'Customer_Dim.csv'), index=False)
            if not child_df.empty:
                child_df.to_csv(os.path.join(cfg.export_csv_dir, 'Customer_User.csv'), index=False)
            prod_df.to_csv(os.path.join(cfg.export_csv_dir, 'Product_Dim.csv'), index=False)
            promo_df.to_csv(os.path.join(cfg.export_csv_dir, 'Promotion_Dim.csv'), index=False)
            sales_df.to_csv(os.path.join(cfg.export_csv_dir, 'Sales_Fact.csv'), index=False)

        print("[6/6] Chèn Sales_Fact…")
        insert_fact(conn, sales_df[['date_key','customer_key','product_key','employee_key','store_key','promotion_key','so_luong','don_gia','chiet_khau','doanh_thu']])
        print("Hoàn tất!")

        # Build KPI summary after facts inserted
        print("Tính toán KPI_Summary…")
        kpi = sales_df.groupby(['date_key','store_key']).agg(
            line_count=('so_luong','size'),
            total_quantity=('so_luong','sum'),
            total_discount=('chiet_khau','sum'),
            total_revenue=('doanh_thu','sum')
        ).reset_index()
        kpi['avg_ticket'] = (kpi['total_revenue'] / kpi['line_count']).round(2)

        insert_dim(conn, 'KPI_Summary', kpi[['date_key','store_key','line_count','total_quantity','total_discount','total_revenue','avg_ticket']])


if __name__ == '__main__':
    cfg, dbc = load_config_from_env()
    generate_and_load(cfg, dbc)
