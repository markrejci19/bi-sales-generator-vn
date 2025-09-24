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

# Simple region mapping by province/city
MIEN_BAC = {"Hà Nội", "Hải Phòng", "Quảng Ninh", "Hải Dương", "Hưng Yên", "Bắc Ninh", "Nam Định", "Thái Bình", "Ninh Bình", "Vĩnh Phúc", "Phú Thọ"}
MIEN_TRUNG = {"Đà Nẵng", "Thừa Thiên Huế", "Nghệ An", "Thanh Hóa", "Quảng Bình", "Quảng Trị", "Quảng Nam", "Khánh Hòa", "Bình Định", "Nha Trang"}
MIEN_NAM = {"Hồ Chí Minh", "Đồng Nai", "Bình Dương", "Cần Thơ", "Bà Rịa - Vũng Tàu", "Bến Tre", "Long An", "Tây Ninh", "Vĩnh Long", "Tiền Giang", "An Giang"}

def classify_mien(city: str | None, province: str | None) -> str:
    p = (province or city or "").strip()
    if p in MIEN_BAC:
        return "Miền Bắc"
    if p in MIEN_TRUNG:
        return "Miền Trung"
    if p in MIEN_NAM:
        return "Miền Nam"
    return "Miền Nam"  # default fallback

PRODUCT_CATEGORIES = [
    ("Sữa bột", ["Dielac", "Friso", "Aptamil", "Nan", "Glico"]),
    ("Tã/bỉm", ["Pampers", "Moony", "Merries", "Huggies", "Bobby"]),
    ("Đồ ăn dặm", ["Heinz", "Gerber", "Nestlé", "Ella's"]),
    ("Bình sữa & núm ti", ["Pigeon", "Chicco", "Philips Avent", "Comotomo"]),
    ("Xe đẩy & ghế ngồi", ["Aprica", "Joie", "Graco", "Combi"]),
    ("Đồ vệ sinh", ["Kodomo", "Dnee", "Johnson's", "Lactacyd"]),
    ("Quần áo sơ sinh", ["Carters", "Gap", "H&M", "Ninomaxx"]),
]

# Category-specific specs to ensure product names align with categories
CATEGORY_SPECS = {
    "Sữa bột": {
        "base_names": ["Sữa bột công thức", "Sữa bột dinh dưỡng", "Sữa công thức"],
        "sizes": ["400g", "800g", "1.2kg"],
        "units": ["hộp", "lon"],
        # Giá niêm yết tham khảo theo cỡ hộp
        "price": (250_000, 1_200_000),
    },
    "Tã/bỉm": {
        "base_names": ["Tã dán", "Tã quần"],
        "sizes": ["NB", "S", "M", "L", "XL", "XXL"],
        "units": ["bịch", "gói"],
        "price": (120_000, 600_000),
    },
    "Đồ ăn dặm": {
        "base_names": ["Bột ăn dặm", "Bánh ăn dặm", "Pouch ăn dặm"],
        "sizes": ["120g", "200g", "250g"],
        "units": ["hộp", "gói"],
        "price": (25_000, 120_000),
    },
    "Bình sữa & núm ti": {
        "base_names": ["Bình sữa", "Núm ti"],
        "sizes": ["120ml", "160ml", "240ml"],
        "units": ["cái", "bộ"],
        "price": (60_000, 600_000),
    },
    "Xe đẩy & ghế ngồi": {
        "base_names": ["Xe đẩy", "Ghế ngồi ô tô", "Ghế ăn dặm"],
        "sizes": ["1 chiếc"],
        "units": ["cái"],
        "price": (800_000, 7_000_000),
    },
    "Đồ vệ sinh": {
        "base_names": ["Sữa tắm gội", "Khăn ướt", "Bông tăm", "Nước giặt đồ em bé"],
        "sizes": ["200ml", "500ml", "100 tờ"],
        "units": ["chai", "gói"],
        "price": (20_000, 250_000),
    },
    "Quần áo sơ sinh": {
        "base_names": ["Bộ quần áo sơ sinh", "Bodysuit", "Bao tay chân"],
        "sizes": ["S", "M", "L"],
        "units": ["bộ"],
        "price": (40_000, 300_000),
    },
}

# Helper map for brands per category
CATEGORY_BRANDS = {cat: brands for cat, brands in PRODUCT_CATEGORIES}

UNITS = ["hộp", "bịch", "gói", "chai", "cái", "bộ"]

EMP_ROLES = ["Nhân viên bán hàng", "Thu ngân", "Quản lý cửa hàng", "Tư vấn viên"]

PROMO_TYPES = ["Percent", "Amount", "Bundle"]

# Online marketplace stores (fixed IDs) used for Online orders
ONLINE_PLATFORM_STORES = [
    ("ONL-SHOPEE", "Shopee"),
    ("ONL-LAZADA", "Lazada"),
    ("ONL-TIKTOK", "TikTok"),
    ("ONL-FACEBOOK", "Facebook"),
    ("ONL-WEBAPP", "Web-App"),
]

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
    db_export_dir: Optional[str] = None  # export tables from DB to CSV with UTF-8 BOM
    # Monthly active customers range per month
    monthly_active_min: int = 700
    monthly_active_max: int = 900

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
        # New env vars with backward compatibility
        monthly_active_min=int(os.getenv('MONTHLY_ACTIVE_MIN', 700)),
        monthly_active_max=int(os.getenv('MONTHLY_ACTIVE_MAX', 900)),
        export_csv_dir=os.getenv('EXPORT_CSV_DIR'),
        db_export_dir=os.getenv('DB_EXPORT_DIR')
    )
    # Backward compatibility: if MONTHLY_ACTIVE_CUSTOMERS provided, pin min=max=value
    legacy_mac = os.getenv('MONTHLY_ACTIVE_CUSTOMERS')
    if legacy_mac is not None and legacy_mac != "":
        try:
            v = int(legacy_mac)
            cfg.monthly_active_min = v
            cfg.monthly_active_max = v
        except ValueError:
            pass
    # Ensure min <= max
    if cfg.monthly_active_min > cfg.monthly_active_max:
        cfg.monthly_active_min, cfg.monthly_active_max = cfg.monthly_active_max, cfg.monthly_active_min
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
            'date_id': int(d.strftime('%Y%m%d')),
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
        # Loyalty points and tier
        pts = random.randint(0, 20000)
        if pts < 1000:
            tier = 'Bronze'
        elif pts < 5000:
            tier = 'Silver'
        elif pts < 15000:
            tier = 'Gold'
        else:
            tier = 'Platinum'
        rows.append({
            'customer_id': f'CUST-{i+1:04d}',
            'ho_ten': fake.name(),
            'gioi_tinh': gender,
            'ngay_sinh': birth,
            'so_dien_thoai': fake.phone_number(),
            'email': fake.free_email(),
            'dia_chi': fake.street_address(),
            'thanh_pho': city,
            'tinh_thanh': province,
            'point': pts,
            'tier': tier
        })
    return pd.DataFrame(rows)


def build_customer_children(cust_df: pd.DataFrame) -> pd.DataFrame:
    """For each customer, create 0-5 product users (children) with name, gender and DOB."""
    rows = []
    for _, row in cust_df.iterrows():
        customer_id = row['customer_id']
        count = random.randint(0, 5)
        for _ in range(count):
            gender = random.choice(["Nam", "Nữ"])
            # Child age: 0-10 years old
            years = random.randint(0, 10)
            # ensure valid date within last 'years'
            start = date.today() - relativedelta(years=years, days=random.randint(0, 364))
            dob = start
            rows.append({
                'customer_id': customer_id,
                'ho_ten': fake.first_name_male() if gender == 'Nam' else fake.first_name_female(),
                'gioi_tinh': gender,
                'ngay_sinh': dob
            })
    return pd.DataFrame(rows)


def build_product_dim(n: int) -> pd.DataFrame:
    """Generate products with names consistent to their category (danh_muc)."""
    rows = []
    pid = 1
    # Distribute products across categories fairly
    cats = list(CATEGORY_SPECS.keys())
    while len(rows) < n:
        cat = random.choice(cats)
        spec = CATEGORY_SPECS[cat]
        brand = random.choice(CATEGORY_BRANDS[cat])
        unit = random.choice(spec["units"]) if spec.get("units") else random.choice(UNITS)
        base_name = random.choice(spec["base_names"]) if spec.get("base_names") else cat
        size = random.choice(spec["sizes"]) if spec.get("sizes") else ""
        min_p, max_p = spec.get("price", (35_000, 3_500_000))
        # Chọn giá trong khoảng và làm tròn đến nghìn cho thực tế Việt Nam
        raw_price = random.uniform(min_p, max_p)
        list_price = int(round(raw_price / 1000.0) * 1000)
        size_part = f" {size}" if size else ""
        product_name = f"{base_name} {brand}{size_part}"
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


def build_product_daily_costs(date_df: pd.DataFrame, prod_df: pd.DataFrame) -> pd.DataFrame:
    """Generate daily input cost per product with smooth constraints approximated:
    - Day-to-day change within ~±2% (<=3% hard limit enforced by DB trigger)
    - Over any year, cost stays within roughly ±15% band (~<=20% enforced by trigger)
    """
    if prod_df.empty or date_df.empty:
        return pd.DataFrame(columns=['product_id','date_id','cost'])
    # Use only business days or all days? We'll use all days for simplicity
    dates = date_df[['date_id','full_date']].sort_values('full_date').reset_index(drop=True)
    out_rows: list[dict[str, Any]] = []
    for prod in prod_df.itertuples(index=False):
        base = float(prod.gia_niem_yet)
        # Start baseline cost a bit below list price (e.g., 70%–90% of list)
        cost0 = base * random.uniform(0.7, 0.9)
        # Keep a slow-moving anchor to avoid exceeding 20% annually
        anchor = cost0
        anchor_reset_date = dates.loc[0, 'full_date']
        prev_cost = None
        for i, row in dates.iterrows():
            dkey = int(row['date_id'])
            cur_date = row['full_date']
            # Every ~30-60 days, allow anchor to drift slightly (±3%)
            if (cur_date - anchor_reset_date).days >= random.randint(30, 60):
                anchor *= random.uniform(0.98, 1.02)
                anchor_reset_date = cur_date
            if prev_cost is None:
                c = anchor
            else:
                # Small day-to-day walk within ±1.5% around anchor pull
                step = random.uniform(-0.015, 0.015)
                c = prev_cost * (1 + step)
                # Pull back toward anchor a bit
                c = (0.8 * c) + (0.2 * anchor)
            # Soft bounds relative to anchor: keep within ~±15%
            lo = anchor * 0.85
            hi = anchor * 1.15
            c = max(lo, min(hi, c))
            # Ensure non-zero and reasonable minimum
            c = max(1000.0, c)
            out_rows.append({'product_id': prod.product_id, 'date_id': dkey, 'cost': round(float(c), 2)})
            prev_cost = c
    return pd.DataFrame(out_rows)


def refresh_products_only(dbc: DbConfig):
    """Regenerate products attributes in place (update only) to keep FKs intact."""
    with get_conn(dbc) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM products ORDER BY id")
            ids = [r[0] for r in cur.fetchall()]
        if not ids:
            print("Không có sản phẩm nào để cập nhật.")
            return
        new_df = build_product_dim(len(ids))
        # Preserve existing product_id order
        new_df['product_id'] = ids
        # Normalize numpy types
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
        updates = [
            (
                pyify(row.ten_san_pham),
                pyify(row.danh_muc),
                pyify(row.thuong_hieu),
                pyify(row.don_vi),
                pyify(row.gia_niem_yet),
                pyify(row.product_id),
            )
            for row in new_df.itertuples(index=False)
        ]
        with conn.cursor() as cur:
            cur.executemany(
                """
                UPDATE products
                SET ten_san_pham=%s,
                    danh_muc=%s,
                    thuong_hieu=%s,
                    don_vi=%s,
                    gia_niem_yet=%s
                WHERE id=%s
                """,
                updates,
            )
        conn.commit()
        print(f"Đã cập nhật lại {len(ids)} sản phẩm trong products (chỉ update).")


def refresh_stores_only(dbc: DbConfig):
    """Regenerate store attributes in place while preserving existing store IDs and FKs.
    - Updates offline stores (ids like 'STO-###') with new name/address/city/province/mien.
    - Ensures online platform stores exist and are set to mien='Online'.
    - Does not touch other tables.
    """
    online_id_set = {sid for sid, _ in ONLINE_PLATFORM_STORES}
    with get_conn(dbc) as conn:
        with conn.cursor() as cur:
            # Ensure column exists
            cur.execute("ALTER TABLE IF EXISTS stores ADD COLUMN IF NOT EXISTS mien VARCHAR(20)")
            # Load current store ids
            cur.execute("SELECT id FROM stores ORDER BY id")
            all_ids = [r[0] for r in cur.fetchall()]
        # Partition
        offline_ids = sorted([sid for sid in all_ids if sid and sid.startswith('STO-')])
        # Build fresh offline rows
        new_df = build_store_dim(len(offline_ids))
        new_offline = new_df[new_df['store_type'] == 'Offline'].copy()
        # Map generated rows to existing ids in order
        new_offline.sort_index(inplace=True)
        # If counts mismatch, trim or pad (pad from generator)
        if len(new_offline) > len(offline_ids):
            new_offline = new_offline.iloc[:len(offline_ids)].copy()
        elif len(new_offline) < len(offline_ids):
            # generate extra to match
            extra_needed = len(offline_ids) - len(new_offline)
            extra_df = build_store_dim(extra_needed)
            extra_off = extra_df[extra_df['store_type'] == 'Offline'].copy()
            new_offline = pd.concat([new_offline, extra_off.iloc[:extra_needed]], ignore_index=True)
        new_offline['store_id'] = offline_ids
        # Prepare updates
        up_rows = [
            (
                row.ten_cua_hang,
                row.dia_chi,
                row.thanh_pho,
                row.tinh_thanh,
                row.mien,
                row.store_id,
            )
            for row in new_offline.itertuples(index=False)
        ]
        with conn.cursor() as cur:
            # Update offline stores
            cur.executemany(
                """
                UPDATE stores
                SET ten_cua_hang=%s,
                    dia_chi=%s,
                    thanh_pho=%s,
                    tinh_thanh=%s,
                    mien=%s
                WHERE id=%s
                """,
                up_rows,
            )
            # Upsert online platform stores
            platform_rows = [
                (sid, name, None, None, None, 'Online')
                for sid, name in ONLINE_PLATFORM_STORES
            ]
            cur.executemany(
                """
                INSERT INTO stores (id, ten_cua_hang, dia_chi, thanh_pho, tinh_thanh, mien)
                VALUES (%s,%s,%s,%s,%s,%s)
                ON CONFLICT (id) DO UPDATE SET
                    ten_cua_hang=EXCLUDED.ten_cua_hang,
                    dia_chi=EXCLUDED.dia_chi,
                    thanh_pho=EXCLUDED.thanh_pho,
                    tinh_thanh=EXCLUDED.tinh_thanh,
                    mien=EXCLUDED.mien
                """,
                platform_rows,
            )
        conn.commit()
    print(f"Đã cập nhật {len(offline_ids)} cửa hàng offline và đồng bộ cửa hàng online (chỉ update stores).")


def build_employee_dim(n: int, stores: List[str]) -> pd.DataFrame:
    rows = []
    for i in range(n):
        rows.append({
            'employee_id': f'EMP-{i+1:04d}',
            'ho_ten': fake.name(),
            'chuc_danh': random.choice(EMP_ROLES),
            # If no stores provided, leave default store empty
            'cua_hang_mac_dinh': (random.choice(stores) if stores else None)
        })
    return pd.DataFrame(rows)


def build_store_dim(n: int) -> pd.DataFrame:
    rows = []
    # Offline physical stores
    for i in range(n):
        city, province = random.choice(VN_CITIES)
        rows.append({
            'store_id': f'STO-{i+1:03d}',
            'ten_cua_hang': f"Cửa hàng Mẹ&Bé {i+1}",
            'dia_chi': fake.street_address(),
            'thanh_pho': city,
            'tinh_thanh': province,
            'mien': classify_mien(city, province),
            'store_type': 'Offline'
        })
    # Online platform stores
    for sid, name in ONLINE_PLATFORM_STORES:
        rows.append({
            'store_id': sid,
            'ten_cua_hang': name,
            'dia_chi': None,
            'thanh_pho': None,
            'tinh_thanh': None,
            'mien': 'Online',
            'store_type': 'Online'
        })
    # Ensure DataFrame always has expected columns even when n == 0
    return pd.DataFrame(rows, columns=['store_id','ten_cua_hang','dia_chi','thanh_pho','tinh_thanh','mien','store_type'])


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
    # Giá bán thực tế dao động nhẹ quanh giá niêm yết (ưu đãi nhẹ)
    price = base * random.uniform(0.95, 1.02)
    return int(round(price / 1000.0) * 1000)


def compute_item_discounts(price: float, qty: int, promo_row: Optional[pd.Series]) -> Tuple[float, float]:
    """Return (promo_per_unit, extra_discount_per_unit) ensuring promo+discount < price."""
    promo_unit = 0.0
    if promo_row is not None:
        ptype = promo_row['loai']
        value = float(promo_row['gia_tri'])
        if ptype == 'Percent':
            promo_unit = price * (value / 100.0)
        elif ptype == 'Amount':
            # Distribute amount per unit conservatively and cap at 60% price
            promo_unit = min(max(value / max(1, qty), 0.0), price * 0.6)
        else:  # Bundle: approximate per-unit benefit
            free_units = qty // 3
            promo_unit = (free_units * price) / max(qty, 1)
    # Extra discount up to 10% of price
    discount_unit = random.uniform(0, price * 0.1)
    # Enforce promo + discount < price
    promo_unit = min(promo_unit, price - 1)
    discount_unit = min(discount_unit, max(0.0, price - 1 - promo_unit))
    return round(promo_unit, 0), round(discount_unit, 0)


def build_orders(
    min_rows: int,
    max_rows: int,
    date_df: pd.DataFrame,
    cust_df: pd.DataFrame,
    prod_df: pd.DataFrame,
    emp_df: pd.DataFrame,
    store_df: pd.DataFrame,
    promo_df: pd.DataFrame,
    monthly_active_min: int = 700,
    monthly_active_max: int = 900,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    n_orders = random.randint(min_rows, max_rows)
    date_keys = date_df['date_id'].tolist()
    # Map date_id -> year_month
    date_df = date_df.copy()
    date_df['year_month'] = date_df['date_id'].astype(str).str.slice(0, 6).astype(int)
    dkey_to_month = dict(zip(date_df['date_id'], date_df['year_month']))
    months = sorted(date_df['year_month'].unique().tolist())
    # Build monthly active customer sets and cycling indices
    all_cust_ids = cust_df['customer_id'].tolist() if not cust_df.empty else []
    month_active_map: dict[int, list[str]] = {}
    month_cycle_idx: dict[int, int] = {}
    for m in months:
        if all_cust_ids:
            # choose active set size in [min, max]
            try:
                lo = int(monthly_active_min)
                hi = int(monthly_active_max)
            except Exception:
                lo, hi = 700, 900
            if lo > hi:
                lo, hi = hi, lo
            k = random.randint(lo, hi)
            k = min(k, len(all_cust_ids))
            act = random.sample(all_cust_ids, k)
            random.shuffle(act)
            month_active_map[m] = act
            month_cycle_idx[m] = 0
        else:
            month_active_map[m] = []
            month_cycle_idx[m] = 0
    # Partition stores
    offline_df = store_df[store_df.get('store_type', 'Offline') == 'Offline'] if not store_df.empty else store_df
    online_df = store_df[store_df.get('store_type', '') == 'Online'] if not store_df.empty else store_df
    offline_ids = offline_df['store_id'].tolist() if not offline_df.empty else []
    online_ids = online_df['store_id'].tolist() if not online_df.empty else []
    # Build Pareto-like weights: top 30% stores take 70% of offline traffic
    offline_probs = None
    if len(offline_ids) > 0:
        idxs = list(range(len(offline_ids)))
        random.shuffle(idxs)
        k = max(1, int(math.ceil(0.3 * len(offline_ids))))
        top_set = set(idxs[:k])
        rest = len(offline_ids) - k
        w_top = 0.7 / k
        w_rest = (0.3 / rest) if rest > 0 else 0.0
        offline_probs = [w_top if i in top_set else w_rest for i in range(len(offline_ids))]
    # index promotions by date for simple matching
    promo_by_date: dict[int, list[pd.Series]] = {}
    for _, r in promo_df.iterrows():
        d = r['start_date']
        while d <= r['end_date']:
            promo_by_date.setdefault(int(d.strftime('%Y%m%d')), []).append(r)
            d += timedelta(days=1)
    headers = []
    items = []
    for oid in range(1, n_orders + 1):
        dkey = random.choice(date_keys)
        # Pick channel first
        channel = 'Online' if random.random() < 0.35 else 'Offline'
        # Select store per channel
        if channel == 'Online' and online_ids:
            store_id = random.choice(online_ids)
            emp = None  # marketplace orders typically no in-store employee
        else:
            if offline_ids:
                if offline_probs is not None:
                    # weighted choice
                    store_id = random.choices(offline_ids, weights=offline_probs, k=1)[0]
                else:
                    store_id = random.choice(offline_ids)
            else:
                store_id = None
            emp = emp_df.sample(1).iloc[0] if not emp_df.empty else None
        # Choose customer based on month activity
        month = dkey_to_month.get(dkey)
        cust = None
        if month is not None and month_active_map.get(month):
            idx = month_cycle_idx[month]
            cust_id = month_active_map[month][idx]
            month_cycle_idx[month] = (idx + 1) % len(month_active_map[month])
            # Construct a minimal Series-like for consistency
            cust = pd.Series({'customer_id': cust_id})
        headers.append({
            'order_id': oid,
            'date_id': dkey,
            'customer_id': (cust['customer_id'] if cust is not None else None),
            'employee_id': (emp['employee_id'] if emp is not None else None),
            'store_id': store_id,
            'channel': channel,
        })
        # Items 1-5 unique products
        item_count = random.randint(1, 5)
        prods = prod_df.sample(item_count, replace=False).itertuples(index=False)
        p_opts = promo_by_date.get(dkey, [])
        promo_row = random.choice(p_opts) if p_opts and random.random() < 0.35 else None
        for prod in prods:
            price = weighted_price(float(prod.gia_niem_yet))
            qty = random.choices([1,2,3,4,5,6], weights=[45,25,15,8,5,2])[0]
            km_unit, ck_unit = compute_item_discounts(price, qty, promo_row)
            line_rev = max((price - km_unit - ck_unit) * qty, 0.0)
            items.append({
                'order_id': oid,
                'product_id': prod.product_id,
                'promotion_id': (promo_row['promotion_id'] if promo_row is not None else None),
                'so_luong': qty,
                'don_gia': price,
                'khuyen_mai': km_unit,
                'chiet_khau': ck_unit,
                'doanh_thu': round(line_rev, 0)
            })
    return pd.DataFrame(headers), pd.DataFrame(items)


def create_schema(conn):
    from pathlib import Path
    schema_path = Path(__file__).with_name('schema.sql')
    sql = schema_path.read_text(encoding='utf-8')
    run_sql(conn, sql)


def truncate_tables(conn):
    # Truncate in FK-safe order
    run_sql(conn, "TRUNCATE TABLE order_items RESTART IDENTITY CASCADE;")
    run_sql(conn, "TRUNCATE TABLE orders RESTART IDENTITY CASCADE;")
    run_sql(conn, "TRUNCATE TABLE KPI_Target_Monthly RESTART IDENTITY CASCADE;")
    run_sql(conn, "TRUNCATE TABLE promotions RESTART IDENTITY CASCADE;")
    run_sql(conn, "TRUNCATE TABLE stores RESTART IDENTITY CASCADE;")
    run_sql(conn, "TRUNCATE TABLE employees RESTART IDENTITY CASCADE;")
    run_sql(conn, "TRUNCATE TABLE products RESTART IDENTITY CASCADE;")
    run_sql(conn, "TRUNCATE TABLE customer_child RESTART IDENTITY CASCADE;")
    run_sql(conn, "TRUNCATE TABLE customers RESTART IDENTITY CASCADE;")
    run_sql(conn, "TRUNCATE TABLE dates RESTART IDENTITY CASCADE;")


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


def insert_orders(conn, df: pd.DataFrame):
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
                       f"INSERT INTO orders ({colnames}) VALUES %s",
                       values,
                       page_size=5000)
    conn.commit()


def insert_order_items(conn, df: pd.DataFrame):
    def pyify(x: Any) -> Any:
        if x is None:
            return None
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
                       f"INSERT INTO order_items ({colnames}) VALUES %s",
                       values,
                       page_size=5000)
    conn.commit()


def generate_and_load(cfg: Config, dbc: DbConfig):
    # Detect export-only intent: all sizes/years/rows set to 0 and export folder specified
    export_only = (
        (cfg.customers == 0 and cfg.products == 0 and cfg.employees == 0 and cfg.stores == 0 and cfg.promotions == 0)
        and (cfg.years == 0)
        and (cfg.min_rows == 0 and cfg.max_rows == 0)
        and (cfg.db_export_dir is not None)
    )

    print("[1/6] Đảm bảo database tồn tại…")
    ensure_database(dbc)

    if export_only:
        # Do not touch schema or truncate; just export existing tables
        print("[2/2] Chế độ chỉ xuất CSV từ database hiện có…")
        export_tables_to_csv(dbc, cfg.db_export_dir)
        print("Xuất CSV hoàn tất.")
        return

    with get_conn(dbc) as conn:
        print("[2/6] Tạo schema…")
        create_schema(conn)
        print("[3/6] Làm sạch dữ liệu cũ…")
        truncate_tables(conn)

        print("[4/6] Tạo dữ liệu dimension…")
        date_df = build_date_dim(cfg.years)
        store_df = build_store_dim(cfg.stores)
        offline_names = store_df.loc[store_df.get('store_type', 'Offline') == 'Offline', 'ten_cua_hang'].dropna().tolist()
        emp_df = build_employee_dim(cfg.employees, offline_names)
        cust_df = build_customer_dim(cfg.customers)
        prod_df = build_product_dim(cfg.products)
        promo_df = build_promotion_dim(cfg.promotions, date_df)
        child_df = build_customer_children(cust_df)

        print("Chèn dates…")
        insert_dim(conn, 'dates', date_df[['date_id','full_date','day','week','month','month_name_vi','quarter','year','is_weekend']])
        print("Chèn stores…")
        df_store = store_df.rename(columns={'store_id':'id'})
        # stores table doesn't have store_type column; insert supported columns including 'mien'
        insert_dim(conn, 'stores', df_store[['id','ten_cua_hang','dia_chi','thanh_pho','tinh_thanh','mien']])
        print("Chèn employees…")
        df_emp = emp_df.rename(columns={'employee_id':'id'})
        insert_dim(conn, 'employees', df_emp[['id','ho_ten','chuc_danh','cua_hang_mac_dinh']])
        print("Chèn customers…")
        df_cust = cust_df.rename(columns={'customer_id':'id'})
        insert_dim(conn, 'customers', df_cust[['id','ho_ten','gioi_tinh','ngay_sinh','so_dien_thoai','email','dia_chi','thanh_pho','tinh_thanh','point','tier']])
        if not child_df.empty:
            print("Chèn customer_child…")
            insert_dim(conn, 'customer_child', child_df[['customer_id','ho_ten','gioi_tinh','ngay_sinh']])
        print("Chèn products…")
        df_prod = prod_df.rename(columns={'product_id':'id'})
        insert_dim(conn, 'products', df_prod[['id','ten_san_pham','danh_muc','thuong_hieu','don_vi','gia_niem_yet']])
        # Build and insert product daily costs
        print("Chèn product_daily_costs…")
        pdc_df = build_product_daily_costs(date_df, prod_df)
        if not pdc_df.empty:
            insert_dim(conn, 'product_daily_costs', pdc_df[['product_id','date_id','cost']])
        print("Chèn promotions…")
        df_promo = promo_df.rename(columns={'promotion_id':'id'})
        insert_dim(conn, 'promotions', df_promo[['id','ten_chuong_trinh','loai','gia_tri','start_date','end_date']])

        print("[5/6] Tạo dữ liệu orders + order_items…")
        orders_df, items_df = build_orders(
            cfg.min_rows,
            cfg.max_rows,
            date_df,
            cust_df,
            prod_df,
            emp_df,
            store_df,
            promo_df,
            monthly_active_min=cfg.monthly_active_min,
            monthly_active_max=cfg.monthly_active_max,
        )

        # Optional CSV export
        if cfg.export_csv_dir:
            os.makedirs(cfg.export_csv_dir, exist_ok=True)
            date_df.to_csv(os.path.join(cfg.export_csv_dir, 'dates.csv'), index=False)
            df_store.to_csv(os.path.join(cfg.export_csv_dir, 'stores.csv'), index=False)
            df_emp.to_csv(os.path.join(cfg.export_csv_dir, 'employees.csv'), index=False)
            df_cust.to_csv(os.path.join(cfg.export_csv_dir, 'customers.csv'), index=False)
            if not child_df.empty:
                child_df.to_csv(os.path.join(cfg.export_csv_dir, 'customer_child.csv'), index=False)
            df_prod.to_csv(os.path.join(cfg.export_csv_dir, 'products.csv'), index=False)
            if not pdc_df.empty:
                pdc_df.to_csv(os.path.join(cfg.export_csv_dir, 'product_daily_costs.csv'), index=False)
            df_promo.to_csv(os.path.join(cfg.export_csv_dir, 'promotions.csv'), index=False)
            orders_df.to_csv(os.path.join(cfg.export_csv_dir, 'orders.csv'), index=False)
            items_df.to_csv(os.path.join(cfg.export_csv_dir, 'order_items.csv'), index=False)

        print("[6/6] Chèn orders…")
        insert_orders(conn, orders_df[['order_id','date_id','customer_id','employee_id','store_id','channel']])
        print("Chèn order_items…")
        insert_order_items(conn, items_df[['order_id','product_id','promotion_id','so_luong','don_gia','khuyen_mai','chiet_khau','doanh_thu']])
        print("Hoàn tất!")

        # Build Monthly KPI targets per store (non-decreasing month over month)
        # Join items with orders to get date_id and store_id
        items_join = items_df.merge(orders_df[['order_id','date_id','store_id']], on='order_id', how='left')
        items_join['year_month'] = items_join['date_id'].astype(str).str.slice(0, 6).astype(int)
        monthly = items_join.groupby(['store_id','year_month']).agg(
            doanh_thu=('doanh_thu','sum'),
            so_luong_don_hang=('order_id','nunique'),
            so_luong_san_pham=('so_luong','sum')
        ).reset_index()
        # Enforce non-decreasing targets month-over-month per store
        monthly.sort_values(['store_id','year_month'], inplace=True)
        def monotonic_targets(df_store: pd.DataFrame) -> pd.DataFrame:
            max_rev = 0.0
            max_orders = 0
            max_qty = 0
            rows = []
            for _, r in df_store.iterrows():
                max_rev = max(max_rev, float(r['doanh_thu']))
                max_orders = max(max_orders, int(r['so_luong_don_hang']))
                max_qty = max(max_qty, int(r['so_luong_san_pham']))
                rows.append({
                    'store_id': r['store_id'],
                    'year_month': int(r['year_month']),
                    'doanh_thu': round(max_rev, 2),
                    'so_luong_don_hang': max_orders,
                    'so_luong_san_pham': max_qty,
                })
            return pd.DataFrame(rows)
        target_df = (
            monthly.groupby('store_id', group_keys=False)
            .apply(monotonic_targets)
            .reset_index(drop=True)
        )
        # Clean and insert
        run_sql(conn, "TRUNCATE TABLE KPI_Target_Monthly RESTART IDENTITY CASCADE;")
        insert_dim(conn, 'KPI_Target_Monthly', target_df[['store_id','year_month','doanh_thu','so_luong_don_hang','so_luong_san_pham']])

        # Optional: export DB tables to CSV with UTF-8 BOM (friendly for Vietnamese in Excel)
        if cfg.db_export_dir:
            export_tables_to_csv(dbc, cfg.db_export_dir)


def export_tables_to_csv(dbc: DbConfig, out_dir: str, tables: Optional[List[str]] = None):
    """Export selected DB tables to CSV using PostgreSQL COPY.
    Files are written with UTF-8 BOM (utf-8-sig) to support Vietnamese in Excel.
    """
    os.makedirs(out_dir, exist_ok=True)
    if tables is None:
        tables = [
            'dates', 'customers', 'customer_child', 'products',
            'employees', 'stores', 'promotions', 'product_daily_costs', 'orders',
            'order_items', 'KPI_Target_Monthly'
        ]
    with get_conn(dbc) as conn:
        with conn.cursor() as cur:
            for t in tables:
                target = os.path.join(out_dir, f"{t}.csv")
                # Open with utf-8-sig to emit BOM; COPY writes text rows to this handle
                with open(target, 'w', encoding='utf-8-sig', newline='') as f:
                    sql = f"COPY {t} TO STDOUT WITH (FORMAT CSV, HEADER TRUE)"
                    try:
                        cur.copy_expert(sql, f)
                        print(f"Đã xuất {t} -> {target}")
                    except Exception as e:
                        # Skip tables that might not exist or other copy errors
                        print(f"Bỏ qua bảng {t}: {e}")


if __name__ == '__main__':
    cfg, dbc = load_config_from_env()
    generate_and_load(cfg, dbc)


# --- Utilities for one-off migrations (not executed on import) ---
def migrate_add_mien_to_stores(dbc: DbConfig):
    """Add 'mien' column to stores and backfill values without touching other tables."""
    online_ids = [sid for sid, _ in ONLINE_PLATFORM_STORES]
    with get_conn(dbc) as conn:
        with conn.cursor() as cur:
            # Add column if missing
            cur.execute("ALTER TABLE IF EXISTS stores ADD COLUMN IF NOT EXISTS mien VARCHAR(20)")
            # Online stores -> 'Online'
            if online_ids:
                ids_tuple = tuple(online_ids)
                if len(ids_tuple) == 1:
                    cur.execute("UPDATE stores SET mien='Online' WHERE id = %s", (ids_tuple[0],))
                else:
                    cur.execute(f"UPDATE stores SET mien='Online' WHERE id IN %s", (ids_tuple,))
            # Region backfill for others where mien IS NULL
            def update_region(names: set, label: str):
                if not names:
                    return
                vals = tuple(names)
                if len(vals) == 1:
                    cur.execute("UPDATE stores SET mien=%s WHERE mien IS NULL AND (tinh_thanh=%s OR thanh_pho=%s)", (label, vals[0], vals[0]))
                else:
                    cur.execute("UPDATE stores SET mien=%s WHERE mien IS NULL AND (tinh_thanh IN %s OR thanh_pho IN %s)", (label, vals, vals))
            update_region(MIEN_BAC, "Miền Bắc")
            update_region(MIEN_TRUNG, "Miền Trung")
            update_region(MIEN_NAM, "Miền Nam")
            # Default fallback
            cur.execute("UPDATE stores SET mien='Miền Nam' WHERE mien IS NULL")
        conn.commit()
    print("Đã cập nhật cột 'mien' cho bảng stores mà không ảnh hưởng các bảng khác.")
