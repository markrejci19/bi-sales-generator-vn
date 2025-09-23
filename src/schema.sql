-- Star schema for Vietnamese Mother & Baby retail dataset
-- Using natural business IDs as primary keys

-- Drop existing tables to allow PK/FK changes safely
DROP TABLE IF EXISTS KPI_Target_Monthly;
DROP TABLE IF EXISTS order_items;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS customer_child;
DROP TABLE IF EXISTS promotions;
DROP TABLE IF EXISTS employees;
DROP TABLE IF EXISTS stores;
DROP TABLE IF EXISTS products;
DROP TABLE IF EXISTS customers;
DROP TABLE IF EXISTS dates;

CREATE TABLE IF NOT EXISTS dates (
    date_id INT PRIMARY KEY, -- yyyymmdd
    full_date DATE NOT NULL,
    day INT NOT NULL,
    week INT NOT NULL,
    month INT NOT NULL,
    month_name_vi VARCHAR(20) NOT NULL,
    quarter INT NOT NULL,
    year INT NOT NULL,
    is_weekend BOOLEAN NOT NULL
);

CREATE TABLE IF NOT EXISTS customers (
    id VARCHAR(50) PRIMARY KEY,
    ho_ten VARCHAR(100) NOT NULL,
    gioi_tinh VARCHAR(10) NOT NULL,
    ngay_sinh DATE,
    so_dien_thoai VARCHAR(20),
    email VARCHAR(120),
    dia_chi VARCHAR(255),
    thanh_pho VARCHAR(100),
    tinh_thanh VARCHAR(100),
    point INT NOT NULL DEFAULT 0,
    tier VARCHAR(20) NOT NULL DEFAULT 'Bronze'
);

CREATE TABLE IF NOT EXISTS customer_child (
    id SERIAL PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL REFERENCES customers(id) ON DELETE CASCADE,
    ho_ten VARCHAR(100) NOT NULL,
    gioi_tinh VARCHAR(10) NOT NULL,
    ngay_sinh DATE NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_customer_child_customer ON customer_child(customer_id);

CREATE TABLE IF NOT EXISTS products (
    id VARCHAR(50) PRIMARY KEY,
    ten_san_pham VARCHAR(200) NOT NULL,
    danh_muc VARCHAR(100) NOT NULL,
    thuong_hieu VARCHAR(100),
    don_vi VARCHAR(30) NOT NULL,
    gia_niem_yet NUMERIC(12,2) NOT NULL
);

CREATE TABLE IF NOT EXISTS employees (
    id VARCHAR(50) PRIMARY KEY,
    ho_ten VARCHAR(100) NOT NULL,
    chuc_danh VARCHAR(100),
    cua_hang_mac_dinh VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS stores (
    id VARCHAR(50) PRIMARY KEY,
    ten_cua_hang VARCHAR(120) NOT NULL,
    dia_chi VARCHAR(255),
    thanh_pho VARCHAR(100),
    tinh_thanh VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS promotions (
    id VARCHAR(50) PRIMARY KEY,
    ten_chuong_trinh VARCHAR(200) NOT NULL,
    loai VARCHAR(50) NOT NULL, -- Percent, Amount, Bundle
    gia_tri NUMERIC(10,2) NOT NULL,
    start_date DATE NOT NULL,
    end_date DATE NOT NULL
);

CREATE TABLE IF NOT EXISTS orders (
    order_id BIGSERIAL PRIMARY KEY,
    date_id INT NOT NULL REFERENCES dates(date_id),
    customer_id VARCHAR(50) REFERENCES customers(id),
    employee_id VARCHAR(50) REFERENCES employees(id),
    store_id VARCHAR(50) REFERENCES stores(id),
    channel VARCHAR(10) NOT NULL DEFAULT 'Offline' -- Online | Offline
);

-- Order items: each order has 1-5 products (one line per product)
CREATE TABLE IF NOT EXISTS order_items (
    order_id BIGINT NOT NULL REFERENCES orders(order_id) ON DELETE CASCADE,
    product_id VARCHAR(50) NOT NULL REFERENCES products(id),
    promotion_id VARCHAR(50) REFERENCES promotions(id),
    so_luong INT NOT NULL,
    don_gia NUMERIC(12,2) NOT NULL, -- unit price
    khuyen_mai NUMERIC(12,2) NOT NULL DEFAULT 0, -- per-unit promotion amount
    chiet_khau NUMERIC(12,2) NOT NULL DEFAULT 0, -- per-unit discount amount
    doanh_thu NUMERIC(14,2) NOT NULL, -- line revenue = (unit price - promo - discount) * qty
    id VARCHAR(120) GENERATED ALWAYS AS (order_id::text || '-' || product_id) STORED,
    CONSTRAINT pk_order_items PRIMARY KEY (id),
    CONSTRAINT ck_item_discount CHECK ((khuyen_mai + chiet_khau) < don_gia)
);
CREATE INDEX IF NOT EXISTS idx_order_items_order ON order_items(order_id);

-- Enforce: (khuyen_mai + chiet_khau) < products.gia_niem_yet using a trigger (cross-table CHECK not allowed)
CREATE OR REPLACE FUNCTION enforce_item_discount_vs_list_price()
RETURNS trigger AS $$
DECLARE
    v_list_price NUMERIC(12,2);
BEGIN
    SELECT gia_niem_yet INTO v_list_price FROM products WHERE id = NEW.product_id;
    IF v_list_price IS NULL THEN
        RAISE EXCEPTION 'Product % not found or has no gia_niem_yet', NEW.product_id;
    END IF;
    IF (COALESCE(NEW.khuyen_mai, 0) + COALESCE(NEW.chiet_khau, 0)) >= v_list_price THEN
        RAISE EXCEPTION 'khuyen_mai + chiet_khau (%.2f) must be less than gia_niem_yet (%.2f) for product %',
            (COALESCE(NEW.khuyen_mai, 0) + COALESCE(NEW.chiet_khau, 0)), v_list_price, NEW.product_id;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_order_items_discount_vs_list ON order_items;
CREATE TRIGGER trg_order_items_discount_vs_list
BEFORE INSERT OR UPDATE ON order_items
FOR EACH ROW EXECUTE FUNCTION enforce_item_discount_vs_list_price();

-- Monthly KPI targets per store (non-decreasing month over month)
CREATE TABLE IF NOT EXISTS KPI_Target_Monthly (
    kpi_target_id BIGSERIAL PRIMARY KEY,
    store_id VARCHAR(50) NOT NULL REFERENCES stores(id),
    year_month INT NOT NULL, -- yyyymm
    doanh_thu NUMERIC(16,2) NOT NULL,
    so_luong_don_hang BIGINT NOT NULL,
    so_luong_san_pham BIGINT NOT NULL,
    CONSTRAINT uk_store_month UNIQUE(store_id, year_month)
);
CREATE INDEX IF NOT EXISTS idx_kpi_target_month ON KPI_Target_Monthly(year_month);
