-- Star schema for Vietnamese Mother & Baby retail dataset
-- Using natural business IDs as primary keys

-- Drop existing tables to allow PK/FK changes safely
DROP TABLE IF EXISTS KPI_Target_Monthly;
DROP TABLE IF EXISTS product_daily_costs;
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
    tinh_thanh VARCHAR(100),
    mien VARCHAR(20)
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

-- Daily product input cost table with smoothness constraints
CREATE TABLE IF NOT EXISTS product_daily_costs (
    product_id VARCHAR(50) NOT NULL REFERENCES products(id) ON DELETE CASCADE,
    date_id INT NOT NULL REFERENCES dates(date_id) ON DELETE CASCADE,
    cost NUMERIC(12,2) NOT NULL,
    CONSTRAINT pk_product_daily_costs PRIMARY KEY (product_id, date_id)
);
CREATE INDEX IF NOT EXISTS idx_pdc_product ON product_daily_costs(product_id);
CREATE INDEX IF NOT EXISTS idx_pdc_date ON product_daily_costs(date_id);

-- Enforce: day-to-day change within 3%, and any 365-day window range within 20%
CREATE OR REPLACE FUNCTION enforce_product_cost_smoothness()
RETURNS trigger AS $$
DECLARE
    v_prev_date_id INT;
    v_next_date_id INT;
    v_prev_cost NUMERIC(12,2);
    v_next_cost NUMERIC(12,2);
    v_cur_date DATE;
    v_window_start DATE;
    v_window_end DATE;
    v_min_cost NUMERIC(12,2);
    v_max_cost NUMERIC(12,2);
BEGIN
    -- Resolve current date
    SELECT full_date INTO v_cur_date FROM dates WHERE date_id = NEW.date_id;
    IF v_cur_date IS NULL THEN
        RAISE EXCEPTION 'date_id % not found in dates', NEW.date_id;
    END IF;

    -- Check previous day (+-3%)
    SELECT d2.date_id INTO v_prev_date_id
    FROM dates d1
    JOIN dates d2 ON d2.full_date = d1.full_date - INTERVAL '1 day'
    WHERE d1.date_id = NEW.date_id;
    IF v_prev_date_id IS NOT NULL THEN
        SELECT cost INTO v_prev_cost FROM product_daily_costs WHERE product_id = NEW.product_id AND date_id = v_prev_date_id;
        IF v_prev_cost IS NOT NULL THEN
            IF NEW.cost > v_prev_cost * 1.03 OR NEW.cost < v_prev_cost * 0.97 THEN
                RAISE EXCEPTION 'Daily cost change exceeds 3%% for product %, date_id % (prev % vs new %)', NEW.product_id, NEW.date_id, v_prev_cost, NEW.cost;
            END IF;
        END IF;
    END IF;

    -- Also check next day if it exists (to cover updates affecting forward neighbor)
    SELECT d2.date_id INTO v_next_date_id
    FROM dates d1
    JOIN dates d2 ON d2.full_date = d1.full_date + INTERVAL '1 day'
    WHERE d1.date_id = NEW.date_id;
    IF v_next_date_id IS NOT NULL THEN
        SELECT cost INTO v_next_cost FROM product_daily_costs WHERE product_id = NEW.product_id AND date_id = v_next_date_id;
        IF v_next_cost IS NOT NULL THEN
            IF v_next_cost > NEW.cost * 1.03 OR v_next_cost < NEW.cost * 0.97 THEN
                RAISE EXCEPTION 'Daily cost change exceeds 3%% for product %, next day % vs new %', NEW.product_id, v_next_cost, NEW.cost;
            END IF;
        END IF;
    END IF;

    -- 365-day rolling window: ensure max/min <= 1.2
    v_window_start := v_cur_date - INTERVAL '365 days';
    v_window_end := v_cur_date; -- backward-looking window
    SELECT MIN(cost), MAX(cost)
      INTO v_min_cost, v_max_cost
      FROM product_daily_costs pdc
      JOIN dates d ON d.date_id = pdc.date_id
     WHERE pdc.product_id = NEW.product_id
       AND d.full_date BETWEEN v_window_start AND v_window_end;
    -- Consider NEW.cost in the window
    IF v_min_cost IS NULL OR NEW.cost < v_min_cost THEN v_min_cost := NEW.cost; END IF;
    IF v_max_cost IS NULL OR NEW.cost > v_max_cost THEN v_max_cost := NEW.cost; END IF;
    IF v_min_cost > 0 AND (v_max_cost / v_min_cost) > 1.2 THEN
        RAISE EXCEPTION 'Cost range exceeds 20%% within 365 days for product % on date_id % (min %, max %)', NEW.product_id, NEW.date_id, v_min_cost, v_max_cost;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_product_cost_smoothness ON product_daily_costs;
CREATE TRIGGER trg_product_cost_smoothness
BEFORE INSERT OR UPDATE ON product_daily_costs
FOR EACH ROW EXECUTE FUNCTION enforce_product_cost_smoothness();
