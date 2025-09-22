-- Star schema for Vietnamese Mother & Baby retail dataset
CREATE TABLE IF NOT EXISTS Date_Dim (
    date_key INT PRIMARY KEY, -- yyyymmdd
    full_date DATE NOT NULL,
    day INT NOT NULL,
    week INT NOT NULL,
    month INT NOT NULL,
    month_name_vi VARCHAR(20) NOT NULL,
    quarter INT NOT NULL,
    year INT NOT NULL,
    is_weekend BOOLEAN NOT NULL
);

CREATE TABLE IF NOT EXISTS Customer_Dim (
    customer_key SERIAL PRIMARY KEY,
    customer_id VARCHAR(50) UNIQUE NOT NULL,
    ho_ten VARCHAR(100) NOT NULL,
    gioi_tinh VARCHAR(10) NOT NULL,
    ngay_sinh DATE,
    so_dien_thoai VARCHAR(20),
    email VARCHAR(120),
    dia_chi VARCHAR(255),
    thanh_pho VARCHAR(100),
    tinh_thanh VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS Product_Dim (
    product_key SERIAL PRIMARY KEY,
    product_id VARCHAR(50) UNIQUE NOT NULL,
    ten_san_pham VARCHAR(200) NOT NULL,
    danh_muc VARCHAR(100) NOT NULL,
    thuong_hieu VARCHAR(100),
    don_vi VARCHAR(30) NOT NULL,
    gia_niem_yet NUMERIC(12,2) NOT NULL
);

CREATE TABLE IF NOT EXISTS Employee_Dim (
    employee_key SERIAL PRIMARY KEY,
    employee_id VARCHAR(50) UNIQUE NOT NULL,
    ho_ten VARCHAR(100) NOT NULL,
    chuc_danh VARCHAR(100),
    cua_hang_mac_dinh VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS Store_Dim (
    store_key SERIAL PRIMARY KEY,
    store_id VARCHAR(50) UNIQUE NOT NULL,
    ten_cua_hang VARCHAR(120) NOT NULL,
    dia_chi VARCHAR(255),
    thanh_pho VARCHAR(100),
    tinh_thanh VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS Promotion_Dim (
    promotion_key SERIAL PRIMARY KEY,
    promotion_id VARCHAR(50) UNIQUE NOT NULL,
    ten_chuong_trinh VARCHAR(200) NOT NULL,
    loai VARCHAR(50) NOT NULL, -- Percent, Amount, Bundle
    gia_tri NUMERIC(10,2) NOT NULL,
    start_date DATE NOT NULL,
    end_date DATE NOT NULL
);

CREATE TABLE IF NOT EXISTS Sales_Fact (
    sales_key BIGSERIAL PRIMARY KEY,
    date_key INT NOT NULL REFERENCES Date_Dim(date_key),
    customer_key INT REFERENCES Customer_Dim(customer_key),
    product_key INT NOT NULL REFERENCES Product_Dim(product_key),
    employee_key INT REFERENCES Employee_Dim(employee_key),
    store_key INT REFERENCES Store_Dim(store_key),
    promotion_key INT REFERENCES Promotion_Dim(promotion_key),
    so_luong INT NOT NULL,
    don_gia NUMERIC(12,2) NOT NULL,
    chiet_khau NUMERIC(12,2) NOT NULL DEFAULT 0,
    doanh_thu NUMERIC(14,2) NOT NULL
);

-- KPI summary per day and store
CREATE TABLE IF NOT EXISTS KPI_Summary (
    kpi_id BIGSERIAL PRIMARY KEY,
    date_key INT NOT NULL REFERENCES Date_Dim(date_key),
    store_key INT REFERENCES Store_Dim(store_key),
    line_count BIGINT NOT NULL,
    total_quantity BIGINT NOT NULL,
    total_discount NUMERIC(14,2) NOT NULL,
    total_revenue NUMERIC(16,2) NOT NULL,
    avg_ticket NUMERIC(14,2) NOT NULL
);
