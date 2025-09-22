# BI Sales Generator (Vietnamese Mother & Baby)

Tạo dữ liệu mẫu (tiếng Việt) cho báo cáo Power BI theo mô hình sao (star schema) và nạp vào Postgres.

## Bảng dữ liệu
- Date_Dim (bao phủ >= 3 năm)
- Customer_Dim (50–100 KH)
- Product_Dim (100–200 SP dành cho mẹ và bé)
- Employee_Dim (20–50 NV)
- Store_Dim (5–20 cửa hàng)
- Promotion_Dim (10–20 CTKM)
- Sales_Fact (1,000–5,000 giao dịch)

## Yêu cầu hệ thống
- Python 3.9+
- Postgres đang chạy trên localhost (database: `bi_courses`, user: `postgres`, pass: `1`)

## Cài đặt nhanh (Windows PowerShell)
```powershell
# 1) Tạo virtualenv (tuỳ chọn nhưng khuyến nghị)
python -m venv .venv
.\.venv\Scripts\Activate.ps1

# 2) Cài dependencies
python -m pip install --upgrade pip
pip install -r requirements.txt

# 3) (Tuỳ chọn) Tạo file .env từ mẫu
Copy-Item .env.example .env -Force
# Sửa .env nếu cần

# 4) Tạo dữ liệu và nạp vào Postgres
python .\src\main.py --min-rows 1000 --max-rows 5000 \
  --customers 100 --products 180 --employees 40 --stores 10 --promotions 15 --years 3
```

Mặc định script sẽ dùng kết nối: host=localhost, db=bi_courses, user=postgres, password=1, port=5432. Bạn có thể override bằng biến môi trường hoặc tham số CLI.

## Tuỳ chọn tham số
```
--customers [50-100]
--products [100-200]
--employees [20-50]
--stores [5-20]
--promotions [10-20]
--years (>=3)
--min-rows (>=1000)
--max-rows (<=5000)
--export-csv <thư_mục>
```

Ví dụ tạo dữ liệu nhỏ để thử nhanh:
```powershell
python .\src\main.py --customers 60 --products 120 --employees 25 --stores 8 --promotions 12 --years 3 --min-rows 1200 --max-rows 2400
```

## Ghi chú
- Script sẽ tạo bảng (nếu chưa có) và TRUNCATE trước khi nạp để tránh dữ liệu trùng lặp.
- Nếu database `bi_courses` chưa tồn tại và tài khoản có quyền, script sẽ cố gắng tạo tự động.
- Dữ liệu tên, địa chỉ, nhân viên, khách hàng, cửa hàng là tiếng Việt (sử dụng Faker vi_VN). Tên sản phẩm thuộc ngành hàng mẹ & bé.

---

Nếu bạn muốn xuất ra CSV thay vì nạp DB, có thể dùng flag `--export-csv .\export` (thư mục sẽ được tạo nếu chưa có).

## Khối lượng lớn: 50k Sales_Fact và 3k–5k khách hàng

Bạn có thể tạo tập dữ liệu lớn hơn để luyện tập với Power BI và hiệu năng Postgres.

- Tạo 50.000 bản ghi Sales_Fact với kích thước dimension cao hơn:

```powershell
python .\src\main.py `
  --customers 100 `
  --products 180 `
  --employees 40 `
  --stores 10 `
  --promotions 15 `
  --years 3 `
  --min-rows 50000 `
  --max-rows 50000
```

- Tăng số lượng khách hàng lên 3.000–5.000 (ví dụ 5.000):

```powershell
python .\src\main.py `
  --customers 5000 `
  --products 180 `
  --employees 40 `
  --stores 10 `
  --promotions 15 `
  --years 3 `
  --min-rows 50000 `
  --max-rows 50000
```

Lưu ý: Script đã tối ưu chèn dữ liệu Sales_Fact theo batch (bulk insert). Thời gian chạy phụ thuộc cấu hình máy và Postgres.

## Bảng KPI_Summary (tuỳ chọn cho báo cáo)

Bạn có thể thêm bảng tổng hợp KPI theo tháng để thuận tiện khi dựng báo cáo.

### Tạo bảng KPI

```sql
CREATE TABLE IF NOT EXISTS KPI_Summary (
    kpi_id BIGSERIAL PRIMARY KEY,
    month_key INT NOT NULL,         -- yyyymm
    year INT NOT NULL,
    month INT NOT NULL,
    store_key INT,
    -- Có thể mở rộng: product_key, employee_key, promotion_key
    total_revenue NUMERIC(14,2) NOT NULL,
    total_quantity INT NOT NULL,
    order_count BIGINT NOT NULL,
    unique_customers INT NOT NULL,
    total_discount NUMERIC(14,2) NOT NULL,
    avg_order_value NUMERIC(14,2) NOT NULL,
    CONSTRAINT fk_kpi_store FOREIGN KEY (store_key) REFERENCES Store_Dim(store_key)
);

CREATE INDEX IF NOT EXISTS idx_kpi_month ON KPI_Summary(month_key);
CREATE INDEX IF NOT EXISTS idx_kpi_month_store ON KPI_Summary(month_key, store_key);
```

### Đổ dữ liệu KPI theo tháng x cửa hàng

```sql
-- Làm sạch trước khi tổng hợp lại (tuỳ chọn)
TRUNCATE TABLE KPI_Summary RESTART IDENTITY;

INSERT INTO KPI_Summary (
    month_key, year, month, store_key,
    total_revenue, total_quantity, order_count, unique_customers, total_discount, avg_order_value
)
SELECT
    (dd.year * 100 + dd.month) AS month_key,
    dd.year,
    dd.month,
    sf.store_key,
    SUM(sf.doanh_thu) AS total_revenue,
    SUM(sf.so_luong) AS total_quantity,
    COUNT(*) AS order_count,
    COUNT(DISTINCT sf.customer_key) AS unique_customers,
    SUM(sf.chiet_khau) AS total_discount,
    CASE WHEN COUNT(*) > 0 THEN ROUND(SUM(sf.doanh_thu)::numeric / COUNT(*), 2) ELSE 0 END AS avg_order_value
FROM Sales_Fact sf
JOIN Date_Dim dd ON dd.date_key = sf.date_key
GROUP BY dd.year, dd.month, month_key, sf.store_key
ORDER BY dd.year, dd.month, sf.store_key;
```

### Mở rộng KPI theo tháng x sản phẩm (tuỳ chọn)

```sql
-- Ví dụ truy vấn tham khảo (không đổ vào KPI_Summary):
SELECT
  (dd.year*100 + dd.month) AS month_key,
  dd.year, dd.month,
  sf.product_key,
  SUM(sf.doanh_thu) AS revenue,
  SUM(sf.so_luong) AS quantity
FROM Sales_Fact sf
JOIN Date_Dim dd ON dd.date_key = sf.date_key
GROUP BY dd.year, dd.month, month_key, sf.product_key;
```

Sau khi nạp lại dữ liệu fact, bạn có thể chạy lại phần tổng hợp KPI để cập nhật.
