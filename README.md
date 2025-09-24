# BI Sales Generator (Vietnamese Mother & Baby)

Tạo dữ liệu mẫu (tiếng Việt) cho báo cáo Power BI theo mô hình sao (star schema) và nạp vào Postgres.

## Bảng dữ liệu
- dates (bao phủ >= 3 năm)
- customers (50–100 KH, có point và tier)
  - customer_child (0–5 người sử dụng/khách hàng: ngày sinh, giới tính)
- products (100–200 SP dành cho mẹ và bé)
- employees (20–50 NV)
- stores (5–20 cửa hàng)
- promotions (10–20 CTKM)
- orders (1,000–5,000 đơn hàng)
- order_items (1–5 dòng sản phẩm mỗi đơn; có khuyến mãi/chiết khấu)
- KPI_Target_Monthly (mục tiêu theo tháng x cửa hàng, không giảm theo thời gian)

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
--customers [900-1900]
--products [100-500]
--employees [50-100]
--stores [30-40]
--promotions [50-200]
--years (>=3)
--min-rows (>=500000)
--max-rows (<=600000)
--export-csv <thư_mục>
--monthly-active-min <int>   # số KH hoạt động tối thiểu mỗi tháng (mặc định 700)
--monthly-active-max <int>   # số KH hoạt động tối đa mỗi tháng (mặc định 900)
--monthly-active-customers <int>  # (cũ) cố định một giá trị cho mọi tháng
```

Ví dụ tạo dữ liệu nhỏ để thử nhanh:
```powershell
python .\src\main.py --customers 60 --products 120 --employees 25 --stores 8 --promotions 12 --years 3 --min-rows 1200 --max-rows 2400
```

Ví dụ ràng buộc số khách mua theo tháng dao động 700–900 (mặc định đã dùng khoảng này, có thể điều chỉnh bằng cờ dưới):

```powershell
python .\src\main.py --monthly-active-min 700 --monthly-active-max 900
```

## Ghi chú
- Script sẽ tạo bảng (nếu chưa có) và TRUNCATE trước khi nạp để tránh dữ liệu trùng lặp.
- Nếu database `bi_courses` chưa tồn tại và tài khoản có quyền, script sẽ cố gắng tạo tự động.
- Dữ liệu tên, địa chỉ, nhân viên, khách hàng, cửa hàng là tiếng Việt (sử dụng Faker vi_VN). Tên sản phẩm thuộc ngành hàng mẹ & bé.

---

Nếu bạn muốn xuất ra CSV thay vì nạp DB, có thể dùng flag `--export-csv .\export` (thư mục sẽ được tạo nếu chưa có).

## Khối lượng lớn: 50k orders và 3k–5k khách hàng

Bạn có thể tạo tập dữ liệu lớn hơn để luyện tập với Power BI và hiệu năng Postgres.

- Tạo 50.000 bản ghi orders với kích thước dimension cao hơn:

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

Lưu ý: Script đã tối ưu chèn dữ liệu orders theo batch (bulk insert). Thời gian chạy phụ thuộc cấu hình máy và Postgres.

## KPI theo tháng

Bảng `KPI_Target_Monthly` được sinh ra tự động từ dữ liệu thực tế theo nguyên tắc mục tiêu không giảm theo tháng cho mỗi cửa hàng. Bạn có thể dùng bảng này để vẽ KPI trong Power BI.