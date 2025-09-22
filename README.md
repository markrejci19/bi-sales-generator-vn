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
