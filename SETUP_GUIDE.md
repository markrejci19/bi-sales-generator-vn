# PostgreSQL 16 Setup và Chạy thử Project

## Tóm tắt
Dự án đã được thiết lập thành công với PostgreSQL 16 chạy trên Docker và đã chèn dữ liệu thành công.

## Các bước thực hiện

### 1. Cài đặt PostgreSQL 16 từ Docker
```bash
docker run --name postgres16-bi -e POSTGRES_PASSWORD=1 -e POSTGRES_DB=bi_courses -p 5432:5432 -d postgres:16
```

### 2. Thiết lập môi trường Python
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

### 3. Tạo file cấu hình
Tạo file `.env`:
```
PG_HOST=localhost
PG_PORT=5432
PG_DB=bi_courses
PG_USER=postgres
PG_PASSWORD=1
CUSTOMERS=100
PRODUCTS=180
EMPLOYEES=40
STORES=10
PROMOTIONS=15
YEARS=3
MIN_ROWS=1000
MAX_ROWS=5000
```

### 4. Chạy script tạo dữ liệu
```bash
python src/main.py --customers 100 --products 180 --employees 40 --stores 10 --promotions 15 --years 3 --min-rows 1000 --max-rows 5000
```

## Kết quả

### Thông tin Container
- **Container Name**: postgres16-bi
- **PostgreSQL Version**: 16.10
- **Port**: 5432
- **Database**: bi_courses
- **Username**: postgres
- **Password**: 1

### Dữ liệu đã chèn
- **Customer_Dim**: 100 khách hàng
- **Date_Dim**: 1,097 ngày (3+ năm)
- **Employee_Dim**: 40 nhân viên
- **Product_Dim**: 180 sản phẩm
- **Store_Dim**: 10 cửa hàng
- **Promotion_Dim**: 15 chương trình khuyến mãi
- **Sales_Fact**: 4,365 giao dịch
- **KPI_Summary**: 3,574 bản tóm tắt

### Doanh thu theo năm
- 2022: ₫1.4 tỷ (383 giao dịch)
- 2023: ₫5.1 tỷ (1,443 giao dịch)
- 2024: ₫5.6 tỷ (1,490 giao dịch)
- 2025: ₫3.8 tỷ (1,049 giao dịch)

**Tổng doanh thu**: ₫15.9 tỷ

## Kiểm tra dữ liệu
```bash
# Kiểm tra kết nối
docker exec postgres16-bi psql -U postgres -d bi_courses -c "SELECT version();"

# Kiểm tra số lượng bản ghi
docker exec postgres16-bi psql -U postgres -d bi_courses -c "
SELECT 'Sales_Fact' as table_name, COUNT(*) as row_count FROM Sales_Fact
UNION ALL
SELECT 'Customer_Dim', COUNT(*) FROM Customer_Dim
ORDER BY table_name;"
```

## Lưu ý
- Container sẽ tự động khởi động lại nếu restart Docker
- Dữ liệu sẽ được lưu trữ trong container (không persistent nếu xóa container)
- Để dữ liệu persistent, sử dụng volume mapping: `-v postgres_data:/var/lib/postgresql/data`