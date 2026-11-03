# Hướng Dẫn Sử Dụng & Triển Khai Dashboard BI Trên Metabase

Thư mục này chứa mã nguồn tự động hóa việc khởi tạo, cấu hình tham số động và vẽ các Dashboard BI phục vụ Khóa luận tốt nghiệp: **Vietnam Electricity Lakehouse**.

---

## 📁 Cấu Trúc Mã Nguồn

```text
scripts/metabase/
├── common.py                 # Các hàm tiện ích dùng chung (kết nối, token, upsert card, bố cục 2 màn hình)
├── setup_dashboard_01.py     # Script riêng cho Dashboard 01 (Cung - Cầu & Duck Curve)
├── setup_dashboard_02.py     # Script riêng cho Dashboard 02 (Khí Hậu Cực Đoan & Thủy Điện)
├── setup_dashboard_03.py     # Script riêng cho Dashboard 03 (Mất Cân Đối Vùng Miền & Siêu Lưới 500kV)
├── setup_dashboards.py       # Script tổng hợp (Master Runner) cho phép chạy lẻ hoặc chạy tất cả
└── README.md                 # Tài liệu hướng dẫn
```

---

## 🚀 Cách Chạy Từng Dashboard (Dành Cho Thành Viên Nhóm)

Khi bạn hoặc thành viên trong nhóm `git pull` nhánh `feature/Lakehouse` về, các bạn có thể cập nhật lẻ từng Dashboard hoặc chạy toàn bộ bằng 1 dòng lệnh duy nhất.

### 1. Triển khai lẻ từng Dashboard
* **Chỉ cập nhật Dashboard 01** (*Cân Bằng Cung - Cầu & Duck Curve*):
  ```bash
  python scripts/metabase/setup_dashboard_01.py
  ```
* **Chỉ cập nhật Dashboard 02** (*Khí Hậu Cực Đoan & Thủy Điện*):
  ```bash
  python scripts/metabase/setup_dashboard_02.py
  ```
* **Chỉ cập nhật Dashboard 03** (*Mất Cân Đối Vùng Miền & Siêu Lưới 500kV*):
  ```bash
  python scripts/metabase/setup_dashboard_03.py
  ```

### 2. Triển khai qua Script Tổng Hợp (`setup_dashboards.py`)
* **Chạy tất cả 3 Dashboard cùng lúc**:
  ```bash
  python scripts/metabase/setup_dashboards.py --all
  ```
* **Chỉ chạy Dashboard 3 bằng cờ CLI**:
  ```bash
  python scripts/metabase/setup_dashboards.py --dashboard 3
  ```

---

## ⚡ Các Tính Năng Nổi Bật

1. **Tự động nhận diện môi trường (Smart URL Detection)**:
   * Chạy trực tiếp trên máy Host: Tự động trỏ `http://localhost:3000`.
   * Chạy bên trong mạng Docker: Tự động trỏ `http://metabase:3000`.
2. **Cơ chế Idempotent (Không tạo trùng lặp)**:
   * Nếu Dashboard và Card đã có sẵn trên Metabase, script sẽ **tự động cập nhật câu lệnh SQL và giao diện** mà không tạo rác hay nhân bản thẻ.
3. **Chế độ Tự động Lấy Ngày Mới Nhất (Zero-Hardcoding)**:
   * Khi không chọn ngày: Tự động neo theo ngày mới nhất trong Lakehouse (`SELECT MAX(date_key)`).
   * Khi chọn ngày: Tự động lọc chính xác theo ngày lịch sử được chọn.
