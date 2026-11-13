# Metabase BI Specification: Dashboard 1 - Cân Bằng Cung Cầu & Chuyển Dịch Năng Lượng
> **Tên Dashboard**: `01. Cân Bằng Cung - Cầu & Chuyển Dịch Năng Lượng Quốc Gia`  
> **Mục tiêu**: Giám sát sát sao phụ tải 3 miền, cân bằng cung - cầu 24h, hiện tượng sụt giảm phụ tải ròng (Duck Curve) và tỷ trọng chuyển dịch năng lượng tái tạo.  
> **Nguồn dữ liệu (Source View)**: `iceberg.gold.v_dashboard1_supply_demand_hourly`

---

## 1. Bộ Lọc Toàn Trang (Dashboard Filters)
1. **`Date` (Field Filter)**:
   - Target: `full_date` (Date picker, default: Latest date).
2. **`Region` (Dropdown)**:
   - Target: `region_name` (North, Central, South, National).
3. **`Day Type` (Dropdown)**:
   - Target: `is_weekend` (Weekday / Weekend).

---

## 2. Bố Cục Lưới (24-Column Grid Layout)

### NHỊP 1: MÀN HÌNH ĐẦU TIÊN (ABOVE THE FOLD - ZERO SCROLL)

#### Hàng KPI (Row 0 - 3, Height: 4)
* **Card 1.1: Phụ tải Hiện tại / Gần nhất ($P_{\text{current}}$)**
  - Vị trí: `col: 0, row: 0, size_x: 6, size_y: 4`
  - Kiểu hiển thị: **Scalar / Number KPI**
  - SQL:
    ```sql
    SELECT 
        national_load_mw AS current_load_mw
    FROM iceberg.gold.v_dashboard1_supply_demand_hourly
    WHERE {{Date}}
    ORDER BY hour DESC
    LIMIT 1;
    ```
  - Định dạng: Số nguyên, đơn vị `MW`, màu Xanh dương (#1f77b4).

* **Card 1.2: Phụ tải Đỉnh trong ngày ($P_{\text{max}}$)**
  - Vị trí: `col: 6, row: 0, size_x: 6, size_y: 4`
  - Kiểu hiển thị: **Scalar / Number KPI**
  - SQL:
    ```sql
    SELECT 
        MAX(national_load_mw) AS peak_load_mw
    FROM iceberg.gold.v_dashboard1_supply_demand_hourly
    WHERE {{Date}};
    ```
  - Định dạng: Số nguyên, đơn vị `MW`, màu Đỏ (#d62728).

* **Card 1.3: Phụ tải Đáy trong ngày ($P_{\text{min}}$ & $\Delta P$)**
  - Vị trí: `col: 12, row: 0, size_x: 6, size_y: 4`
  - Kiểu hiển thị: **Scalar / Number KPI**
  - SQL:
    ```sql
    SELECT 
        MIN(national_load_mw) AS min_load_mw
    FROM iceberg.gold.v_dashboard1_supply_demand_hourly
    WHERE {{Date}};
    ```
  - Định dạng: Số nguyên, đơn vị `MW`, màu Tím (#9467bd).

* **Card 1.4: Tỷ trọng Năng lượng Tái tạo (RE%)**
  - Vị trí: `col: 18, row: 0, size_x: 6, size_y: 4`
  - Kiểu hiển thị: **Progress Bar / Gauge / Number**
  - SQL:
    ```sql
    SELECT 
        AVG(renewable_share_pct) AS re_share_pct
    FROM iceberg.gold.v_dashboard1_supply_demand_hourly
    WHERE {{Date}};
    ```
  - Định dạng: Phần trăm (%), 1 chữ số thập phân, màu Xanh lá (#2ca02c). Mục tiêu: 25.0%.

---

#### Hàng Hero Charts (Row 4 - 13, Height: 10)
* **Card 1.5 [Hero 1]: Cân Bằng Cung vs Cầu 24 Giờ (24h Supply vs Demand Balance)**
  - Vị trí: `col: 0, row: 4, size_x: 12, size_y: 10`
  - Kiểu hiển thị: **Line Chart (2 Lines)**
  - Trục X: `hour` (0h - 23h)
  - Trục Y1: `national_load_mw` (Đường nét liền Xanh đậm: Nhu cầu phụ tải tiêu thụ)
  - Trục Y2: `avg_hourly_generation_mw` (Đường nét đứt Cam: Nguồn phát đáp ứng)
  - SQL:
    ```sql
    SELECT 
        hour,
        AVG(national_load_mw) AS "Phụ tải Tiêu thụ (MW)",
        AVG(avg_hourly_generation_mw) AS "Nguồn phát Huy động (MW)"
    FROM iceberg.gold.v_dashboard1_supply_demand_hourly
    WHERE {{Date}}
    GROUP BY hour
    ORDER BY hour;
    ```

* **Card 1.6 [Hero 2]: Phân tích Hiện tượng "Đường cong con vịt" (The Duck Curve & Net Load)**
  - Vị trí: `col: 12, row: 4, size_x: 12, size_y: 10`
  - Kiểu hiển thị: **Area / Line Chart**
  - Trục X: `hour` (0h - 23h)
  - Trục Y: `national_load_mw` (Phụ tải thông thường) vs `net_load_mw` (Phụ tải ròng sau khi trừ điện mặt trời)
  - SQL:
    ```sql
    SELECT 
        hour,
        AVG(national_load_mw) AS "Phụ tải Tổng (Gross Load)",
        AVG(net_load_mw) AS "Phụ tải Ròng (Net Load = Gross - Solar - Wind)"
    FROM iceberg.gold.v_dashboard1_supply_demand_hourly
    WHERE {{Date}}
    GROUP BY hour
    ORDER BY hour;
    ```
  - Điểm nhấn: Thể hiện rõ hiện tượng hõm bụng sâu từ 10h–14h khi điện mặt trời áp mái phát mạnh và dốc đứng tăng vọt lúc 18h tối khi mặt trời lặn.

---

### NHỊP 2: CUỘN NHẸ 1 LẦN (BELOW THE FOLD - DRILL-DOWN & CHI TIẾT)

#### Hàng Phân tích Cơ cấu (Row 14 - 23, Height: 10)
* **Card 1.7: Phân Bổ Phụ Tải 3 Miền Bắc - Trung - Nam**
  - Vị trí: `col: 0, row: 14, size_x: 12, size_y: 10`
  - Kiểu hiển thị: **Stacked Area Chart**
  - Trục X: `hour` (0h - 23h)
  - Chuỗi dữ liệu: `Miền Bắc`, `Miền Nam`, `Miền Trung`
  - SQL:
    ```sql
    SELECT 
        hour,
        AVG(north_load_mw) AS "Miền Bắc",
        AVG(south_load_mw) AS "Miền Nam",
        AVG(central_load_mw) AS "Miền Trung"
    FROM iceberg.gold.v_dashboard1_supply_demand_hourly
    WHERE {{Date}}
    GROUP BY hour
    ORDER BY hour;
    ```

* **Card 1.8: Cơ Cấu Nguồn Phát Theo Tháng / Mùa (12 Tháng Hoán Đổi)**
  - Vị trí: `col: 12, row: 14, size_x: 12, size_y: 10`
  - Kiểu hiển thị: **Stacked Bar Chart**
  - Trục X: `month` (Tháng 1 - 12)
  - Chuỗi dữ liệu: `Thủy điện`, `Nhiệt điện than`, `Tua bin khí`, `Điện mặt trời`, `Điện gió`
  - SQL:
    ```sql
    SELECT 
        month AS "Tháng",
        ROUND(AVG(hydro_generation_mwh), 0) AS "Thủy điện (MWh)",
        ROUND(AVG(coal_generation_mwh), 0) AS "Nhiệt điện than (MWh)",
        ROUND(AVG(gas_generation_mwh), 0) AS "Tua bin khí (MWh)",
        ROUND(AVG(solar_generation_mwh), 0) AS "Điện mặt trời (MWh)",
        ROUND(AVG(wind_generation_mwh), 0) AS "Điện gió (MWh)"
    FROM iceberg.gold.v_dashboard1_supply_demand_hourly
    GROUP BY month
    ORDER BY month;
    ```

---

#### Bảng Chi tiết Drill-Down (Row 24 - 31, Height: 8)
* **Card 1.9: Bảng Điều Độ Chi Tiết Nguồn Phát**
  - Vị trí: `col: 0, row: 24, size_x: 24, size_y: 8`
  - Kiểu hiển thị: **Table (với Conditional Formatting)**
  - SQL:
    ```sql
    SELECT 
        s.source_name AS "Loại Hình Nguồn Phát",
        CASE WHEN s.renewable_flag = true THEN '🌱 Năng lượng Tái tạo (Xanh)' ELSE '⚡ Năng lượng Truyền thống (Nâu)' END AS "Phân Loại",
        ROUND(g.generation_mwh, 1) AS "Sản lượng (MWh)",
        ROUND(g.generation_share_pct, 2) AS "Tỷ trọng (%)",
        ROUND(g.generation_growth_pct, 2) AS "Tăng trưởng vs Hôm qua (%)",
        g.source_rank_in_day AS "Thứ hạng Công suất"
    FROM iceberg.gold.fact_generation_daily g
    JOIN iceberg.gold.dim_energy_source s ON g.energy_source_key = s.energy_source_key
    JOIN iceberg.gold.dim_date d ON g.date_key = d.date_key
    WHERE {{Date}}
    ORDER BY g.source_rank_in_day;
    ```
