# Metabase BI Specification: Dashboard 2 - Khí Hậu Cực Đoan, An Ninh Nước & Thủy Điện
> **Tên Dashboard**: `02. Khí Hậu Cực Đoan, An Ninh Nguồn Nước & Thủy Điện`  
> **Mục tiêu**: Theo dõi tương quan nhiệt độ cực đoan - phụ tải máy lạnh, cảnh báo an toàn đập hồ chứa và tương quan bức xạ - điện mặt trời.  
> **Nguồn dữ liệu (Source View)**: `iceberg.gold.v_dashboard2_climate_hydro_daily`

---

## 1. Bộ Lọc Toàn Trang (Dashboard Filters)
1. **`Date` (Field Filter)**:
   - Target: `full_date` (Date picker, default: Latest date).
2. **`Lưu Vực Sông` (Dropdown Filter)**:
   - Target: `river_basin` (Sông Đà, Sông Lô - Gâm, Sông Mã - Cả, Sông Đồng Nai, Sông Sê San...).
3. **`Hồ Thủy Điện` (Dropdown Filter Động)**:
   - Target: `plant_name` (Hòa Bình, Sơn La, Lai Châu, Tuyên Quang, Trị An, Ialy...). Default: `Hòa Bình`.

---

## 2. Bố Cục Lưới (24-Column Grid Layout)

### NHỊP 1: MÀN HÌNH ĐẦU TIÊN (ABOVE THE FOLD - ZERO SCROLL)

#### Hàng KPI (Row 0 - 3, Height: 4)
* **Card 2.1: Nhiệt độ Đỉnh Vùng ($T_{\text{max}}$)**
  - Vị trí: `col: 0, row: 0, size_x: 6, size_y: 4`
  - Kiểu hiển thị: **Scalar / Number KPI**
  - SQL:
    ```sql
    SELECT 
        MAX(national_max_temp_c) AS max_temperature_c
    FROM iceberg.gold.v_dashboard2_climate_hydro_daily
    WHERE {{Date}};
    ```
  - Định dạng: Số thập phân 1 chữ số, đơn vị `°C`, màu Đỏ cam (#ff7f0e). Phụ đề: "Nhiệt độ cao nhất ghi nhận".

* **Card 2.2: Tổng Lưu lượng Nước về Hồ Toàn Hệ thống ($Q_{\text{về}}$)**
  - Vị trí: `col: 6, row: 0, size_x: 6, size_y: 4`
  - Kiểu hiển thị: **Scalar / Number KPI**
  - SQL:
    ```sql
    SELECT 
        ROUND(SUM(reservoir_inflow_m3_per_s), 0) AS total_inflow_m3_s
    FROM iceberg.gold.v_dashboard2_climate_hydro_daily
    WHERE {{Date}};
    ```
  - Định dạng: Số nguyên, đơn vị `m³/s`, màu Xanh lam (#1f77b4).

* **Card 2.3: Tổng Lưu lượng Xả qua Phát điện & Cấp nước ($Q_{\text{xả}}$)**
  - Vị trí: `col: 12, row: 0, size_x: 6, size_y: 4`
  - Kiểu hiển thị: **Scalar / Number KPI**
  - SQL:
    ```sql
    SELECT 
        ROUND(SUM(total_discharge_m3_per_s), 0) AS total_discharge_m3_s
    FROM iceberg.gold.v_dashboard2_climate_hydro_daily
    WHERE {{Date}};
    ```
  - Định dạng: Số nguyên, đơn vị `m³/s`, màu Xanh mòng két (#17becf).

* **Card 2.4: Đèn Tín Hiệu Cảnh Báo An Toàn Xả Lũ**
  - Vị trí: `col: 18, row: 0, size_x: 6, size_y: 4`
  - Kiểu hiển thị: **Number / Text Status**
  - SQL:
    ```sql
    SELECT 
        CASE 
            WHEN SUM(total_gates_open) > 0 THEN '⚠️ ĐANG XẢ LŨ (' || CAST(SUM(total_gates_open) AS VARCHAR) || ' cửa mở)'
            ELSE '✅ AN TOÀN (0 cửa xả mở)'
        END AS alert_status
    FROM iceberg.gold.v_dashboard2_climate_hydro_daily
    WHERE {{Date}};
    ```
  - Định dạng: Đỏ cảnh báo nếu > 0 cửa mở; Xanh lá nếu 0 cửa mở.

---

#### Hàng Hero Charts (Row 4 - 13, Height: 10)
* **Card 2.5 [Hero 1]: Đường Cong Độ Nhạy Nhiệt Độ - Phụ Tải (Temperature Sensitivity Curve)**
  - Vị trí: `col: 0, row: 4, size_x: 12, size_y: 10`
  - Kiểu hiển thị: **Scatter Plot with Polynomial Trendline**
  - Trục X: `national_avg_temp_c` (Nhiệt độ °C)
  - Trục Y: `peak_load_mw` (Phụ tải đỉnh MW)
  - SQL:
    ```sql
    SELECT 
        national_avg_temp_c AS "Nhiệt độ Trung bình (°C)",
        peak_load_mw AS "Phụ tải Đỉnh (MW)"
    FROM iceberg.gold.v_dashboard2_climate_hydro_daily
    WHERE national_avg_temp_c IS NOT NULL AND peak_load_mw IS NOT NULL
    GROUP BY date_key, national_avg_temp_c, peak_load_mw
    ORDER BY national_avg_temp_c;
    ```
  - Điểm nhấn: Nhìn rõ điểm uốn bùng nổ phụ tải máy lạnh dân dụng và thương mại khi nhiệt độ vượt ngưỡng $28^\circ\text{C}$ lên $38^\circ\text{C}$.

* **Card 2.6 [Hero 2]: Đồng Hồ Mực Nước An Toàn Động (Dynamic Water Level Gauge)**
  - Vị trí: `col: 12, row: 4, size_x: 12, size_y: 10`
  - Kiểu hiển thị: **Gauge Chart** (Hoặc Progress Meter có vạch cảnh báo)
  - Mục tiêu: Tự động nhảy theo Hồ được chọn ở bộ lọc dropdown `{{Hồ Thủy Điện}}`.
  - Giá trị kim đo: `upstream_water_level_m`
  - Cận dưới: `dead_water_level_m` (Mực nước chết)
  - Cận trên: `normal_water_level_m` (Mực nước dâng bình thường)
  - SQL:
    ```sql
    SELECT 
        plant_name AS "Nhà máy",
        upstream_water_level_m AS "Mực nước hiện tại (m)",
        dead_water_level_m AS "Mực nước chết (m)",
        normal_water_level_m AS "Mực nước dâng BT (m)",
        useful_capacity_pct AS "Dung tích hữu dụng (%)",
        reservoir_alert_status AS "Trạng thái"
    FROM iceberg.gold.v_dashboard2_climate_hydro_daily
    WHERE {{Date}} AND {{Hồ Thủy Điện}}
    LIMIT 1;
    ```
  - Điểm nhấn: Tiết kiệm không gian triệt để. Người dùng chọn hồ nào thì xem trọn vẹn tình trạng an toàn của hồ đó mà không bị choáng ngợp bởi 8 đồng hồ cùng lúc.

---

### NHỊP 2: CUỘN NHẸ 1 LẦN (BELOW THE FOLD - DRILL-DOWN & CHI TIẾT)

#### Hàng Phân tích Chuyên sâu (Row 14 - 23, Height: 10)
* **Card 2.7: Cân Bằng Nước Hồ Chứa Đang Chọn ($Q_{\text{về}}$ vs $Q_{\text{xả}}$ theo Thời Gian)**
  - Vị trí: `col: 0, row: 14, size_x: 12, size_y: 10`
  - Kiểu hiển thị: **Line Chart (2 Lines)**
  - Trục X: `full_date` (Theo chuỗi 30 ngày gần nhất)
  - Đường 1: `reservoir_inflow_m3_per_s` (Lưu lượng nước về hồ $m^3/s$ - Xanh đậm)
  - Đường 2: `total_discharge_m3_per_s` (Lưu lượng xả qua nhà máy & xả tràn $m^3/s$ - Cam)
  - SQL:
    ```sql
    SELECT 
        full_date AS "Ngày",
        reservoir_inflow_m3_per_s AS "Lưu lượng về Q_về (m³/s)",
        total_discharge_m3_per_s AS "Tổng lưu lượng xả Q_xả (m³/s)"
    FROM iceberg.gold.v_dashboard2_climate_hydro_daily
    WHERE {{Hồ Thủy Điện}}
    ORDER BY full_date DESC
    LIMIT 30;
    ```

* **Card 2.8: Tương Quan Bức Xạ Mặt Trời vs Sản Lượng Điện Mặt Trời Phát Lưới**
  - Vị trí: `col: 12, row: 14, size_x: 12, size_y: 10`
  - Kiểu hiển thị: **Dual-Axis Chart (Cột & Đường)**
  - Trục X: `full_date`
  - Cột: `national_solar_radiation_w_m2` (Cường độ bức xạ $W/m^2$)
  - Đường: `solar_generation_mwh` (Sản lượng điện mặt trời MWh)
  - SQL:
    ```sql
    SELECT 
        full_date AS "Ngày",
        national_solar_radiation_w_m2 AS "Bức xạ Mặt trời (W/m²)",
        solar_generation_mwh AS "Sản lượng Điện mặt trời (MWh)"
    FROM iceberg.gold.v_dashboard2_climate_hydro_daily
    WHERE national_solar_radiation_w_m2 IS NOT NULL
    GROUP BY full_date, national_solar_radiation_w_m2, solar_generation_mwh
    ORDER BY full_date DESC
    LIMIT 30;
    ```

---

#### Bảng Chi tiết Drill-Down (Row 24 - 31, Height: 8)
* **Card 2.9: Bảng Giám Sát An Toàn Toàn Bộ Các Đập Thủy Điện (Dam Safety Matrix)**
  - Vị trí: `col: 0, row: 24, size_x: 24, size_y: 8`
  - Kiểu hiển thị: **Table (Interactive Drill-Down Matrix)**
  - SQL:
    ```sql
    SELECT 
        plant_name AS "Tên Hồ Thủy Điện",
        river_basin AS "Lưu Vực Sông",
        upstream_water_level_m AS "Mực nước Thượng lưu (m)",
        dead_water_level_m AS "Mực nước Chết (m)",
        normal_water_level_m AS "Mực nước Dâng BT (m)",
        useful_capacity_pct AS "Dung tích Hữu dụng (%)",
        reservoir_inflow_m3_per_s AS "Lưu lượng Về (m³/s)",
        total_discharge_m3_per_s AS "Lưu lượng Xả (m³/s)",
        total_gates_open AS "Số Cửa Xả Mở",
        reservoir_alert_status AS "Tình Trạng An Toàn"
    FROM iceberg.gold.v_dashboard2_climate_hydro_daily
    WHERE {{Date}}
    ORDER BY 
        total_gates_open DESC,
        useful_capacity_pct DESC;
    ```
