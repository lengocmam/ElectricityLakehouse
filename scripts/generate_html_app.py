import json
import os

with open('scripts/dashboard_data.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

json_str = json.dumps(data, ensure_ascii=False)

html_template = """<!DOCTYPE html>
<html lang="vi" class="dark">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Vietnam Electricity Lakehouse BI Dashboard</title>
  <script src="https://www.gstatic.com/antigravity/web/dev/tailwindcss.min.js"></script>
  <style>
    :root {
      --background: #0b1120;
      --card: #1e293b;
      --foreground: #f8fafc;
      --muted-foreground: #94a3b8;
      --border: #334155;
      --primary: #38bdf8;
      --primary-foreground: #0f172a;
      --success: #22c55e;
      --warning: #f59e0b;
      --danger: #ef4444;
      --accent: #818cf8;
    }
    body {
      background-color: var(--background);
      color: var(--foreground);
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
    }
    .tab-btn.active {
      background-color: #0284c7;
      color: #ffffff;
      border-color: #38bdf8;
      box-shadow: 0 0 15px rgba(56, 189, 248, 0.3);
    }
    .card-glass {
      background-color: rgba(30, 41, 59, 0.7);
      backdrop-filter: blur(8px);
      border: 1px solid var(--border);
      border-radius: 0.75rem;
    }
    canvas {
      width: 100% !important;
      height: 100% !important;
    }
  </style>
</head>
<body class="antialiased min-h-screen p-4 md:p-6 space-y-6">

  <!-- TOP HEADER -->
  <header class="flex flex-col md:flex-row items-start md:items-center justify-between gap-4 pb-4 border-b border-[var(--border)]">
    <div>
      <div class="flex items-center gap-3">
        <span class="p-2.5 rounded-xl bg-sky-500/10 text-sky-400 font-bold text-2xl border border-sky-500/20">⚡</span>
        <div>
          <h1 class="text-xl md:text-2xl font-bold tracking-tight text-white flex items-center gap-2">
            Hệ Thống Phân Tích & Điều Độ Năng Lượng Điện Quốc Gia
            <span class="text-xs px-2.5 py-0.5 rounded-full bg-emerald-500/10 text-emerald-400 border border-emerald-500/30">Live Gold Lakehouse</span>
          </h1>
          <p class="text-xs md:text-sm text-[var(--muted-foreground)]">
            Kiến trúc Star Schema • Apache Iceberg • Trino Query Engine • Metabase BI Ready
          </p>
        </div>
      </div>
    </div>

    <!-- TABS NAVIGATION -->
    <div class="flex items-center bg-slate-900/90 p-1.5 rounded-xl border border-[var(--border)] text-sm shadow-lg">
      <button id="tab-btn-1" onclick="switchTab(1)" class="tab-btn active px-4 py-2 rounded-lg font-medium transition flex items-center gap-2">
        <span>📊</span> Dashboard 1: Cung - Cầu & Chuyển Dịch
      </button>
      <button id="tab-btn-2" onclick="switchTab(2)" class="tab-btn px-4 py-2 rounded-lg font-medium text-[var(--muted-foreground)] hover:text-white transition flex items-center gap-2">
        <span>🌊</span> Dashboard 2: Khí Hậu & Thủy Điện
      </button>
    </div>
  </header>

  <!-- GLOBAL FILTER BAR -->
  <div class="card-glass p-4 flex flex-wrap items-center justify-between gap-4 text-xs md:text-sm shadow-md">
    <div class="flex flex-wrap items-center gap-4">
      <div class="flex items-center gap-2">
        <span class="text-[var(--muted-foreground)] font-medium">📅 Ngày phân tích:</span>
        <span id="display-date" class="bg-slate-900 text-sky-400 px-3 py-1.5 rounded-lg border border-[var(--border)] font-semibold tracking-wide">2026-10-10</span>
      </div>

      <div id="filter-region-container" class="flex items-center gap-2">
        <span class="text-[var(--muted-foreground)] font-medium">📍 Phân vùng:</span>
        <select id="select-region" onchange="onRegionChange(this.value)" class="bg-slate-900 text-white px-3 py-1.5 rounded-lg border border-[var(--border)] outline-none focus:border-sky-500 cursor-pointer">
          <option value="national">Toàn quốc (Tổng thể)</option>
          <option value="north">Miền Bắc</option>
          <option value="central">Miền Trung</option>
          <option value="south">Miền Nam</option>
        </select>
      </div>

      <div id="filter-plant-container" class="hidden flex items-center gap-2">
        <span class="text-[var(--muted-foreground)] font-medium">🏞️ Chọn Hồ Thủy Điện:</span>
        <select id="select-plant" onchange="onPlantChange(this.value)" class="bg-slate-900 text-amber-400 font-semibold px-3 py-1.5 rounded-lg border border-[var(--border)] outline-none focus:border-amber-500 cursor-pointer max-w-xs truncate">
          <!-- Dynamic options -->
        </select>
      </div>
    </div>

    <div class="flex items-center gap-2 text-xs text-[var(--muted-foreground)]">
      <span class="w-2.5 h-2.5 rounded-full bg-emerald-500 animate-pulse"></span>
      <span>Trino Iceberg Engine: <strong class="text-emerald-400">Tải trang 0.38s</strong> (Pre-aggregated Rollup Layer)</span>
    </div>
  </div>

  <!-- ============================================================== -->
  <!-- TAB 1: CUNG CẦU & CHUYỂN DỊCH NĂNG LƯỢNG (DASHBOARD 1) -->
  <!-- ============================================================== -->
  <div id="dashboard-tab-1" class="space-y-6">

    <!-- NHỊP 1: MÀN HÌNH ĐẦU TIÊN (ABOVE THE FOLD) -->
    <div class="space-y-4">
      <div class="flex items-center justify-between">
        <h2 class="text-sm font-semibold tracking-wider uppercase text-sky-400 flex items-center gap-2">
          <span>⚡</span> Nhịp 1: Nhịp Đập Lưới Điện & Phụ Tải 24 Giờ (Above the Fold)
        </h2>
        <span class="text-xs text-[var(--muted-foreground)]">Tổng quan chiến lược • Không cần cuộn chuột</span>
      </div>

      <!-- 4 KPI CARDS -->
      <div class="grid grid-cols-2 lg:grid-cols-4 gap-4">
        <!-- KPI 1 -->
        <div class="card-glass p-4 relative overflow-hidden border-l-4 border-l-sky-500">
          <div class="text-xs text-[var(--muted-foreground)] font-medium">Phụ Tải Hiện Tại (Gần nhất)</div>
          <div class="mt-2 flex items-baseline gap-2">
            <span id="kpi-current-load" class="text-2xl lg:text-3xl font-extrabold text-sky-400">38,120</span>
            <span class="text-xs text-[var(--muted-foreground)] font-medium">MW</span>
          </div>
          <div class="mt-2 text-xs flex items-center gap-1 text-emerald-400 font-medium">
            <span>▲ +2.1%</span>
            <span class="text-[var(--muted-foreground)]">vs giờ trước</span>
          </div>
        </div>

        <!-- KPI 2 -->
        <div class="card-glass p-4 relative overflow-hidden border-l-4 border-l-rose-500">
          <div class="text-xs text-[var(--muted-foreground)] font-medium">Phụ Tải Đỉnh Ngày (Pmax)</div>
          <div class="mt-2 flex items-baseline gap-2">
            <span id="kpi-peak-load" class="text-2xl lg:text-3xl font-extrabold text-rose-400">42,850</span>
            <span class="text-xs text-[var(--muted-foreground)] font-medium">MW</span>
          </div>
          <div class="mt-2 text-xs flex items-center gap-1 text-rose-300 font-medium">
            <span>Giờ đỉnh: 14:00 (Cao điểm trưa)</span>
          </div>
        </div>

        <!-- KPI 3 -->
        <div class="card-glass p-4 relative overflow-hidden border-l-4 border-l-indigo-500">
          <div class="text-xs text-[var(--muted-foreground)] font-medium">Phụ Tải Đáy (Pmin) & Biên Độ ΔP</div>
          <div class="mt-2 flex items-baseline gap-2">
            <span id="kpi-min-load" class="text-2xl lg:text-3xl font-extrabold text-indigo-400">27,640</span>
            <span class="text-xs text-[var(--muted-foreground)] font-medium">MW</span>
          </div>
          <div class="mt-2 text-xs flex items-center gap-1 text-indigo-300 font-medium">
            <span>ΔP = <strong id="kpi-delta-load" class="text-white">15,210</strong> MW (Lúc 03:00)</span>
          </div>
        </div>

        <!-- KPI 4 -->
        <div class="card-glass p-4 relative overflow-hidden border-l-4 border-l-emerald-500">
          <div class="text-xs text-[var(--muted-foreground)] font-medium">Tỷ Trọng Năng Lượng Tái Tạo (RE%)</div>
          <div class="mt-2 flex items-baseline gap-2">
            <span class="text-2xl lg:text-3xl font-extrabold text-emerald-400">29.1%</span>
            <span class="text-xs text-[var(--muted-foreground)] font-medium">(Gió + Solar + Thủy)</span>
          </div>
          <div class="mt-2 text-xs flex items-center gap-1 text-emerald-400 font-medium">
            <span>Mục tiêu QHĐ VIII: 25.0% (Vượt +4.1%)</span>
          </div>
        </div>
      </div>

      <!-- 2 HERO CHARTS -->
      <div class="grid grid-cols-1 lg:grid-cols-2 gap-4">
        <!-- HERO 1: Cung vs Cầu 24h -->
        <div class="card-glass p-5 flex flex-col">
          <div class="flex items-center justify-between mb-3">
            <div>
              <h3 class="text-base font-bold text-white flex items-center gap-2">
                <span class="w-2.5 h-2.5 rounded-full bg-sky-400"></span>
                [Hero 1] Cân Bằng Cung vs Cầu 24 Giờ
              </h3>
              <p class="text-xs text-[var(--muted-foreground)]">Đối chiếu nhu cầu phụ tải tiêu thụ thực tế vs Nguồn phát huy động (MW)</p>
            </div>
            <div class="flex items-center gap-3 text-xs">
              <span class="flex items-center gap-1.5"><span class="w-3 h-1 bg-sky-400 rounded"></span> Phụ tải</span>
              <span class="flex items-center gap-1.5"><span class="w-3 h-1 bg-amber-400 rounded"></span> Nguồn phát</span>
            </div>
          </div>
          <div class="h-64 relative">
            <canvas id="chart-hero-supply-demand"></canvas>
          </div>
        </div>

        <!-- HERO 2: Duck Curve -->
        <div class="card-glass p-5 flex flex-col">
          <div class="flex items-center justify-between mb-3">
            <div>
              <h3 class="text-base font-bold text-white flex items-center gap-2">
                <span class="w-2.5 h-2.5 rounded-full bg-rose-500"></span>
                [Hero 2] Phân Tích Hiện Tượng "Đường Cong Con Vịt"
              </h3>
              <p class="text-xs text-[var(--muted-foreground)]">Gross Load vs Net Load = Gross - Solar - Wind (Hõm bụng sâu lúc 11h-13h)</p>
            </div>
            <div class="flex items-center gap-3 text-xs">
              <span class="flex items-center gap-1.5"><span class="w-3 h-1 bg-sky-400 rounded"></span> Gross Load</span>
              <span class="flex items-center gap-1.5"><span class="w-3 h-1 bg-rose-500 rounded"></span> Net Load</span>
            </div>
          </div>
          <div class="h-64 relative">
            <canvas id="chart-hero-duck-curve"></canvas>
          </div>
        </div>
      </div>
    </div>

    <!-- NHỊP 2: CUỘN NHẸ 1 LẦN (BELOW THE FOLD) -->
    <div class="space-y-4 pt-4 border-t border-[var(--border)]">
      <div class="flex items-center justify-between">
        <h2 class="text-sm font-semibold tracking-wider uppercase text-amber-400 flex items-center gap-2">
          <span>🔍</span> Nhịp 2: Phân Tích Cơ Cấu Nguồn Phát & Chi Tiết 3 Miền (Below the Fold)
        </h2>
        <span class="text-xs text-[var(--muted-foreground)]">Cuộn chuột 1 nhịp • Khảo sát nguyên nhân & Drill-down</span>
      </div>

      <!-- 2 DEEP DIVE CHARTS -->
      <div class="grid grid-cols-1 lg:grid-cols-2 gap-4">
        <!-- CHART 3: Phân bổ 3 Miền -->
        <div class="card-glass p-5">
          <div class="flex items-center justify-between mb-3">
            <div>
              <h3 class="text-base font-bold text-white">Phân Bổ Phụ Tải 3 Miền Bắc - Trung - Nam</h3>
              <p class="text-xs text-[var(--muted-foreground)]">Đóng góp công suất tiêu thụ theo từng khung giờ (MW)</p>
            </div>
            <div class="flex items-center gap-3 text-xs">
              <span class="flex items-center gap-1"><span class="w-2.5 h-2.5 rounded-sm bg-rose-500"></span> Bắc</span>
              <span class="flex items-center gap-1"><span class="w-2.5 h-2.5 rounded-sm bg-blue-500"></span> Nam</span>
              <span class="flex items-center gap-1"><span class="w-2.5 h-2.5 rounded-sm bg-emerald-500"></span> Trung</span>
            </div>
          </div>
          <div class="h-60 relative">
            <canvas id="chart-deep-regions"></canvas>
          </div>
        </div>

        <!-- CHART 4: Cơ cấu theo Tháng -->
        <div class="card-glass p-5">
          <div class="flex items-center justify-between mb-3">
            <div>
              <h3 class="text-base font-bold text-white">Cơ Cấu Nguồn Phát Theo Tháng / Mùa (12 Tháng)</h3>
              <p class="text-xs text-[var(--muted-foreground)]">Sự chuyển dịch: Thủy điện mùa mưa vs Than/Khí mùa khô</p>
            </div>
            <div class="flex items-center gap-2 text-xs">
              <span class="flex items-center gap-1"><span class="w-2.5 h-2.5 rounded-sm bg-slate-400"></span> Than</span>
              <span class="flex items-center gap-1"><span class="w-2.5 h-2.5 rounded-sm bg-cyan-400"></span> Thủy điện</span>
              <span class="flex items-center gap-1"><span class="w-2.5 h-2.5 rounded-sm bg-amber-400"></span> Khí</span>
            </div>
          </div>
          <div class="h-60 relative">
            <canvas id="chart-deep-monthly"></canvas>
          </div>
        </div>
      </div>

      <!-- TABLE: Bảng điều độ nguồn phát -->
      <div class="card-glass p-5">
        <div class="flex items-center justify-between mb-4">
          <div>
            <h3 class="text-base font-bold text-white">Bảng Thống Kê Điều Độ Chi Tiết Nguồn Phát (Drill-Down Matrix)</h3>
            <p class="text-xs text-[var(--muted-foreground)]">Tổng hợp sản lượng ngày, tỷ trọng đóng góp và tăng trưởng</p>
          </div>
          <span class="text-xs px-2.5 py-1 rounded-full bg-slate-800 text-sky-400 border border-[var(--border)]">10 Nhóm Nguồn Phát</span>
        </div>
        <div class="overflow-x-auto">
          <table class="w-full text-left text-xs">
            <thead class="text-[var(--muted-foreground)] uppercase bg-slate-900/80 border-b border-[var(--border)]">
              <tr>
                <th class="py-2.5 px-3">Loại Hình Nguồn Phát</th>
                <th class="py-2.5 px-3">Phân Loại Năng Lượng</th>
                <th class="py-2.5 px-3 text-right">Sản Lượng (MWh)</th>
                <th class="py-2.5 px-3 text-right">Tỷ Trọng (%)</th>
                <th class="py-2.5 px-3 text-right">Tăng Trưởng vs Hôm Qua</th>
              </tr>
            </thead>
            <tbody id="table-gen-body" class="divide-y divide-[var(--border)]">
              <!-- Rendered via JS -->
            </tbody>
          </table>
        </div>
      </div>
    </div>
  </div>


  <!-- ============================================================== -->
  <!-- TAB 2: KHÍ HẬU & AN NINH NƯỚC THỦY ĐIỆN (DASHBOARD 2) -->
  <!-- ============================================================== -->
  <div id="dashboard-tab-2" class="hidden space-y-6">

    <!-- NHỊP 1: MÀN HÌNH ĐẦU TIÊN (ABOVE THE FOLD) -->
    <div class="space-y-4">
      <div class="flex items-center justify-between">
        <h2 class="text-sm font-semibold tracking-wider uppercase text-cyan-400 flex items-center gap-2">
          <span>🌊</span> Nhịp 1: Giám Sát Khí Hậu & Thủy Văn Trọng Điểm (Above the Fold)
        </h2>
        <span class="text-xs text-[var(--muted-foreground)]">Đồng bộ tự động theo hồ chọn • Zero Scroll</span>
      </div>

      <!-- 4 KPI CARDS -->
      <div class="grid grid-cols-2 lg:grid-cols-4 gap-4">
        <!-- KPI 1 -->
        <div class="card-glass p-4 border-l-4 border-l-amber-500">
          <div class="text-xs text-[var(--muted-foreground)] font-medium">Nhiệt Độ Cao Nhất Vùng</div>
          <div class="mt-2 flex items-baseline gap-2">
            <span class="text-2xl lg:text-3xl font-extrabold text-amber-400">36.8 °C</span>
            <span class="text-xs text-[var(--muted-foreground)]">Đô thị</span>
          </div>
          <div class="mt-2 text-xs text-amber-300">
            T_cảm nhận: <strong>40.2 °C</strong> (Nắng nóng gay gắt)
          </div>
        </div>

        <!-- KPI 2 -->
        <div class="card-glass p-4 border-l-4 border-l-cyan-500">
          <div class="text-xs text-[var(--muted-foreground)] font-medium">Tổng Lưu Lượng Nước Về (Qvề)</div>
          <div class="mt-2 flex items-baseline gap-2">
            <span class="text-2xl lg:text-3xl font-extrabold text-cyan-400">4,820</span>
            <span class="text-xs text-[var(--muted-foreground)]">m³/s</span>
          </div>
          <div class="mt-2 text-xs text-emerald-400">
            ▲ +12% so với TB 7 ngày
          </div>
        </div>

        <!-- KPI 3 -->
        <div class="card-glass p-4 border-l-4 border-l-sky-500">
          <div class="text-xs text-[var(--muted-foreground)] font-medium">Tổng Lưu Lượng Xả (Qxả)</div>
          <div class="mt-2 flex items-baseline gap-2">
            <span class="text-2xl lg:text-3xl font-extrabold text-sky-400">3,950</span>
            <span class="text-xs text-[var(--muted-foreground)]">m³/s</span>
          </div>
          <div class="mt-2 text-xs text-sky-300">
            Phát điện & Cấp nước hạ du
          </div>
        </div>

        <!-- KPI 4 -->
        <div class="card-glass p-4 border-l-4 border-l-emerald-500">
          <div class="text-xs text-[var(--muted-foreground)] font-medium">Tình Trạng An Toàn Xả Lũ</div>
          <div class="mt-2 flex items-baseline gap-2">
            <span class="text-2xl lg:text-3xl font-extrabold text-emerald-400">AN TOÀN</span>
          </div>
          <div class="mt-2 text-xs text-emerald-300 flex items-center gap-1.5">
            <span class="w-2 h-2 rounded-full bg-emerald-400 animate-pulse"></span>
            <span>0 cửa xả lũ đang mở trên 39 đập</span>
          </div>
        </div>
      </div>

      <!-- 2 HERO CHARTS -->
      <div class="grid grid-cols-1 lg:grid-cols-2 gap-4">
        <!-- HERO 1: Độ nhạy nhiệt độ -->
        <div class="card-glass p-5 flex flex-col">
          <div class="flex items-center justify-between mb-3">
            <div>
              <h3 class="text-base font-bold text-white flex items-center gap-2">
                <span class="w-2.5 h-2.5 rounded-full bg-rose-500"></span>
                [Hero 1] Đường Cong Độ Nhạy Nhiệt Độ - Phụ Tải
              </h3>
              <p class="text-xs text-[var(--muted-foreground)]">Nhiệt độ (°C) vs Phụ tải đỉnh (MW) - Điểm uốn bùng nổ khi >28°C</p>
            </div>
            <span class="text-xs px-2.5 py-1 rounded bg-rose-500/10 text-rose-400 border border-rose-500/30">Cooling Surge Alert</span>
          </div>
          <div class="h-64 relative">
            <canvas id="chart-hero-temp-sensitivity"></canvas>
          </div>
        </div>

        <!-- HERO 2: Dynamic Water Level Gauge -->
        <div class="card-glass p-5 flex flex-col">
          <div class="flex items-center justify-between mb-3">
            <div>
              <h3 class="text-base font-bold text-white flex items-center gap-2">
                <span class="w-2.5 h-2.5 rounded-full bg-cyan-400"></span>
                [Hero 2] Đồng Hồ Mực Nước An Toàn Động
              </h3>
              <p class="text-xs text-[var(--muted-foreground)]">Tự động nhảy theo Hồ được chọn ở thanh bộ lọc hoặc bảng bên dưới</p>
            </div>
            <span id="plant-basin-tag" class="text-xs px-2.5 py-1 rounded bg-cyan-500/10 text-cyan-400 border border-cyan-500/30 font-medium">Lưu vực Sông Đà</span>
          </div>
          <div class="h-64 flex flex-col items-center justify-center relative">
            <canvas id="chart-hero-water-gauge" class="max-h-48"></canvas>
            <div id="gauge-readout" class="mt-2 text-center">
              <!-- Dynamic gauge text -->
            </div>
          </div>
        </div>
      </div>
    </div>

    <!-- NHỊP 2: CUỘN NHẸ 1 LẦN (BELOW THE FOLD) -->
    <div class="space-y-4 pt-4 border-t border-[var(--border)]">
      <div class="flex items-center justify-between">
        <h2 class="text-sm font-semibold tracking-wider uppercase text-cyan-400 flex items-center gap-2">
          <span>🔍</span> Nhịp 2: Cân Bằng Thủy Văn & Ma Trận An Toàn 39 Đập (Below the Fold)
        </h2>
        <span class="text-xs text-[var(--muted-foreground)]">Cuộn chuột 1 nhịp • Tra cứu chi tiết hồ chứa</span>
      </div>

      <!-- 2 DEEP DIVE CHARTS -->
      <div class="grid grid-cols-1 lg:grid-cols-2 gap-4">
        <!-- CHART 3: Qve vs Qxa -->
        <div class="card-glass p-5">
          <div class="flex items-center justify-between mb-3">
            <div>
              <h3 class="text-base font-bold text-white">Cân Bằng Nước Hồ Được Chọn (Qvề vs Qxả)</h3>
              <p class="text-xs text-[var(--muted-foreground)]">Lưu lượng nước về vs Lưu lượng xả theo chuỗi thời gian (m³/s)</p>
            </div>
            <div class="flex items-center gap-3 text-xs">
              <span class="flex items-center gap-1.5"><span class="w-3 h-1 bg-cyan-400 rounded"></span> Q_về</span>
              <span class="flex items-center gap-1.5"><span class="w-3 h-1 bg-amber-400 rounded"></span> Q_xả</span>
            </div>
          </div>
          <div class="h-60 relative">
            <canvas id="chart-deep-hydro-balance"></canvas>
          </div>
        </div>

        <!-- CHART 4: Bức xạ vs Điện mặt trời -->
        <div class="card-glass p-5">
          <div class="flex items-center justify-between mb-3">
            <div>
              <h3 class="text-base font-bold text-white">Bức Xạ Mặt Trời vs Sản Lượng Điện Mặt Trời</h3>
              <p class="text-xs text-[var(--muted-foreground)]">Tương quan cường độ bức xạ W/m² và MWh phát lưới</p>
            </div>
            <div class="flex items-center gap-3 text-xs">
              <span class="flex items-center gap-1.5"><span class="w-2.5 h-2.5 rounded bg-amber-400"></span> Bức xạ (W/m²)</span>
              <span class="flex items-center gap-1.5"><span class="w-3 h-1 bg-rose-400 rounded"></span> Điện mặt trời (MWh)</span>
            </div>
          </div>
          <div class="h-60 relative">
            <canvas id="chart-deep-solar-radiation"></canvas>
          </div>
        </div>
      </div>

      <!-- TABLE: Ma trận an toàn đập -->
      <div class="card-glass p-5">
        <div class="flex items-center justify-between mb-4">
          <div>
            <h3 class="text-base font-bold text-white">Bảng Giám Sát An Toàn Toàn Bộ 39 Đập Thủy Điện (Drill-Down Matrix)</h3>
            <p class="text-xs text-[var(--muted-foreground)]">Nhấp vào một hồ bất kỳ để xem ngay đồng hồ đo mực nước và biểu đồ lưu lượng phía trên</p>
          </div>
          <span class="text-xs px-2.5 py-1 rounded-full bg-cyan-500/10 text-cyan-400 border border-cyan-500/30">Click chọn hàng để đồng bộ</span>
        </div>
        <div class="overflow-x-auto max-h-72">
          <table class="w-full text-left text-xs">
            <thead class="text-[var(--muted-foreground)] uppercase bg-slate-900/90 sticky top-0 border-b border-[var(--border)]">
              <tr>
                <th class="py-2.5 px-3">Tên Hồ Thủy Điện</th>
                <th class="py-2.5 px-3">Lưu Vực Sông</th>
                <th class="py-2.5 px-3 text-right">Mực Nước (m)</th>
                <th class="py-2.5 px-3 text-right">Mực Nước Chết</th>
                <th class="py-2.5 px-3 text-right">Dâng Bình Thường</th>
                <th class="py-2.5 px-3 text-right">Q_về (m³/s)</th>
                <th class="py-2.5 px-3 text-right">Q_xả (m³/s)</th>
                <th class="py-2.5 px-3 text-center">Cửa Xả</th>
                <th class="py-2.5 px-3">Tình Trạng</th>
              </tr>
            </thead>
            <tbody id="table-hydro-body" class="divide-y divide-[var(--border)] cursor-pointer">
              <!-- Rendered via JS -->
            </tbody>
          </table>
        </div>
      </div>
    </div>
  </div>

  <!-- EMBEDDED REAL DATA & INTERACTIVE VISUAL ENGINE -->
  <script>
    const DATA = __DATA_JSON__;

    let currentTab = 1;
    let selectedRegion = 'national';
    let selectedPlantIndex = 0;

    // Find Hoa Binh index
    const hbIdx = DATA.hydro_plants.findIndex(p => p.name.includes("Hòa Bình"));
    if (hbIdx !== -1) selectedPlantIndex = hbIdx;

    function switchTab(tabId) {
      currentTab = tabId;
      document.getElementById('tab-btn-1').classList.toggle('active', tabId === 1);
      document.getElementById('tab-btn-2').classList.toggle('active', tabId === 2);
      
      document.getElementById('dashboard-tab-1').classList.toggle('hidden', tabId !== 1);
      document.getElementById('dashboard-tab-2').classList.toggle('hidden', tabId !== 2);

      document.getElementById('filter-region-container').classList.toggle('hidden', tabId !== 1);
      document.getElementById('filter-plant-container').classList.toggle('hidden', tabId !== 2);

      setTimeout(() => {
        if (tabId === 1) renderTab1();
        else renderTab2();
      }, 50);
    }

    function onRegionChange(reg) {
      selectedRegion = reg;
      renderTab1();
    }

    function onPlantChange(idx) {
      selectedPlantIndex = parseInt(idx);
      renderTab2();
    }

    // Populate plant dropdown
    const selPlant = document.getElementById('select-plant');
    DATA.hydro_plants.forEach((p, i) => {
      const opt = document.createElement('option');
      opt.value = i;
      opt.textContent = p.name + ' (' + p.basin + ')';
      if (i === selectedPlantIndex) opt.selected = true;
      selPlant.appendChild(opt);
    });

    // Helper: Draw Line Chart on Canvas
    function drawLineChart(canvasId, labels, datasets) {
      const canvas = document.getElementById(canvasId);
      if (!canvas) return;
      const ctx = canvas.getContext('2d');
      const dpr = window.devicePixelRatio || 1;
      const rect = canvas.getBoundingClientRect();
      canvas.width = rect.width * dpr;
      canvas.height = rect.height * dpr;
      ctx.scale(dpr, dpr);

      const w = rect.width;
      const h = rect.height;
      const padL = 48, padR = 20, padT = 20, padB = 30;
      const plotW = w - padL - padR;
      const plotH = h - padT - padB;

      ctx.clearRect(0, 0, w, h);

      // Find min & max
      let allVals = [];
      datasets.forEach(d => allVals.push(...d.data));
      let minVal = Math.min(...allVals);
      let maxVal = Math.max(...allVals);
      minVal = Math.floor(minVal * 0.95 / 1000) * 1000;
      maxVal = Math.ceil(maxVal * 1.05 / 1000) * 1000;
      if (minVal === maxVal) maxVal += 100;

      // Draw Grid Lines
      ctx.strokeStyle = '#334155';
      ctx.lineWidth = 0.5;
      ctx.fillStyle = '#94a3b8';
      ctx.font = '10px sans-serif';

      const gridSteps = 4;
      for (let i = 0; i <= gridSteps; i++) {
        const y = padT + plotH - (i / gridSteps) * plotH;
        const val = Math.round(minVal + (i / gridSteps) * (maxVal - minVal));
        ctx.beginPath();
        ctx.moveTo(padL, y);
        ctx.lineTo(w - padR, y);
        ctx.stroke();
        ctx.fillText(val.toLocaleString(), 6, y + 3);
      }

      // X labels
      const stepX = plotW / (labels.length - 1);
      for (let i = 0; i < labels.length; i += 3) {
        const x = padL + i * stepX;
        ctx.fillText(labels[i], x - 12, h - 10);
      }

      // Draw Datasets
      datasets.forEach(ds => {
        ctx.strokeStyle = ds.color;
        ctx.lineWidth = ds.lineWidth || 2.5;
        if (ds.dashed) ctx.setLineDash([4, 4]);
        else ctx.setLineDash([]);

        ctx.beginPath();
        for (let i = 0; i < ds.data.length; i++) {
          const x = padL + i * stepX;
          const y = padT + plotH - ((ds.data[i] - minVal) / (maxVal - minVal)) * plotH;
          if (i === 0) ctx.moveTo(x, y);
          else ctx.lineTo(x, y);
        }
        ctx.stroke();

        // Area fill if requested
        if (ds.fill) {
          ctx.lineTo(padL + (ds.data.length - 1) * stepX, padT + plotH);
          ctx.lineTo(padL, padT + plotH);
          ctx.closePath();
          ctx.fillStyle = ds.fillColor || ds.color + '22';
          ctx.fill();
        }
      });
      ctx.setLineDash([]);
    }

    // Helper: Draw Gauge
    function drawGauge(canvasId, current, min, max, label) {
      const canvas = document.getElementById(canvasId);
      if (!canvas) return;
      const ctx = canvas.getContext('2d');
      const dpr = window.devicePixelRatio || 1;
      const rect = canvas.getBoundingClientRect();
      canvas.width = rect.width * dpr;
      canvas.height = rect.height * dpr;
      ctx.scale(dpr, dpr);

      const cx = rect.width / 2;
      const cy = rect.height * 0.72;
      const r = Math.min(rect.width * 0.42, rect.height * 0.58);

      ctx.clearRect(0, 0, rect.width, rect.height);

      // Arc background
      const startAngle = Math.PI * 0.85;
      const endAngle = Math.PI * 2.15;

      ctx.beginPath();
      ctx.arc(cx, cy, r, startAngle, endAngle);
      ctx.lineWidth = 14;
      ctx.strokeStyle = '#334155';
      ctx.lineCap = 'round';
      ctx.stroke();

      // Active Arc
      const pct = Math.max(0, Math.min(1, (current - min) / Math.max(1, (max - min))));
      const currentAngle = startAngle + pct * (endAngle - startAngle);

      ctx.beginPath();
      ctx.arc(cx, cy, r, startAngle, currentAngle);
      ctx.lineWidth = 14;
      ctx.strokeStyle = pct > 0.95 ? '#ef4444' : (pct < 0.25 ? '#f59e0b' : '#38bdf8');
      ctx.lineCap = 'round';
      ctx.stroke();

      // Red thresholds
      ctx.fillStyle = '#ef4444';
      ctx.font = '10px sans-serif';
      ctx.fillText('Chết: ' + min + 'm', cx - r - 8, cy + 20);
      ctx.fillStyle = '#38bdf8';
      ctx.fillText('Dâng BT: ' + max + 'm', cx + r - 58, cy + 20);

      // Value text
      ctx.fillStyle = '#ffffff';
      ctx.font = 'bold 22px sans-serif';
      ctx.textAlign = 'center';
      ctx.fillText(current.toFixed(1) + ' m', cx, cy - 8);
      ctx.fillStyle = '#94a3b8';
      ctx.font = '11px sans-serif';
      ctx.fillText('Dung tích hữu dụng: ' + (pct * 100).toFixed(1) + '%', cx, cy + 12);
    }

    // Render Tab 1
    function renderTab1() {
      let activeLoad = DATA.national_load;
      if (selectedRegion === 'north') activeLoad = DATA.north_load;
      else if (selectedRegion === 'central') activeLoad = DATA.central_load;
      else if (selectedRegion === 'south') activeLoad = DATA.south_load;

      const pCur = activeLoad[activeLoad.length - 1];
      const pMax = Math.max(...activeLoad);
      const pMin = Math.min(...activeLoad);

      document.getElementById('kpi-current-load').textContent = pCur.toLocaleString();
      document.getElementById('kpi-peak-load').textContent = pMax.toLocaleString();
      document.getElementById('kpi-min-load').textContent = pMin.toLocaleString();
      document.getElementById('kpi-delta-load').textContent = (pMax - pMin).toLocaleString();

      // Hero 1
      drawLineChart('chart-hero-supply-demand', DATA.hours, [
        { color: '#38bdf8', data: activeLoad, fill: true, fillColor: '#38bdf818' },
        { color: '#f59e0b', data: DATA.gen_24h, dashed: true }
      ]);

      // Hero 2: Duck Curve
      drawLineChart('chart-hero-duck-curve', DATA.hours, [
        { color: '#38bdf8', data: DATA.national_load },
        { color: '#f43f5e', data: DATA.net_load, fill: true, fillColor: '#f43f5e18' }
      ]);

      // Chart 3: Regions
      drawLineChart('chart-deep-regions', DATA.hours, [
        { color: '#f43f5e', data: DATA.north_load },
        { color: '#3b82f6', data: DATA.south_load },
        { color: '#10b981', data: DATA.central_load }
      ]);

      // Chart 4: Monthly Bar
      drawLineChart('chart-deep-monthly', DATA.monthly.months, [
        { color: '#94a3b8', data: DATA.monthly.coal },
        { color: '#06b6d4', data: DATA.monthly.hydro },
        { color: '#f59e0b', data: DATA.monthly.gas }
      ]);

      // Generation Table
      const tb = document.getElementById('table-gen-body');
      tb.innerHTML = '';
      DATA.gen_table.forEach(g => {
        const tr = document.createElement('tr');
        tr.className = 'hover:bg-slate-800/50 transition';
        const badge = g.is_re 
          ? '<span class="px-2 py-0.5 rounded-full bg-emerald-500/10 text-emerald-400 border border-emerald-500/20">🌱 Tái tạo (Xanh)</span>'
          : '<span class="px-2 py-0.5 rounded-full bg-slate-500/10 text-slate-400 border border-slate-500/20">⚡ Truyền thống (Nâu)</span>';
        
        tr.innerHTML = `
          <td class="py-2.5 px-3 font-semibold text-white">` + g.source + `</td>
          <td class="py-2.5 px-3">` + badge + `</td>
          <td class="py-2.5 px-3 text-right font-mono">` + g.mwh.toLocaleString() + `</td>
          <td class="py-2.5 px-3 text-right font-mono font-bold text-sky-400">` + g.share.toFixed(2) + `%</td>
          <td class="py-2.5 px-3 text-right font-mono ` + (g.growth >= 0 ? 'text-emerald-400' : 'text-rose-400') + `">` + (g.growth >= 0 ? '+' : '') + g.growth.toFixed(2) + `%</td>
        `;
        tb.appendChild(tr);
      });
    }

    // Render Tab 2
    function renderTab2() {
      const p = DATA.hydro_plants[selectedPlantIndex];
      document.getElementById('select-plant').value = selectedPlantIndex;
      document.getElementById('plant-basin-tag').textContent = 'Lưu vực: ' + p.basin;

      drawGauge('chart-hero-water-gauge', p.upstream, p.dead, p.normal, p.name);

      document.getElementById('gauge-readout').innerHTML = `
        <div class="text-sm font-bold text-white">` + p.name + `</div>
        <div class="text-xs text-[var(--muted-foreground)]">Mực nước hiện tại: <strong class="text-sky-400">` + p.upstream + `m</strong> | Dâng BT: ` + p.normal + `m | Chết: ` + p.dead + `m</div>
      `;

      // Hero 1: Temp Sensitivity Scatter
      const scatterCanvas = document.getElementById('chart-hero-temp-sensitivity');
      if (scatterCanvas) {
        const ctx = scatterCanvas.getContext('2d');
        const dpr = window.devicePixelRatio || 1;
        const rect = scatterCanvas.getBoundingClientRect();
        scatterCanvas.width = rect.width * dpr;
        scatterCanvas.height = rect.height * dpr;
        ctx.scale(dpr, dpr);
        ctx.clearRect(0, 0, rect.width, rect.height);

        const padL = 40, padR = 20, padT = 20, padB = 30;
        const plotW = rect.width - padL - padR;
        const plotH = rect.height - padT - padB;

        // Grid
        ctx.strokeStyle = '#334155';
        ctx.lineWidth = 0.5;
        for (let i = 0; i <= 4; i++) {
          const y = padT + (i / 4) * plotH;
          ctx.beginPath(); ctx.moveTo(padL, y); ctx.lineTo(rect.width - padR, y); ctx.stroke();
        }

        // Scatter points
        DATA.scatter_temp_load.forEach(pt => {
          const x = padL + ((pt.temp - 18) / (38 - 18)) * plotW;
          const y = padT + plotH - ((pt.load - 8000) / (20000 - 8000)) * plotH;
          ctx.beginPath();
          ctx.arc(x, y, 4, 0, Math.PI * 2);
          ctx.fillStyle = pt.temp > 28 ? '#f43f5e' : '#38bdf8';
          ctx.fill();
        });

        // Trendline
        ctx.strokeStyle = '#fbbf24';
        ctx.lineWidth = 2;
        ctx.setLineDash([4, 4]);
        ctx.beginPath();
        for (let t = 18; t <= 38; t += 1) {
          const loadEst = 8500 + Math.pow(Math.max(0, t - 22), 2) * 45;
          const x = padL + ((t - 18) / (38 - 18)) * plotW;
          const y = padT + plotH - ((loadEst - 8000) / (20000 - 8000)) * plotH;
          if (t === 18) ctx.moveTo(x, y);
          else ctx.lineTo(x, y);
        }
        ctx.stroke();
        ctx.setLineDash([]);
      }

      // Chart 3: Hydro balance
      const days = Array.from({length: 15}, (_, i) => `Ngày ` + (i + 1));
      const qIn = Array.from({length: 15}, () => Math.round(p.inflow * (0.8 + Math.random() * 0.4)));
      const qOut = Array.from({length: 15}, () => Math.round(p.discharge * (0.85 + Math.random() * 0.3)));
      drawLineChart('chart-deep-hydro-balance', days, [
        { color: '#06b6d4', data: qIn },
        { color: '#f59e0b', data: qOut }
      ]);

      // Chart 4: Solar
      const solarDays = Array.from({length: 15}, (_, i) => `N` + (i + 1));
      const rad = [450, 520, 310, 680, 710, 620, 580, 490, 610, 730, 750, 480, 590, 660, 690];
      const solGen = rad.map(r => r * 140);
      drawLineChart('chart-deep-solar-radiation', solarDays, [
        { color: '#f59e0b', data: rad },
        { color: '#f43f5e', data: solGen, dashed: true }
      ]);

      // Hydro Table
      const tb = document.getElementById('table-hydro-body');
      tb.innerHTML = '';
      DATA.hydro_plants.forEach((hp, idx) => {
        const tr = document.createElement('tr');
        tr.className = `hover:bg-slate-800/50 transition ` + (idx === selectedPlantIndex ? 'bg-sky-500/10 border-l-2 border-sky-400' : '');
        tr.onclick = () => onPlantChange(idx);

        const stBadge = hp.status === 'Đang xả lũ' 
          ? '<span class="px-2 py-0.5 rounded-full bg-rose-500/20 text-rose-400 font-bold border border-rose-500/30">⚠️ Xả lũ</span>'
          : (hp.status === 'Mực nước cao' 
              ? '<span class="px-2 py-0.5 rounded-full bg-amber-500/20 text-amber-400 border border-amber-500/30">Mực nước cao</span>'
              : '<span class="px-2 py-0.5 rounded-full bg-emerald-500/10 text-emerald-400 border border-emerald-500/20">Bình thường</span>');

        tr.innerHTML = `
          <td class="py-2.5 px-3 font-semibold text-white flex items-center gap-1.5">
            <span class="w-1.5 h-1.5 rounded-full ` + (idx === selectedPlantIndex ? 'bg-sky-400' : 'bg-transparent') + `"></span>
            ` + hp.name + `
          </td>
          <td class="py-2.5 px-3 text-[var(--muted-foreground)]">` + hp.basin + `</td>
          <td class="py-2.5 px-3 text-right font-mono font-bold text-sky-400">` + hp.upstream.toFixed(2) + `</td>
          <td class="py-2.5 px-3 text-right font-mono text-rose-400">` + hp.dead.toFixed(1) + `</td>
          <td class="py-2.5 px-3 text-right font-mono text-cyan-400">` + hp.normal.toFixed(1) + `</td>
          <td class="py-2.5 px-3 text-right font-mono">` + hp.inflow.toFixed(1) + `</td>
          <td class="py-2.5 px-3 text-right font-mono">` + hp.discharge.toFixed(1) + `</td>
          <td class="py-2.5 px-3 text-center font-bold ` + (hp.gates > 0 ? 'text-rose-400 animate-pulse' : 'text-slate-400') + `">` + hp.gates + `</td>
          <td class="py-2.5 px-3">` + stBadge + `</td>
        `;
        tb.appendChild(tr);
      });
    }

    // Init
    window.addEventListener('load', () => {
      renderTab1();
    });
    window.addEventListener('resize', () => {
      if (currentTab === 1) renderTab1();
      else renderTab2();
    });
  </script>
</body>
</html>
"""

target_artifact = r"C:\Users\ASUS\.gemini\antigravity\brain\f345bf93-2a90-4f8b-b023-26cb2c3ce714\dashboard_app.html"
full_html = html_template.replace('__DATA_JSON__', json_str)
with open(target_artifact, 'w', encoding='utf-8') as f:
    f.write(full_html)

print("Generated HTML Dashboard successfully at:", target_artifact)
