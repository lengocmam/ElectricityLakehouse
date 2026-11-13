# Dự báo điện bằng S4D và các thuật toán từ bài báo (bản cuối)

Cập nhật 29/10/2026. Thuật toán chính là **S4D**; so sánh với **TimeXer** và **S-Mamba** (lấy từ bài báo) và **LightGBM** (baseline, được giữ lại).

## Các thuật toán

| Thuật toán | Nguồn | Ghi chú |
|---|---|---|
| **S4D** | arXiv 2206.11893 (đơn giản hóa của S4, arXiv 2111.00396) | Tự cài đặt, thuật toán chính |
| TimeXer | NeurIPS 2024, arXiv 2402.19072 | Transformer cho dự báo có biến ngoại sinh; bản gọn tự cài đặt |
| S-Mamba | arXiv 2403.11144 | Mamba hai chiều quét qua các biến; tự cài đặt bằng PyTorch thuần (CPU, không cần mamba-ssm/CUDA) |
| LightGBM | baseline cổ điển | Boosting trên đặc trưng lag, dự báo phần dư so với hôm trước |

Cả bốn mô hình nhận cùng thông tin: lịch sử các biến, cộng đặc trưng biết trước của các ngày cần dự báo (lịch, ngày lễ). TimeXer và S-Mamba bản gốc không dùng đặc trưng biết trước; ở đây chúng được nối vào phần đầu ra giống S4D để so sánh công bằng.

## Giao thức

Dữ liệu ngày quốc gia (`data_v2/`), test là năm 2026. Mỗi mô hình tinh chỉnh ngẫu nhiên trên validation (8 lần thử), rồi chạy lại 5 seed mới và lấy trung bình dự báo (tổ hợp 5 seed). Phụ tải dùng đặc trưng lịch + thời tiết + ngày lễ; thủy điện dùng lịch + thủy văn.

## Kết quả (MAE trên test 2026; in đậm là tốt nhất)

| Bài toán | LightGBM | TimeXer | S-Mamba | S4D | **Trung bình 3 mô hình bài báo** |
|---|---|---|---|---|---|
| Phụ tải 1 ngày (MW) | **834** | 1.018 | 979 | 919 | 873 |
| Phụ tải 7 ngày (MW) | 1.933 | 1.639 | 1.665 | 1.560 | **1.545** |
| Thủy điện 1 ngày (MWh) | **14.787** | 16.416 | 16.423 | 16.804 | 15.599 |
| Thủy điện 7 ngày (MWh) | 25.167 | 24.367 | 24.343 | 30.177 | **23.994** |

"Trung bình 3 mô hình bài báo" là trung bình dự báo của S4D, TimeXer, S-Mamba, không huấn luyện thêm (`python ensemble_paper.py`). Kiểm định Diebold–Mariano của nó so với LightGBM: phụ tải 7 ngày tốt hơn (p = 0,004); phụ tải 1 ngày (p = 0,36), thủy điện 1 ngày (p = 0,16) và thủy điện 7 ngày (p = 0,28) chưa khác biệt rõ.

### Tinh chỉnh mở rộng (12 lần thử, thêm tùy chọn "mốc neo"), chỉ chạy cho phụ tải

| Bài toán | Mô hình | 8 lần thử | 12 lần thử + mốc neo |
|---|---|---|---|
| Phụ tải 1 ngày | S4D | 919 | 929 |
| | TimeXer | 1.018 | 997 |
| | S-Mamba | 979 | **911** |
| | Trung bình 3 mô hình | 873 | **835** (LightGBM: 834, p = 0,98) |
| Phụ tải 7 ngày | S4D | 1.560 | 1.597 |
| | TimeXer | 1.639 | 1.612 |
| | S-Mamba | 1.665 | 1.627 |
| | Trung bình 3 mô hình | 1.545 | **1.531** (tốt hơn LightGBM, p = 0,001) |

Tinh chỉnh mở rộng giúp S-Mamba và TimeXer ở phụ tải 1 ngày, nhưng **không cải thiện S4D ở cả hai bài toán** (cải thiện trên validation không chuyển sang test). "Mốc neo" (mô hình chỉ học phần lệch so với hôm qua hoặc cùng thứ tuần trước) không phải cải tiến chung: validation chỉ cho S-Mamba chọn mốc "hôm qua", và một lần thử ép dùng "cùng thứ tuần trước" cho kết quả từng seed tệ hơn. Tinh chỉnh mở rộng cho thủy điện **chưa chạy xong** nên chưa có trong bảng.

### Kết luận theo bài toán

- **Phụ tải 7 ngày:** các mô hình bài báo thắng LightGBM rõ; S4D (8 lần thử) tốt nhất đơn lẻ, và trung bình ba mô hình nhỉnh hơn một chút nhưng chưa khác biệt rõ với S4D (p = 0,23 sau tinh chỉnh mở rộng).
- **Phụ tải 1 ngày:** LightGBM tốt nhất; trung bình ba mô hình bài báo sau tinh chỉnh mở rộng ngang LightGBM và gần như không thiên lệch (−0,01% so với −0,65%).
- **Thủy điện:** S4D không ổn định (±2.791 MWh giữa các seed ở 7 ngày); TimeXer và S-Mamba ổn định và tốt hơn ở 7 ngày; ở 1 ngày LightGBM tốt nhất.

## Các phần khác trong gói

- `code/results/extended4/EXTENDED_METRICS.md`: chỉ số mở rộng cho cả bốn mô hình (thiên lệch, sai số đuôi, R², bootstrap, theo bước dự báo và theo loại ngày). `python extended_metrics.py`.
- `code/hybrid.py`: mô hình ghép LightGBM + S4D. Lợi ích nhỏ và mong manh (blend theo bước dự báo tốt hơn S4D khoảng 4% khi gộp, p = 0,043; chỉ 2,4% và không có ý nghĩa khi đo theo thời gian). Định tuyến cứng theo loại ngày không giúp gì.
- `code/chronos2_zeroshot.py`: Chronos-2 zero-shot, chạy trên máy bạn (`pip install "chronos-forecasting>=2.0" "pandas[pyarrow]"`). Phần dựng dữ liệu đã chạy thử, **phần gọi mô hình chưa chạy được** trong môi trường của tôi nên chưa kiểm chứng.
- `data_v2/`: làm sạch dữ liệu và lịch ngày lễ. Lịch đã đối chiếu văn bản chính thức (Tết 2023 khớp; phát hiện thiếu ngày làm bù 10/01/2026 và đã sửa ở `data_v2/`). Các kết quả trên dùng `code/unified_daily_v2.csv` (trước khi sửa). Chạy lại phụ tải 7 ngày với dữ liệu đã sửa chỉ làm MAE đổi dưới 0,5%.

## Chạy

```bash
cd code && pip install -r requirements.txt        # cần PyTorch; tự dùng GPU nếu có
sh run_best.sh load_H7        # hoặc load_H1 | hydro_H1 | hydro_H7 | all  (cấu hình tốt nhất: best_configs/)
python ensemble_paper.py      # bảng trung bình 3 mô hình bài báo so với LightGBM
sh run_tune2.sh               # tinh chỉnh mở rộng (có lưu tiến trình, chạy lại để tiếp tục); kết quả trong results/tuned2
```

`best_configs_extended/` chứa cấu hình tinh chỉnh mở rộng cho phụ tải. Dựng lại dữ liệu: `cd data_v2 && sh run_all.sh <thư_mục_chứa_3_csv_gốc>`.

## Giới hạn

Chỉ một tập test (2026) cho các bảng chính, khoảng 700 cửa sổ train; p-value chưa hiệu chỉnh đa so sánh; trung bình 3 mô hình là lựa chọn tốt nhất trong nhiều lựa chọn nên có thiên lệch chọn lọc; TimeXer và S-Mamba là bản tự cài đặt gọn; thời tiết là giá trị thực tế, không phải dự báo; walk-forward chỉ có S4D và LightGBM (chưa chạy cho TimeXer và S-Mamba); ablation đặc trưng (`results/abl*`) chạy trên phiên bản trước khi thêm đặc trưng biết trước; chưa làm khoảng dự báo và nhánh hệ thống thủy điện–phụ tải–nhiệt điện.
