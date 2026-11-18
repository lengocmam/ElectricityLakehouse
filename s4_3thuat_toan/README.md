# Bộ ba thuật toán lõi dự báo điện lực: S4D, S-Mamba và XGBoost

Bộ ba thuật toán được chuẩn hóa cho toàn bộ quá trình huấn luyện và phát triển của luận văn:
1. **S4D (Diagonal Structured State Space Model)**: Đại diện dòng mô hình không gian trạng thái liên tục với ma trận đường chéo phức, huấn luyện tích chập nhanh qua FFT và suy luận đệ quy $O(1)$.
2. **S-Mamba (Selective State Space Model, Wang et al. 2024)**: Đại diện dòng mô hình chọn lọc trạng thái (Selective SSM) hiện đại nhất, quét hai chiều chọn lọc qua các biến đặc trưng để nắm bắt tương quan chéo giữa thời tiết, phụ tải và nguồn phát.
3. **XGBoost (Extreme Gradient Boosting)**: Đại diện dòng học máy dạng bảng dựa trên cây quyết định (GBDT) - chuẩn mực công nghiệp tối ưu hóa sai số tuyệt đối với tốc độ cao và tính ổn định bền bỉ.

## Các thuật toán lõi

| Thuật toán | Nguồn gốc | Kiến trúc & Cơ chế | Vai trò trong luận văn |
|---|---|---|---|
| **S4D** | arXiv 2206.11893 (Gu et al., 2022) | Continuous Complex Diagonal SSM, Cauchy Kernel Convolution qua FFT | Mô hình chuỗi thời gian liên tục $O(L \log L)$ |
| **S-Mamba** | arXiv 2403.11144 (Wang et al., 2024) | Bidirectional Selective State Space Layers quét qua kênh biến | Mô hình Deep SSM hiện đại nhất học tương quan đa biến |
| **XGBoost** | Chen & Guestrin, KDD 2016 | Gradient Boosted Decision Trees với L1/L2 regularization | Chuẩn mực đối sánh máy học dạng cây công nghiệp |

## Giao thức huấn luyện chống rò rỉ (Strict Anti-Leakage Protocol)
- **Tập dữ liệu**: Hồ dữ liệu Lakehouse theo giờ (`data_exports/hourly_feature_store.csv`, 25.199 dòng đo lường thực tế từ 01/01/2024 đến 16/11/2026).
- **Phân chia theo thứ tự thời gian (Chronological Split)**:
  * **Train Set** (69,5%, 17.519 dòng): 02/01/2024 01:00 đến 31/12/2025 23:00 (2 năm trọn vẹn, 8 chu kỳ mùa). Mọi phép chuẩn hóa (Scalers), ma trận tương quan và lựa chọn biến CHỈ được tính trên tập này.
  * **Validation Set** (17,2%, 4.344 dòng): 01/01/2026 00:00 đến 30/06/2026 23:00 (H1/2026 dùng để tinh chỉnh siêu tham số và early stopping).
  * **Test Set Out-Of-Distribution** (13,2%, 3.336 dòng): 01/07/2026 00:00 đến 16/11/2026 23:00 (Đánh giá mù tuyệt đối trên dữ liệu thực tế).
