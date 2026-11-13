# Lakehouse Docker Stack

Stack local de dung lakehouse voi MinIO, Spark, Apache Iceberg, Nessie,
Trino, Airflow va Metabase.

## Version matrix

| Component | Version/Image |
| --- | --- |
| MinIO | `quay.io/minio/minio:latest` |
| Spark | `apache/spark:4.1.3-scala2.13-java17-python3-ubuntu` |
| Iceberg | `1.11.0` |
| Nessie | `ghcr.io/projectnessie/nessie:0.108.4` |
| Trino | `trinodb/trino:483` |
| Airflow | `apache/airflow:3.3.1-python3.12` |
| Metabase | `metabase/metabase:v0.63.17` |
| Postgres | `postgres:16-alpine` |

Spark 4.2.0 da co release, nhung Iceberg 1.11.0 hien cung cap runtime jar
chinh thuc cho Spark 4.1/4.0/3.5. Vi vay stack pin Spark 4.1.3 de tranh
lech dependency.

Metabase dung official Starburst connection de ket noi Trino. Starburst driver
trong Metabase cung ho tro Trino, nen BI layer se query Iceberg thong qua Trino
catalog `iceberg`.

## Start

```powershell
docker compose up -d --build --remove-orphans
```

Lan dau build Airflow se cai Java, PySpark va Airflow providers nen co the mat
vai phut. Metabase khong can build rieng vi dung image official.

## UI va endpoint

| Service | URL | Login |
| --- | --- | --- |
| MinIO API | http://localhost:9000 | `minioadmin` / `minioadmin` |
| MinIO Console | http://localhost:9001 | `minioadmin` / `minioadmin` |
| Nessie API | http://localhost:19120 | no auth |
| Spark Master UI | http://localhost:8081 | n/a |
| Spark Worker UI | http://localhost:8083 | n/a |
| Trino | http://localhost:8082 | user `trino` |
| Airflow | http://localhost:8080 | `airflow` / `airflow` |
| Metabase | http://localhost:3000 | setup lan dau tren UI |
| Postgres | `localhost:5433` | per-service users |

## Metabase

Metabase dung Postgres lam application database:

```text
postgres://metabase:metabase@postgres:5432/metabase
```

Lan dau vao http://localhost:3000, tao admin user trong UI. Sau do them database
ket noi Trino nhu sau:

```text
Database type: Starburst
Display name: Trino Iceberg
Host: trino
Port: 8080
Catalog: iceberg
Schema: de trong hoac nhap demo neu chi muon browse schema demo
Username: trino
Password: de trong
SSL: off
```

Neu UI co muc connection string, co the dung:

```text
jdbc:trino://trino:8080/iceberg
```

## Smoke test bang Spark SQL

```powershell
docker compose run --rm spark-sql -e "CREATE NAMESPACE IF NOT EXISTS nessie.demo; CREATE TABLE IF NOT EXISTS nessie.demo.events (id BIGINT, name STRING) USING iceberg; INSERT INTO nessie.demo.events VALUES (1, 'hello-lakehouse'); SELECT * FROM nessie.demo.events;"
```

Kiem tra bang Trino:

```powershell
docker compose exec trino trino --catalog iceberg --schema demo --execute "SELECT * FROM events"
```

## Airflow DAG

DAG mau `lakehouse_smoke_dag` submit PySpark job
`/opt/lakehouse/scripts/spark/iceberg_smoke.py` len Spark standalone cluster va
ghi vao `nessie.demo.airflow_events`.

## Project layout

```text
airflow/          Airflow DAGs, plugins va logs
config/           Runtime config cho Spark va Trino
infra/docker/     Dockerfiles va image requirements
infra/postgres/   Postgres bootstrap scripts
scripts/spark/    PySpark jobs dung cho Airflow hoac chay truc tiep
```

## Stop/reset

```powershell
docker compose down
```

Xoa ca data volumes:

```powershell
docker compose down -v
```

## Tai lieu tham chieu

- Spark downloads: https://spark.apache.org/downloads
- Iceberg releases: https://iceberg.apache.org/releases/
- Nessie Iceberg REST: https://projectnessie.org/guides/iceberg-rest/
- Trino Iceberg/REST catalog: https://trino.io/docs/current/object-storage/metastores.html
- Airflow Docker Compose: https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html
- Metabase Docker: https://www.metabase.com/docs/latest/installation-and-operation/running-metabase-on-docker
- Metabase Starburst/Trino connection: https://github.com/metabase/metabase/blob/master/docs/databases/connections/starburst.md

## NSMO Load va NSMO Power

NSMO Load va NSMO Power la hai dataset rieng: `nsmo_load` va
`nsmo_power`. Hai bang Bronze la `nessie.bronze.nsmo_load` va
`nessie.bronze.nsmo_power`. Cac bang nay dung Nessie REST catalog,
MinIO endpoint `http://minio:9000` va warehouse `s3://warehouse/` trong
`docker-compose.yml`; Iceberg tu quan ly location rieng cho tung bang.
Bảng Load hiện có đã được đổi tên trong catalog thành `nessie.bronze.nsmo_load`;
location vật lý cũ và lịch sử audit được giữ nguyên.

Silver `nessie.silver.load`, `nessie.silver.power_daily` va `nessie.silver.power_hourly` giu du cac
diem 30 phut. Gold `nessie.gold.fact_load_hourly` va
`nessie.gold.fact_generation_power_hourly` chi giu gia tri goc tai HH:00, gom ca
D+1 00:00. Gold không lấy trung bình và không tính MWh.
`fact_generation_power_hourly` có 10 cột: `generation_power_key` (INT), `date_key`,
`time_key`, `energy_source_key`, `generation_power_mw`, `generation_power_share_pct`,
`previous_generation_power_mw`, `generation_power_change_mw`, `generation_power_growth_pct`,
`generation_power_rank_in_hour`. Tỷ trọng tính trên các nguồn NSMO có dữ liệu tại cùng giờ;
previous/change/growth dùng đúng giờ liền trước, thiếu giờ thì NULL. Khóa số nguyên được tạo từ
ngày, giờ và nguồn điện; giá trị công suất thay đổi không làm đổi khóa.
Quy ước tên cột và cách đổi tên các bảng đang lưu được ghi tại
[Tên cột ba bảng phát điện](docs/generation_column_names.md).
Hướng dẫn nâng cấp schema hourly và tính lại lịch sử:
[Chỉ số công suất theo giờ](docs/hourly_power_metrics.md).
Moi bang Silver valid/reject dung partition `report_date` va cung mot writer.
NSMO Load/Power lay `report_date` tu ngay bao cao; EVN, Hydro va thoi tiet dat
`report_date = data_date` sau extract. `data_date`/timestamp cua diem do van
duoc giu de Gold tao `date_key` theo ngay su kien. Cac bang Silver cu duoc giu
duoi ten `*_data_date_backup_20261107` sau khi chuyen partition.
Silver Power hourly cung dat `data_date` theo thoi diem do: diem D+1 00:00 co
`report_date = D` va `data_date = D+1`.
Gia tri `generation_power_mw` am duoc ghi vao Silver reject voi ma
`POWER_NEGATIVE`; Gold chi doc cac dong Silver hop le.
Gold lay `report_date` thay doi tu metadata snapshot/partition cua Silver,
khong so row Silver voi Gold. PostgreSQL `gold_silver_snapshot_checkpoint`
giu snapshot Silver cuoi cung da duoc tung fact Gold xu ly thanh cong. Gold
doc `data_date` cua cac report thay doi de chon partition Gold can MERGE;
Hai fact EVN đọc thêm report liền kề để tính lag. Load hourly và Power hourly đọc đầy đủ
report của các ngày sự kiện cần ghi và report ngày trước, tính lại cả ngày chứa
giờ kế tiếp bị ảnh hưởng khi sửa dữ liệu cũ, kể cả khi report kết thúc ở 23:00 và thiếu 00:00.
Cả hai fact hourly chỉ dùng giá trị đúng một giờ trước; thiếu giờ thì previous/change
là NULL. Load vẫn tính avg/min/peak/rank theo ngày và vùng trên các điểm HH:00 có dữ liệu.
Thay đổi logic không tự cập nhật lịch sử đã ghi: chạy backfill `fact_load_hourly`
cho phạm vi report cần tính lại; schema Load không đổi. Checkpoint chỉ tiến
sau khi Gold ghi xong; backfill
theo ngay khong day checkpoint. Neu snapshot checkpoint da het han, Gold
refresh toan bo partition Silver hien tai. Silver overwrite cung ngay van tao
snapshot moi va co the khien Gold xu ly lai, du du lieu khong doi.
Moi fact Gold duoc partition theo `date_key` cua ngay su kien va MERGE theo
grain (ngay/gio/doi tuong). Gold Power hourly match theo `date_key`, `time_key`, `energy_source_key`;
report D ghi vao ca partition D va D+1 neu co diem 00:00. MERGE cap nhat hoac them diem co trong
Silver, khong tu dong xoa diem cu neu nguon sau nay bo mot dong.
`dim_energy_source` có sáu cột: `energy_source_key`, `data_provider`, `source_name`,
`source_group`, `reporting_basis`, `row_type`; không có ngày hiệu lực. Mã nguồn
được tạo từ nơi cung cấp (EVN/NSMO) và tên được công bố, chỉ bỏ khác biệt về
chữ hoa/thường và khoảng trắng. Tên `Gió`, `Điện gió` và các tên `Khác (...)`
được giữ riêng; `source_group` chỉ phân nhóm báo cáo, không quyết định mã nguồn.
Ước tính đầu cực/thương phẩm và dòng tổng/thành phần được phân biệt bằng hai
cột mô tả. Nguồn chưa rõ được giữ lại với nhóm `Chưa xác định`.
Ba fact Generation Daily, Generation Power Daily và Generation Power Hourly
dùng cùng cách tạo mã, với nơi cung cấp tương ứng EVN, EVN và NSMO.
Khi nâng cấp từ cấu trúc cũ, cần chuyển đồng bộ dim và ba fact trước khi chạy
job mới. `maintenance/rebuild_energy_sources.py` xem trước theo mặc định;
`--apply` dựng lại từ Silver và giữ nguyên bảng cũ dưới tên sao lưu.
Chi tiết cấu trúc và cách chuyển đổi ở [energy_source_redesign.md](docs/energy_source_redesign.md).
Moi bang Gold dimension duoc MERGE theo khoa cua no; nguon vang mat trong lan
chay moi khong xoa dong dimension cu. `dim_plant` giu khoa cua ho da co theo
ten ho va chi cap khoa moi cho ho moi, tranh doi khoa khi Silver thieu mot ho.
`dim_location` cung giu khoa theo ten dia diem neu danh sach dia diem doi thu tu.
Neu Silver xoa het mot partition bao cao, Gold dung va giu checkpoint cu de
khong bo qua ngay do; can xu ly xoa Gold theo chinh sach duoc phe duyet.
`generation_power_mw` co `unit_status=ASSUMED_MW`: MW la gia dinh duoc
doi chieu, chua co metadata chinh thuc tu NSMO. Timestamp Mix la thoi gian
local khong co offset (`TIMESTAMP_NTZ`); chua duoc chuyen thanh UTC instant.

NSMO Power lay API
`GetTongHopCongSuatBcsxTheoLoaiHinh?ngay=YYYY-MM-DD`. Bronze append raw,
Silver clean/reject partition theo ngay bao cao, Gold partition theo ngay
su kien. Watermark NSMO Power ghi ngay bao cao lon nhat da ghi vao Bronze ngay sau
khi Bronze commit; ngay thieu o giua xu ly bang backfill thu cong. Ngay pilot
mac dinh `2026-11-04`; thay doi bang bien moi truong
`NSMO_MIX_START_DATE` truoc lan chay dau neu can lich su som hon. `backfill`
can `start_date` va `end_date` theo **ngay bao cao**. Backfill ngay cu giu nguyen
watermark; ngay moi xa hon se nang watermark sau khi Bronze ghi. DAG dung
`params.run_mode`, `params.start_date`, `params.end_date`.
Silver doc partition Bronze theo `ingest_date` cua ngay chay (co the chi dinh
`--run-date`), ke ca khi Bronze vua backfill ngay bao cao cu.

Chay backfill qua DAG `electricity_lakehouse_pipeline` voi
`params.run_mode=backfill`, `params.start_date` va `params.end_date` theo ngay
bao cao. DAG cung cap cau hinh Spark, Iceberg va Nessie cho cac task.

NSMO Load dung cung pattern backfill voi `nsmo_load`, `load` va
`fact_load_hourly`. Replay Bronze Load la can thiet de khoi phuc cac diem
`:30` da bi bo o Silver truoc day. Khi replay theo mot report date, kiem tra
ca hai event dates D va D+1 truoc khi tiep tuc pham vi rong hon.
