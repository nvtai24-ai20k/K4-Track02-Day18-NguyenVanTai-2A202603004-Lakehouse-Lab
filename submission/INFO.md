# Thông tin bài nộp — K4-Track02-Day18 Lakehouse Lab

| Mục | Giá trị |
|---|---|
| Họ tên | Nguyễn Văn Tài |
| MSSV | 2A202603004 |
| Mã bài | K4-Track02-Day18 |
| Repo | https://github.com/nvtai24-ai20k/K4-Track02-Day18-NguyenVanTai-2A202603004-Lakehouse-Lab |
| Đường chạy | **Lightweight** cho cả 8 notebook (NB1–NB4 **không** dùng Spark) |
| Python | 3.11 (venv `.venv`) |
| Hệ điều hành | Windows 11 Home (10.0.26300), PowerShell / Git Bash |

## Phiên bản thư viện chính

`deltalake 1.6.6` · `pyiceberg 0.12.0` (`pyiceberg-core 0.10.1`) · `duckdb 1.5.6` ·
`polars 1.44.2` · `pyarrow 25.0.1` · `numpy 2.4.6`

## Kết quả kiểm tra

| Lệnh (PowerShell tương đương `make`) | Kết quả |
|---|---|
| `python scripts/verify_lite.py` (smoke) | 9/9 PASS |
| `python -m pytest` | 24/24 PASS |
| `python scripts/run_all.py` | 8/8 notebook PASS |

## Bản đồ bằng chứng

| NB | Notebook | Ảnh | Số đo chính |
|---|---|---|---|
| 1 | [01_delta_basics](notebooks/01_delta_basics.ipynb) | [nb01_delta_log](screenshots/nb01_delta_log.png) | 2 commit JSON; `age='thirty'` bị chặn, version 0→0; `tier` thêm qua merge |
| 2 | [02_optimize_zorder](notebooks/02_optimize_zorder.ipynb) | [nb02_optimize](screenshots/nb02_optimize.png), [nb02_zorder_stats](screenshots/nb02_zorder_stats.png) | 200 → 55 file; pruning 55×; speedup > 3× (wall-clock, dao động) |
| 3 | [03_time_travel](notebooks/03_time_travel.ipynb) | [nb03_history_restore](screenshots/nb03_history_restore.png) | MERGE 50K update + 50K insert; 5 version gồm RESTORE; `score<0` = 0 |
| 4 | [04_medallion](notebooks/04_medallion.ipynb) | [nb04_gold](screenshots/nb04_gold.png) | Bronze 200 000 → Silver 190 052; Gold 7 ngày × 3 model, assert p50≤p95, cost>0, error_rate∈[0,1] |
| 5 | [05_iceberg_catalog](notebooks/05_iceberg_catalog.ipynb) | [nb05_iceberg](screenshots/nb05_iceberg.png) | pruning 10×; field_id 4 giữ qua rename; spec 1 & 2 cùng tồn tại |
| 6 | [06_maintenance](notebooks/06_maintenance.ipynb) | [delta](screenshots/nb06_maintenance_delta.png), [iceberg](screenshots/nb06_maintenance_iceberg.png) | 200→11 file (18×); skip 90%; vacuum 16.1 MB; 3 orphan; 20→3 snapshot + 17 manifest list; checkpoint v203 |
| 7 | [07_vectors_multimodal](notebooks/07_vectors_multimodal.ipynb) | [nb07_vectors](screenshots/nb07_vectors.png), [nb07_lifecycle](screenshots/nb07_lifecycle.png) | amplification 200×; int8 5.8× nhỏ hơn; recall@10 0.904; fidelity 1.000; lifecycle bug 0 vs 8 hit |
| 8 | [08_agents_provenance](notebooks/08_agents_provenance.ipynb) | [nb08_agents](screenshots/nb08_agents.png), [nb08_provenance](screenshots/nb08_provenance.png) | 2 partition `agent_version`; replay 1 578 bước khớp; 5 lượt → 1 catalog read; 4 bucket + UNCLASSIFIED |

Mỗi notebook có cell **"📊 Phân tích kết quả"** giải thích số liệu và cơ chế, đặt ngay trước cell pass criteria.

Ảnh trong `screenshots/` được render tự động từ output của các notebook đã thực thi trong
`submission/notebooks/` (script matplotlib, không chỉnh sửa số liệu).

## Thay đổi so với mã đề bài

- **NB1:** cờ "schema enforcement blocked bad write" trước đây hardcode `True`; nay tính từ việc
  write thực sự raise **và** version bảng không tăng. Thêm cell in danh sách `_delta_log/` và các action
  trong commit JSON.
- **NB4:** đặt `SET TimeZone = 'UTC'` cho DuckDB — trước đó `CAST(ts AS DATE)` dùng giờ máy (UTC+7) nên Gold
  ra 8 ngày lệch biên; thêm assert chất lượng Gold (đủ cặp ngày×model, p50≤p95, cost>0, error_rate∈[0,1]) và
  bảng tổng hợp theo model.
- **NB6:** sửa ba lỗi hiển thị: `du()` nhận đường dẫn tương đối nên vacuum báo "0 B"; `count_files` đếm cả
  checkpoint tự động trong `_delta_log/` nên báo 5 "orphan" thay vì 3; dòng "Checkpoint written" in checkpoint
  v99 thay vì checkpoint mới v203. Assert checkpoint nay kiểm tra đúng version hiện tại.

## Bonus

[bonus/ARCHITECTURE.md](bonus/ARCHITECTURE.md) — Topic A (LLM observability 1B req/ngày), kèm PoC
[bonus/poc/pii_retention_poc.py](bonus/poc/pii_retention_poc.py) chứng minh tokenize PII trước khi vào
Bronze và retention 7 ngày được thi hành vật lý (DELETE + VACUUM), Gold vẫn giữ nguyên.
