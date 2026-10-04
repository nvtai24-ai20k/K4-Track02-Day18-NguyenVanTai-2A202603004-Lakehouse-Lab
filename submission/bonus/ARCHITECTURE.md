# Bonus — LLM Observability Lakehouse ở quy mô 1B requests/ngày (Topic A)

**Tác giả:** Nguyễn Văn Tài · 2A202603004 · K4-Track02-Day18
**PoC:** [`poc/pii_retention_poc.py`](poc/pii_retention_poc.py) — chạy từ gốc repo:
`.venv/Scripts/python.exe submission/bonus/poc/pii_retention_poc.py`

---

## 1. Problem statement

Một nhóm API foundation-model log mọi request/response: **1B request/ngày × ~5 KB = 5 TB/ngày
raw** (≈ 58 MB/s trung bình, giả định peak 3× ≈ 174 MB/s). Yêu cầu:
(1) dashboard cost & latency **theo tenant, refresh mỗi 5 phút**;
(2) prompt/response đầy đủ giữ **7 ngày** cho incident review, sau đó **chỉ giữ aggregates 1 năm**;
(3) **PII phải được redact trước khi bất kỳ ai đọc**;
(4) tổng chi phí **storage ≤ $5K/tháng**.

Vì sao khó: giữ raw 1 năm tốn ~$42K/tháng (§5), nên **retention phải là cơ chế xóa vật lý**,
không phải "ẩn khỏi bảng". Time travel, S3 versioning và backup đều giữ lại bản cũ, nên
"đã DELETE" chưa có nghĩa là "đã biến mất". Hai yêu cầu "redact trước khi đọc" và
"incident review cần ngữ cảnh" kéo về hai phía ngược nhau. Còn percentile (p95) thì
**không cộng dồn được**, nên không thể lấy p95 theo ngày bằng cách gộp p95 của từng 5 phút.

## 2. Architecture

```
  API gateways (1B req/day)
        │  JSON events
        ▼
  Kafka (12 h retention, RF=3)  ── canary PII event injected every minute ──┐
        │                                                                   │
        ▼                                                                   │
  Stream processor (Flink/Spark SS)                                         │
   ① PII TOKENIZE  regex+Luhn → HMAC-SHA256(key from KMS) → <EMAIL:ab12…>   │
      raw value never written; token→ciphertext goes to ── PII VAULT ───────┤
                                                       (restricted, audited)│
        │  60 s micro-batch commits (ACID, exactly-once via txn id)         │
        ▼                                                                   │
┌──────────────────────── Object storage (S3, versioning OFF) ──────────────┼─┐
│ BRONZE  delta: bronze.llm_calls   partition date(UTC)                     │ │
│   tokenized prompt/response + raw JSON string (schema-on-read)            │ │
│   ② RETENTION: DELETE date < today-6  → +24 h →  VACUUM (physical)        │ │
│      guard: expected rows ≈ 1/7 table ±20 %; RESTORE window = 24 h        │ │
│         │ parse + dedup by request_id (MERGE, late ≤ 2 h)                 │ │
│         ▼                                                                 │ │
│ SILVER  delta: silver.llm_calls  typed metrics only, NO text              │ │
│   ③ Z-ORDER (tenant_id, model) hourly; schema enforcement on write        │ │
│   retention 7 d (same job)        │ Change Data Feed                      │ │
│                                   ▼                                       │ │
│ GOLD    delta: gold.tenant_5min   (date, bucket, tenant, model)           │ │
│   count, tokens, cost_usd, errors, ④ t-digest sketch → p50/p95 any range  │ │
│   retention 400 d; rollups gold.tenant_daily                              │ │
│ maintenance: hourly compaction 256 MB · checkpoint/100 commits · orphan   │ │
│              sweep (age ≥ 24 h) · metric: avg file size, checkpoint age   │◀┘
└───────────────────────────────────────────────────────────────────────────┘
        │ ⑤ CATALOG (Glue/Unity): table-level + column-level grants, audit log
        ▼
  Trino (3 nodes) ── dashboards (Gold, every 5 min) · incident review (Bronze, audited)
```

Các concept Day18 được áp dụng: **medallion** (Bronze có text, Silver chỉ metric, Gold là aggregate),
**ACID + time travel/RESTORE** (cửa sổ rollback 24 h trước VACUUM), **Z-order/file skipping** cho
hot path `WHERE tenant_id = ?`, **maintenance 5 job** (NB6), **CDF** cho Gold incremental,
**catalog làm control plane** cho phân quyền, **FinOps lifecycle**, và **tokenization ngay khi landing**.

## 3. Các quyết định chính và alternatives đã loại

**D1 — Table format: chọn Delta Lake.**
Lý do: cần commit nguyên tử cho micro-batch 60 s (exactly-once qua `txnAppId/version`), cần
**Change Data Feed** để Gold chỉ xử lý phần Silver vừa thay đổi (bao gồm MERGE do late data),
và cần `DELETE` + `VACUUM` + `RESTORE` đã được kiểm chứng trong NB3/NB6.
- *Loại Iceberg:* field-ID và partition evolution rất tốt (NB5), nhưng consumer ở đây chỉ có
  Spark/Flink + Trino, không cần trung lập đa engine. Incremental read của Iceberg mặc định
  chỉ thấy append; thấy update/delete từ MERGE cần changelog view qua Spark procedure.
  Sẽ xem lại nếu thêm Snowflake/DuckDB làm consumer chính.
- *Loại Parquet + Hive partition:* không có log nên không commit nguyên tử. Reader có thể thấy
  batch ghi dở. Retention bằng cách xóa thư mục không có rollback, và không có stats cho file skipping.
- *Loại ClickHouse làm system-of-record:* dashboard rất nhanh, nhưng giữ ~11 TB text trên
  SSD/EBS ($80/TB-tháng gp3, ×3 replica) đắt gấp ~10 lần S3, và gắn chặt vào một engine.
  Có thể thêm sau làm cache phục vụ Gold.

**D2 — Redact PII ở đâu: chọn tokenize trong stream processor, trước khi ghi Bronze.**
Dùng `HMAC-SHA256(key_tenant, value)` để token mang tính tất định: cùng email cho cùng token,
nên incident reviewer vẫn liên kết được các request. Bảng vault (token → ciphertext) chỉ privacy
officer truy cập được, và mọi lần truy cập đều được audit.
- *Loại "Bronze giữ raw, redact ở Silver":* raw PII nằm trên S3 7 ngày, nằm trong time-travel
  history và mọi bản sao. Ai có quyền đọc bucket là đọc được PII, vi phạm yêu cầu (3).
- *Loại masking lúc query (view/row-filter):* chỉ an toàn nếu **mọi** đường đọc đi qua view.
  Đọc file trực tiếp (Spark path-based, DuckDB, notebook) sẽ bỏ qua view. Mỗi engine mới
  lại là một lỗ hổng tiềm năng.
- *Chi phí phải trả:* regex trên text tốn CPU. PoC đo được **6.8 MB/s/core** với `re` của
  Python (đo ngày 04/10/2026 trên laptop của tác giả), nên peak 174 MB/s cần ~26 core (§5).
  Regex cũng không bắt được tên người, xem F1.

**D3 — Partition & layout: chọn `date(UTC)` + Z-order `(tenant_id, model)`.**
Partition theo ngày là đúng **đơn vị của retention**: xóa một partition chỉ cần metadata, không
phải rewrite file. Z-order làm min/max của `tenant_id` gần như không chồng lấn; NB6 đo được
skip 90% file cho truy vấn điểm, NB2 đo được pruning 55×. Ngày luôn tính theo UTC: NB4 cho thấy
`CAST(ts AS DATE)` theo giờ máy +07 làm lệch biên ngày.
- *Loại partition theo `tenant_id`:* giả sử 5 000 tenant × 1 440 commit/ngày thì mỗi micro-batch
  tạo hàng nghìn file tí hon. Tenant lớn và tenant nhỏ lệch nhau 1000×. Retention phải quét qua
  5 000 thư mục.
- *Loại partition theo giờ:* 24× số partition và file nhỏ hơn 24×, trong khi không có truy vấn
  nào cần lọc theo giờ chính xác hơn mức stats `ts` đã cung cấp.

**D4 — Thi hành retention: chọn `DELETE date < today-6` → chờ 24 h → `VACUUM`, bucket tắt versioning.**
PoC chứng minh: sau DELETE, cả 4 000 dòng hết hạn vẫn đọc được qua time travel. Chỉ sau VACUUM,
số `request_id` hết hạn còn trên đĩa mới bằng 0 và đọc version cũ trả `FileNotFoundError`.
Khoảng chờ 24 h chính là cửa sổ để RESTORE (F2).
- *Loại S3 Lifecycle rule xóa theo prefix:* xóa object sau lưng transaction log. Log vẫn
  tham chiếu các file đã mất, nên query và time travel lỗi `FileNotFound`. Delta cũng không
  biết bảng đã nhỏ đi. (Đây là bài học orphan của NB6, theo chiều ngược lại.)
- *Loại chuyển raw sang Glacier thay vì xóa:* vi phạm yêu cầu "chỉ giữ aggregates" và tốn
  ~$1.8K/tháng chỉ cho 456 TB ở Glacier IR (§5), trong khi text sau 7 ngày không còn ai đọc.
- *Bắt buộc:* tắt S3 versioning hoặc đặt `NoncurrentVersionExpiration = 1 ngày`. Nếu không,
  object do VACUUM xóa vẫn còn dưới dạng noncurrent version, vẫn tính tiền và vẫn chứa dữ liệu.

**D5 — Percentile ở Gold: chọn lưu t-digest sketch cho mỗi bucket 5 phút.**
Sketch merge được, nên p95 cho 1 giờ, 1 ngày hay 1 tháng đều tính từ Gold mà không cần quay lại
Silver (Silver đã bị xóa sau 7 ngày). Mỗi sketch khoảng 400 B nén.
- *Loại chỉ lưu p50/p95 từng bucket:* trung bình các p95 **không phải** p95. Sau 7 ngày không thể
  tính lại p95 theo tháng cho báo cáo SLA.
- *Loại tính lại từ Silver mỗi 5 phút:* chi phí quét 60 GB/ngày × 288 lần/ngày, và không làm được
  với dữ liệu cũ hơn 7 ngày.

**D6 — Compression & kích thước file: chọn zstd(3), commit mỗi 60 s, compaction hàng giờ lên 256 MB.**
- *Loại snappy:* nhanh hơn nhưng với text thường lớn hơn zstd khoảng 25–40%. Phần lớn
  storage là text, nên chênh lệch đó ăn trực tiếp vào ngân sách.
- *Loại commit mỗi 5 s và không compaction:* 17 280 commit/ngày, file chỉ vài trăm KB. NB6 đo
  được: 200 file 51 KB thì metadata/log phình ra và reader lạnh phải replay hàng trăm JSON.
  Dashboard 5 phút sẽ chậm dần theo ngày.

**D7 — Catalog & quyền truy cập: chọn catalog (Glue/Unity OSS) với grant theo bảng/cột, audit mọi lần đọc Bronze.**
- *Loại phân quyền bằng IAM theo path S3:* không phân quyền được theo cột và không có audit
  ở mức bảng. Ai có quyền đọc prefix là đọc được toàn bộ Bronze.
- *Loại Hive Metastore tự vận hành:* không có grant theo cột, không có REST spec, và tốn công vận hành.

## 4. Failure modes (kịch bản 3 giờ sáng)

| # | Sự cố | Phát hiện | Rollback / khắc phục |
|---|---|---|---|
| **F1** | Regex bỏ sót một dạng PII mới (CCCD có dấu cách, tên người, số hộ chiếu) → raw PII vào Bronze | (a) Canary: mỗi phút bơm 1 event chứa PII tổng hợp; job quét Bronze, nếu tìm thấy chuỗi canary thì page on-call. (b) Hằng ngày quét mẫu 0.1% Bronze bằng NER, alert khi tỷ lệ > 0 | Dừng consumer cho tenant bị ảnh hưởng. Thêm pattern, rồi `UPDATE`/`MERGE` tokenize lại các partition liên quan. **Phải VACUUM ngay các file cũ** (cần phê duyệt), vì time travel vẫn giữ bản chứa PII (PoC bước [2]). Gold không bị ảnh hưởng vì không chứa text |
| **F2** | Job retention tính sai cutoff (lỗi múi giờ như NB4, lỗi off-by-one) → xóa cả dữ liệu hôm nay | Guard trước commit: số dòng bị xóa phải ≈ 1/7 bảng ± 20%, nếu không thì abort (PoC có assert này). Monitor số dòng Silver/Gold so với baseline | `RESTORE` về version trước DELETE: chỉ là metadata, mất vài giây (NB3: 0.02 s). Làm được vì VACUUM chạy sau ≥ 24 h. **Concept: time travel** |
| **F3** | SDK upstream đổi `latency_ms` từ int sang chuỗi `"812ms"` → Silver parse ra null, dashboard sai | Schema enforcement chặn batch không tương thích (NB1). Monitor tỷ lệ null theo cột > 1%. Dòng parse lỗi đi vào bảng quarantine | `RESTORE` Silver về version trước deploy lỗi, sửa parser, rồi chạy lại từ Bronze: Bronze giữ raw JSON dạng string (schema-on-read) nên không mất gì. Gold tự sửa qua CDF. **Concept: schema enforcement/evolution** |
| **F4** | Job compaction chết âm thầm → mỗi ngày thêm ~46K file nhỏ (32 writer × 1 440 commit), Trino plan chậm, dashboard trễ quá 5 phút | Metric kích thước file trung bình < 64 MB, số file/partition, checkpoint quá 2 giờ tuổi | Chạy lại OPTIMIZE (idempotent) và tạo checkpoint. Cuối cùng VACUUM để thu hồi tombstone (NB6 Job 1/3/5) |
| **F5** | Event đến muộn (> 2 h) bị bỏ, Gold thiếu số | Đối soát hằng ngày: `count(Silver) = sum(Gold.count)` theo (date, tenant) | MERGE lại bucket của ngày đó từ Silver, vẫn trong cửa sổ 7 ngày |

## 5. Chi phí back-of-envelope (giá list AWS us-east-1, on-demand)

Giả định: tỷ lệ nén zstd thực tế **4×** cho log text + JSON. PoC đo được 5.6× trên text tổng hợp,
nhưng con số đó đánh giá quá cao vì từ vựng lặp lại. S3 Standard $23/TB-tháng.

**Storage**

| Thành phần | Phép tính | TB lưu | $/tháng |
|---|---|---:|---:|
| Bronze (text đã tokenize) | 5 TB/ngày ÷ 4 = 1.25 TB/ngày × (7 ngày + 1 ngày chờ VACUUM + 1 ngày compaction trùng) | 11.25 | 259 |
| Silver (chỉ metric) | ~60 B/req nén × 1B = 60 GB/ngày × 9 ngày | 0.54 | 12 |
| Gold 5 phút + sketch | ~8K (tenant, model) đang hoạt động × 288 bucket × 400 B = 0.92 GB/ngày × 400 ngày | 0.37 | 9 |
| Kafka buffer (EBS gp3 $80/TB) | 5 TB × 0.5 ngày ÷ 2 (lz4) × RF 3 | 3.75 | 300 |
| Request S3 (PUT/GET) | ~3M PUT × $0.005/1K + GET dashboard | — | 20 |
| **Tổng storage** | | | **≈ $600** (12% của cap $5K) |

So sánh để thấy vì sao lifecycle là quyết định quan trọng nhất:
- giữ raw JSON 1 năm: 5 TB × 365 × $23 = **$42K/tháng** (8.4× cap);
- giữ text đã nén 1 năm: 1.25 × 365 = 456 TB × $23 = **$10.5K** (2.1× cap);
- chuyển sang Glacier IR ($4/TB): 456 × 4 = **$1.8K**, nhưng vẫn vi phạm yêu cầu (2).

Độ nhạy: nếu nén chỉ đạt 2.5×, Bronze = 2 TB × 9 = 18 TB → $414, tổng ≈ $760.
Cap $5K chỉ bị chạm khi phần retain vượt ~200 TB, tức retention text dài khoảng 160 ngày.
Vậy ngân sách còn đủ để mở rộng retention lên 30 ngày (1.25 × 32 = 40 TB → $920 + $341 phần còn lại ≈ $1.3K) nếu legal yêu cầu.

**Compute** (không nằm trong cap storage, nhưng tính để thấy toàn cảnh; m7g.2xlarge 8 vCPU $0.3264/h × 730 h ≈ $238/node-tháng)

| Thành phần | Phép tính | $/tháng |
|---|---|---:|
| Tokenize + ingest | peak 174 MB/s ÷ 6.8 MB/s/core ≈ 26 core → 4 node (đo bằng Python `re`; engine biên dịch sẽ nhanh hơn, chưa đo) | 952 |
| Silver/Gold streaming | 2 node | 476 |
| Compaction / Z-order / VACUUM | ghi lại 1.25 TB/ngày, ~2 node-giờ/ngày × 30 × $0.33 | 20 |
| Trino phục vụ dashboard | 3 node luôn chạy | 714 |
| Kafka brokers | 3 × kafka.m7g.large ≈ $0.204/h × 730 | 447 |
| **Tổng compute** | | **≈ $2.6K** |

## 6. MVP một tuần

**Slice:** pipeline đầu-cuối ở **1/1000 quy mô** (1M req/ngày ≈ 12 req/s, 5 GB raw/ngày) trên một máy:
generator (có canary PII) → tokenizer → Bronze Delta → Silver (dedup MERGE) → Gold 5 phút có
t-digest → một dashboard Trino/DuckDB theo tenant. Chạy liên tục 24 h, sau đó giả lập 8 ngày dữ liệu.

| # | Tiêu chí nghiệm thu | Cách kiểm tra |
|---|---|---|
| 1 | **0** chuỗi canary trong mọi cột string của mọi file Bronze | Quét toàn bộ file (như PoC bước [1]) |
| 2 | Gold mới hơn 5 phút trong ≥ 95% số phút | `now() - max(bucket_end)` lấy mẫu mỗi phút |
| 3 | Ngày thứ 8: `request_id` hết hạn **không còn trên đĩa**, time travel về version cũ lỗi, số ngày trong Gold không đổi | PoC bước [2]–[4] |
| 4 | p95 theo ngày tính từ merge sketch lệch ≤ 1% so với p95 exact từ Silver | So sánh trên 7 ngày còn Silver |
| 5 | Diễn tập F2: cutoff sai bị guard chặn; RESTORE thủ công xong trong < 10 phút | Runbook + đo thời gian |
| 6 | Đo bytes/request của Bronze và Silver, ngoại suy chi phí; **fail nếu storage ngoại suy > $2.5K** | Lấy `du` thực tế × 1000 rồi áp bảng §5 |

**Cơ chế khó nhất đã được kiểm chứng bằng PoC** (`poc/pii_retention_poc.py`, 127 dòng, chạy
offline với dependencies của lab). Output thực tế:

```
[1] Bronze files scanned: 9, rows: 18,000, raw PII hits: 0
    sample prompt: Khach hang <CCCD:903fb05a529b> hoi ve don hang #19773
[2] DELETE date < 2026-04-04: current rows 14,000; expired rows still readable via time travel v8: 4,000
[3] After VACUUM: expired request_ids physically on disk: 0; time travel to v8 fails (FileNotFoundError)
[4] Gold still covers 9 days; Bronze keeps 7 days of text
```

Dòng [2] là lý do thiết kế không dừng ở DELETE: nếu thiếu VACUUM, "retention 7 ngày" chỉ là
lời hứa trên giao diện, còn dữ liệu vẫn nằm trên đĩa và vẫn tính tiền.

## Nguồn tham khảo

- Delta Lake protocol — tombstones, VACUUM, Change Data Feed: https://github.com/delta-io/delta/blob/master/PROTOCOL.md
- Apache Iceberg spec — incremental scans, partition evolution: https://iceberg.apache.org/spec/
- AWS S3 pricing và Lifecycle/Versioning (noncurrent versions): https://aws.amazon.com/s3/pricing/ · https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lifecycle-mgmt.html
- Dunning & Ertl, *Computing Extremely Accurate Quantiles Using t-Digests* (2019): https://arxiv.org/abs/1902.04023
- Kết quả đo trong lab này: NB2 (pruning 55×), NB3 (RESTORE), NB4 (lệch múi giờ), NB6 (orphan/VACUUM/checkpoint).
