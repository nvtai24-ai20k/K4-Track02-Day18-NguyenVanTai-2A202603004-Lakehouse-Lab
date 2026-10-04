# ---
# jupyter:
#   jupytext:
#     formats: py:percent
# ---

# %% [markdown]
# # NB4 — Medallion Pipeline (Bronze → Silver → Gold), lightweight
#
# **Use case:** LLM observability — exact schema from slide §8 (Lakehouse cho AI/ML) medallion frame.
# Maps to deliverable bullet 4 (the Milestone-1 Lakehouse artifact).
#
# Pre-req: ran `make data` — but if you jumped straight here, the cell below
# generates the Bronze sample for you rather than failing on a missing path.

# %%
import _setup  # noqa: F401  -- adds scripts/ to sys.path
from pathlib import Path

import polars as pl
import duckdb
from deltalake import DeltaTable, write_deltalake
from lakehouse import path, reset

BRONZE = path("bronze", "llm_calls_raw")
SILVER = path("silver", "llm_calls")
GOLD   = path("gold",   "llm_daily_metrics")

# Self-healing pre-req (same pattern as NB7/NB8). Without this, skipping
# `make data` surfaces as a raw `Os { code: 2, kind: NotFound }` from the Rust
# layer — technically correct, useless to a student.
if not Path(BRONZE).exists():
    print("Bronze not found — running scripts/generate_data_lite.py first ...")
    import generate_data_lite

    generate_data_lite.main()

# %% [markdown]
# ## Bronze — verify raw is loaded

# %%
bronze_n = DeltaTable(BRONZE).to_pyarrow_table().num_rows
print(f"Bronze rows: {bronze_n:,}")
print(pl.from_arrow(DeltaTable(BRONZE).to_pyarrow_table().slice(0, 2)))

# %% [markdown]
# ## Silver — parse, validate, dedup
#
# Rules: drop malformed JSON, dedupe by `request_id`, project typed columns.

# %%
reset(SILVER)

# DuckDB does the JSON parse + dedup in one query — Polars also works,
# DuckDB just has nicer JSON syntax for this case.
# DuckDB reads Delta through Arrow, not through `delta_scan()`. The latter
# autoloads an extension over the network; Arrow registration is offline and
# zero-copy, so the lab works on a locked-down machine.
con = duckdb.connect()
# `ts` is TIMESTAMPTZ; CAST(ts AS DATE) uses the session time zone, which
# defaults to the laptop's local zone (UTC+7 here). Pin UTC so `date` means the
# same UTC day the generator wrote, on every machine.
con.sql("SET TimeZone = 'UTC'")
con.register("bronze", DeltaTable(BRONZE).to_pyarrow_table())

silver_arrow = con.sql(f"""
    WITH parsed AS (
      SELECT
        request_id,
        ts,
        CAST(ts AS DATE)                            AS date,
        json_extract_string(raw_json, '$.model')          AS model,
        json_extract_string(raw_json, '$.user_id')        AS user_id,
        CAST(json_extract(raw_json, '$.usage.input')  AS INTEGER) AS prompt_tokens,
        CAST(json_extract(raw_json, '$.usage.output') AS INTEGER) AS completion_tokens,
        CAST(json_extract(raw_json, '$.latency_ms')   AS INTEGER) AS latency_ms,
        json_extract_string(raw_json, '$.status')         AS status,
        ROW_NUMBER() OVER (PARTITION BY request_id ORDER BY ts) AS rn
      FROM bronze
    )
    SELECT request_id, ts, date, model, user_id,
           prompt_tokens, completion_tokens, latency_ms, status
    FROM parsed
    WHERE rn = 1 AND model IS NOT NULL
""").arrow()

write_deltalake(SILVER, silver_arrow, mode="overwrite", partition_by=["date"])

silver_n = DeltaTable(SILVER).to_pyarrow_table().num_rows
print(f"Silver rows: {silver_n:,}  (Bronze {bronze_n:,} → dedup dropped {bronze_n - silver_n:,})")
assert silver_n < bronze_n, (
    "Silver has the same row count as Bronze — dedup did not run. "
    "Did you regenerate Bronze with the latest generator (which injects retries)?"
)

# %% [markdown]
# ## Gold — aggregate to (date, model) metrics

# %%
reset(GOLD)

# Illustrative cost model — NOT canonical pricing.
# (input USD / 1M tokens, output USD / 1M tokens)
COST_TABLE = """
  VALUES
    ('claude-haiku-4-5',  0.80,  4.00),
    ('claude-sonnet-4-6', 3.00, 15.00),
    ('claude-opus-4-7', 15.00, 75.00)
"""

con.register("silver", DeltaTable(SILVER).to_pyarrow_table())
gold_arrow = con.sql(f"""
    WITH cost(model, c_in, c_out) AS ({COST_TABLE})
    SELECT
      s.date,
      s.model,
      QUANTILE_CONT(s.latency_ms, 0.50) AS p50_latency_ms,
      QUANTILE_CONT(s.latency_ms, 0.95) AS p95_latency_ms,
      SUM(s.prompt_tokens)              AS total_prompt_tokens,
      SUM(s.completion_tokens)          AS total_completion_tokens,
      AVG(CASE WHEN s.status <> 'ok' THEN 1.0 ELSE 0.0 END) AS error_rate,
      (SUM(s.prompt_tokens)     * c.c_in  / 1e6) +
      (SUM(s.completion_tokens) * c.c_out / 1e6) AS cost_usd
    FROM silver s
    JOIN cost c USING (model)
    GROUP BY s.date, s.model, c.c_in, c.c_out
    ORDER BY s.date, s.model
""").arrow()

write_deltalake(GOLD, gold_arrow, mode="overwrite", partition_by=["date"])

# Z-order for fast filter-by-model dashboards
DeltaTable(GOLD).optimize.z_order(["model"])

# %% [markdown]
# ## Verify Gold

# %%
gold_df = pl.from_arrow(DeltaTable(GOLD).to_pyarrow_table()).sort("date", "model")
print(gold_df)

# Slide-5 deliverable: "Gold p50/p95/cost qua ≥ 7 ngày". Make that explicit.
n_dates = gold_df.select("date").n_unique()
n_models = gold_df.select("model").n_unique()
print(
    f"\n──── Gold deliverable metrics ────\n"
    f"  Distinct dates:   {n_dates:>3}   (target ≥ 7)\n"
    f"  Distinct models:  {n_models:>3}\n"
    f"  Total Gold rows:  {gold_df.height:>3}   (= dates × models)"
)
assert n_dates >= 7, (
    f"Gold has only {n_dates} dates — slide deliverable requires ≥ 7. "
    "Re-run `make data` (the generator spreads across 7 UTC days)."
)

# Gold quality checks — the rubric's "Gold correct" criterion, made explicit.
gold_checks = {
    "≥ 7 dates": n_dates >= 7,
    "≥ 3 models": n_models >= 3,
    "every (date, model) pair present": gold_df.height == n_dates * n_models,
    "p50 ≤ p95 on every row": gold_df.filter(pl.col("p50_latency_ms") > pl.col("p95_latency_ms")).height == 0,
    "cost_usd > 0 on every row": gold_df.filter(pl.col("cost_usd") <= 0).height == 0,
    "error_rate in [0, 1]": gold_df.filter(~pl.col("error_rate").is_between(0, 1)).height == 0,
    "error_rate non-zero somewhere": gold_df["error_rate"].sum() > 0,
}
for k, v in gold_checks.items():
    print(f"  [{'PASS' if v else 'FAIL'}] {k}")
assert all(gold_checks.values()), "Gold quality check failed — see FAIL rows above"

# Per-model summary across the week: the question a dashboard would ask.
print(
    gold_df.group_by("model")
    .agg(
        pl.col("p50_latency_ms").median().alias("p50_ms (median of days)"),
        pl.col("p95_latency_ms").median().alias("p95_ms (median of days)"),
        pl.col("error_rate").mean().alias("avg_error_rate"),
        pl.col("cost_usd").sum().alias("week_cost_usd"),
    )
    .sort("model")
)

# %% [markdown]
# ## 📊 Phân tích kết quả (NB4)
#
# **Ba tầng trên đĩa:** `_lakehouse/bronze/llm_calls_raw`, `_lakehouse/silver/llm_calls`
# (partition theo `date`), `_lakehouse/gold/llm_daily_metrics`.
#
# **Bronze → Silver.** Bronze có **200 000** dòng thô (JSON nguyên bản, kể cả request bị
# retry). Silver parse JSON thành cột có kiểu và giữ 1 dòng/`request_id`
# (`ROW_NUMBER() … rn = 1`) → **190 052** dòng; **9 948** bản ghi trùng bị loại, khớp đúng số
# duplicate mà generator cài vào. Nếu không dedup, mọi metric cost/latency ở Gold sẽ bị đếm
# trùng ~5%.
#
# **Silver → Gold.** Gold có **7 ngày × 3 model = 21 dòng**; các kiểm tra đã assert:
# p50 ≤ p95 ở mọi dòng, `cost_usd > 0`, `error_rate ∈ [0, 1]`. Đọc kết quả: opus chậm nhất
# (p50 ≈ 3 s, p95 ≈ 6 s) so với haiku (~0.56 s / ~1.1 s); error rate ~5% cho cả ba model.
# Sonnet tốn nhiều tiền nhất trong tuần dù đơn giá thấp hơn opus, vì lưu lượng token lớn hơn
# nhiều — Gold trả lời được câu hỏi "tiền đi đâu" mà Bronze không trả lời trực tiếp được.
# Giá chỉ là bảng minh họa của lab.
#
# **Lỗi đã sửa:** lần chạy đầu ra **8** ngày vì `CAST(ts AS DATE)` trên `TIMESTAMPTZ` dùng múi giờ
# của máy (UTC+7), làm các sự kiện cuối ngày UTC bị đẩy sang ngày hôm sau. Notebook đặt
# `SET TimeZone = 'UTC'` để `date` là ngày UTC như generator ghi, ổn định trên mọi máy.

# %% [markdown]
# ## ✅ Deliverable check
# - [ ] All three tables exist under `_lakehouse/{bronze,silver,gold}/`
# - [ ] Silver has fewer rows than Bronze (dedup worked)
# - [ ] Gold spans ≥ 7 dates × 3 models (slide §8 medallion contract)
# - [ ] Cost & error_rate columns populated and non-zero
