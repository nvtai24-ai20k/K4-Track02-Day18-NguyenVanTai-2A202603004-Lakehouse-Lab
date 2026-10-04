# ---
# jupyter:
#   jupytext:
#     formats: py:percent
# ---

# %% [markdown]
# # NB1 — Delta Lake Basics (lightweight path)
#
# **Stack:** `deltalake` (delta-rs) + Polars + DuckDB. No Spark, no JVM.
# Maps to slide §2 (Delta Lake) + deliverable bullet 1.
#
# > Spark equivalent: `spark.read.format("delta").load(path)` ↔ `DeltaTable(path).to_pyarrow_table()`.
# > Same on-disk format, different binding.

# %%
import _setup  # noqa: F401  -- adds scripts/ to sys.path (file-relative)
import polars as pl
from deltalake import DeltaTable, write_deltalake
from lakehouse import path, reset

table_path = path("scratch", "users_delta")
reset(table_path)  # idempotent rerun

# %% [markdown]
# ## 1. Write a Delta table

# %%
df = pl.DataFrame({
    "id": [1, 2, 3],
    "name": ["alice", "bob", "charlie"],
    "age": [30, 25, 35],
    "city": ["Hanoi", "HCMC", "Danang"],
})
write_deltalake(table_path, df.to_arrow(), mode="overwrite")

# %% [markdown]
# ## 2. Read it back + inspect transaction log
#
# Look at `_lakehouse/scratch/users_delta/_delta_log/00000000000000000000.json` —
# that's the transaction log. Same JSON format Spark/Databricks would write.

# %%
dt = DeltaTable(table_path)
print(pl.from_arrow(dt.to_pyarrow_table()))
print("\nHistory:")
for h in dt.history():
    print(f"  v{h['version']}  {h['operation']}  {h.get('operationMetrics', {})}")

# %% [markdown]
# ## 3. Schema enforcement — try to write a wrong schema

# %%
bad = pl.DataFrame({"id": [4], "name": ["dan"], "age": ["thirty"], "city": ["Hue"]})
version_before_bad = DeltaTable(table_path).version()
try:
    write_deltalake(table_path, bad.to_arrow(), mode="append")
    bad_write_blocked = False
    print("UNEXPECTED: bad write succeeded — schema enforcement broken")
except Exception as e:
    bad_write_blocked = True
    msg = str(e).splitlines()[0][:120]
    print(f"BLOCKED by schema enforcement (expected): {type(e).__name__}: {msg}")

# The rejected write must not have produced a commit.
version_after_bad = DeltaTable(table_path).version()
print(f"Table version before/after bad write: {version_before_bad} → {version_after_bad}")
bad_write_blocked = bad_write_blocked and version_after_bad == version_before_bad

# %% [markdown]
# ## 4. Schema evolution (opt-in)

# %%
new = pl.DataFrame({
    "id": [4], "name": ["dan"], "age": [28], "city": ["Hue"], "tier": ["premium"],
})
write_deltalake(table_path, new.to_arrow(), mode="append", schema_mode="merge")
dt = DeltaTable(table_path)
# Sort by id so the printout is stable across reruns — Delta does not
# preserve write-order across appends.
print(pl.from_arrow(dt.to_pyarrow_table()).sort("id"))

# %% [markdown]
# ## 4b. Evidence: the transaction log on disk
#
# Each successful write is one JSON commit. The rejected `age='thirty'` write
# left no file behind — that is what "atomic" means.

# %%
import json
from pathlib import Path

log_dir = Path(table_path) / "_delta_log"
for f in sorted(log_dir.glob("*.json")):
    print(f"{f.name}  ({f.stat().st_size} B)")

print("\nActions in the schema-evolution commit (00000000000000000001.json):")
for line in (log_dir / "00000000000000000001.json").read_text().splitlines():
    action = json.loads(line)
    kind = next(iter(action))
    if kind == "metaData":
        fields = json.loads(action["metaData"]["schemaString"])["fields"]
        print(f"  metaData  schema = {[(c['name'], c['type']) for c in fields]}")
    elif kind == "add":
        a = action["add"]
        print(f"  add       path={a['path'][:40]}…  size={a['size']}  stats={a['stats'][:70]}…")
    else:
        print(f"  {kind:<9} {json.dumps(action[kind])[:90]}")

# %% [markdown]
# ## 5. Query with DuckDB via Arrow (part of the required notebook)

# %%
import duckdb

# We hand DuckDB an Arrow table rather than calling `delta_scan()`. delta_scan
# autoloads a DuckDB extension over the network — fine at home, a support
# ticket in a firewalled classroom. Arrow registration is zero-copy and offline.
con = duckdb.connect()
con.register("users", DeltaTable(table_path).to_pyarrow_table())
tier_counts = con.sql("SELECT tier, count(*) AS n FROM users GROUP BY 1 ORDER BY 1").fetchall()
print(tier_counts)

# %% [markdown]
# ## 📊 Phân tích kết quả (NB1)
#
# **Transaction log.** `_delta_log/` có đúng 2 commit JSON: `…000.json` (WRITE 3 dòng) và
# `…001.json` (append có `schema_mode="merge"`). Commit thứ hai chứa 3 action: `commitInfo`
# (ai/khi nào/thao tác gì), `metaData` (schema mới đã có `tier`) và `add` (đường dẫn file
# Parquet + `stats` min/max/nullCount). Bảng Delta = file Parquet + log này; reader chỉ coi
# file là "thuộc bảng" khi nó được `add` trong log. Các `stats` min/max chính là thứ NB2/NB6
# dùng để bỏ qua file.
#
# **Schema enforcement.** Ghi `age='thirty'` bị chặn với lỗi
# `Cast error: Cannot cast string 'thirty' to value of Int64 type`, và version của bảng vẫn
# **0 → 0**: không có commit nào được tạo, nên reader không bao giờ thấy dữ liệu sai kiểu.
# Cờ PASS ở cuối được tính từ hai điều kiện này (có exception **và** version không tăng),
# không còn hardcode.
#
# **Schema evolution là opt-in.** Chỉ khi truyền `schema_mode="merge"` thì cột `tier` mới
# được thêm. 3 dòng cũ đọc ra `tier = null` mà không phải ghi lại file cũ — thay đổi schema
# chỉ là một action `metaData` mới. DuckDB thấy 2 nhóm: `premium` (1) và `NULL` (3).
# Ý nghĩa: upstream đổi kiểu dữ liệu sẽ làm pipeline **fail sớm** thay vì âm thầm làm hỏng
# bảng; còn thêm cột là quyết định có chủ đích của người sở hữu bảng.

# %% [markdown]
# ## ✅ Deliverable check
# - [ ] `_delta_log/` contains JSON files
# - [ ] Schema enforcement blocked the bad write
# - [ ] schema_mode="merge" added the `tier` column
# - [ ] DuckDB query returned 2 tier groups
# The schema-enforcement flag is computed from the bad-write cell: the write
# must raise *and* the table version must not advance.

# %%
from pathlib import Path as _Path  # noqa: E402

_log = sorted(_Path(table_path).glob("_delta_log/*.json"))
_cols = DeltaTable(table_path).schema().to_arrow().names
checks = {
    "_delta_log/ has JSON commits": len(_log) >= 2,
    "schema enforcement blocked bad write (no new commit)": bad_write_blocked,
    "tier column added via schema_mode=merge": "tier" in _cols,
    "duckdb sees 2 tier groups": len(tier_counts) == 2,
}
for k, v in checks.items():
    print(f"  [{'PASS' if v else 'FAIL'}] {k}")
assert all(checks.values()), "NB1 incomplete — see FAIL rows above"
print("\nNB1 complete.")
