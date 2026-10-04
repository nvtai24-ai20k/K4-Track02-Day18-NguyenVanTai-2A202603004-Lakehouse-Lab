"""PoC — the two hardest mechanisms of the Topic A design (LLM observability).

1. PII is tokenized *before* the first byte lands in Bronze (HMAC-SHA256, keyed),
   so no reader — not even one with raw bucket access — ever sees raw PII.
2. The 7-day retention of prompt/response text is enforced *physically*:
   DELETE alone leaves old files reachable by time travel (NB6/NB8 lesson);
   only DELETE + VACUUM makes the text unrecoverable, while Gold aggregates survive.

Run from the repo root:  .venv/Scripts/python.exe submission/bonus/poc/pii_retention_poc.py
"""
from __future__ import annotations

import datetime as dt
import hashlib
import hmac
import random
import re
import shutil
from pathlib import Path

import polars as pl
import pyarrow.parquet as pq
from deltalake import DeltaTable, write_deltalake

ROOT = Path(__file__).resolve().parents[3] / "_lakehouse" / "bonus_poc"
BRONZE, GOLD = ROOT / "bronze_llm_calls", ROOT / "gold_tenant_daily"
TOKEN_KEY = b"demo-key-from-kms"   # production: per-tenant data key fetched from KMS
DAYS, REQ_PER_DAY, RETENTION_DAYS = 9, 2_000, 7
TODAY = dt.date(2026, 4, 10)

# --- 1. Detection + tokenization -------------------------------------------
PII_PATTERNS = {
    "EMAIL": re.compile(r"[\w.+-]+@[\w-]+\.[\w.]+"),
    "PHONE": re.compile(r"(?<!\d)(?:\+84|0)(?:3|5|7|8|9)\d{8}(?!\d)"),   # VN mobile
    "CCCD":  re.compile(r"(?<!\d)0\d{11}(?!\d)"),                        # VN citizen ID, 12 digits
    "CARD":  re.compile(r"(?<!\d)(?:\d{4}[ -]?){3}\d{4}(?!\d)"),
}


def tokenize(text: str) -> str:
    """Replace each PII match with a deterministic token: same value → same token,
    so incident reviewers can still correlate requests without seeing the value."""
    for kind, pat in PII_PATTERNS.items():
        text = pat.sub(lambda m, k=kind: f"<{k}:{hmac.new(TOKEN_KEY, m.group().encode(), hashlib.sha256).hexdigest()[:12]}>", text)
    return text


# --- 2. Synthetic traffic with planted PII canaries --------------------------
random.seed(7)
TENANTS, MODELS = [f"tenant_{i:03d}" for i in range(20)], ["haiku", "sonnet", "opus"]
CANARIES = ["an.nguyen@example.vn", "0912345678", "001203004567", "4111 1111 1111 1111"]


def make_day(day: dt.date) -> pl.DataFrame:
    rows = []
    for i in range(REQ_PER_DAY):
        pii = random.choice(CANARIES)
        prompt = f"Khach hang {pii} hoi ve don hang #{random.randint(1, 99999)}"
        rows.append({
            "date": day, "request_id": f"{day:%Y%m%d}-{i:06d}",
            "tenant_id": random.choice(TENANTS), "model": random.choice(MODELS),
            "latency_ms": int(random.lognormvariate(6.5, 0.5)),
            "prompt": tokenize(prompt),                       # redaction happens HERE, pre-landing
            "response": tokenize(f"Da gui xac nhan toi {pii}."),
        })
    return pl.DataFrame(rows)


shutil.rmtree(ROOT, ignore_errors=True)
for d in range(DAYS):
    day = TODAY - dt.timedelta(days=DAYS - 1 - d)
    write_deltalake(str(BRONZE), make_day(day).to_arrow(), mode="append", partition_by=["date"])

# --- 3. Verify: no canary appears in ANY string value of ANY Bronze file -----
files = [p for p in BRONZE.rglob("*.parquet") if "_delta_log" not in p.parts]
leaks = 0
for f in files:
    t = pq.read_table(f)
    for col in ("prompt", "response", "tenant_id"):
        blob = "\n".join(v for v in t.column(col).to_pylist() if v)
        leaks += sum(blob.count(c) for c in CANARIES)
print(f"[1] Bronze files scanned: {len(files)}, rows: {DeltaTable(str(BRONZE)).count():,}, raw PII hits: {leaks}")
print(f"    sample prompt: {pq.read_table(files[0]).column('prompt')[0]}")
assert leaks == 0, "PII reached Bronze"

# --- 4. Gold aggregates (kept 1 year) ---------------------------------------
bronze_df = pl.from_arrow(DeltaTable(str(BRONZE)).to_pyarrow_table())
gold = (bronze_df.group_by("date", "tenant_id", "model")
        .agg(pl.len().alias("requests"),
             pl.col("latency_ms").quantile(0.5).alias("p50_ms"),
             pl.col("latency_ms").quantile(0.95).alias("p95_ms")))
write_deltalake(str(GOLD), gold.to_arrow(), mode="overwrite", partition_by=["date"])

# --- 5. Retention: DELETE is logical, VACUUM is physical ---------------------
cutoff = TODAY - dt.timedelta(days=RETENTION_DAYS - 1)      # keep the last 7 days incl. today
bt = DeltaTable(str(BRONZE))
v_before = bt.version()
expected = bronze_df.filter(pl.col("date") < cutoff).height
# Guard against a bad cutoff (e.g. timezone bug): refuse to delete outside the expected band.
assert 0 < expected <= bronze_df.height * 3 / DAYS, f"retention guard tripped: {expected} rows"
bt.delete(f"date < '{cutoff.isoformat()}'")
bt = DeltaTable(str(BRONZE))
old_ids = set(pl.from_arrow(DeltaTable(str(BRONZE), version=v_before).to_pyarrow_table())
              .filter(pl.col("date") < cutoff)["request_id"])
print(f"[2] DELETE date < {cutoff}: current rows {bt.count():,}; expired rows still readable "
      f"via time travel v{v_before}: {len(old_ids):,}")
assert len(old_ids) == expected

bt.vacuum(retention_hours=0, enforce_retention_duration=False, dry_run=False)  # prod: ≥ 24 h gap
on_disk_ids: set[str] = set()
for f in BRONZE.rglob("*.parquet"):
    if "_delta_log" not in f.parts:
        on_disk_ids |= set(pq.read_table(f, columns=["request_id"]).column("request_id").to_pylist())
try:
    DeltaTable(str(BRONZE), version=v_before).to_pyarrow_table()
    tt = "still readable"
except Exception as e:  # noqa: BLE001
    tt = f"fails ({type(e).__name__})"
print(f"[3] After VACUUM: expired request_ids physically on disk: {len(old_ids & on_disk_ids)}; "
      f"time travel to v{v_before} {tt}")
assert not (old_ids & on_disk_ids)

gold_dates = pl.from_arrow(DeltaTable(str(GOLD)).to_pyarrow_table())["date"].n_unique()
print(f"[4] Gold still covers {gold_dates} days; Bronze keeps "
      f"{len(bt.to_pyarrow_table().column('date').unique())} days of text")
assert gold_dates == DAYS
print("\nPoC passed: tokenized at landing, retention enforced physically, aggregates preserved.")
