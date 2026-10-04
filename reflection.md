# Lab 17 — Data Pipeline Engineering (K4 Track 02 Day 17)

> Ghi chép cá nhân để học sâu về kiến trúc data pipeline: Bronze / Silver / Gold, CDC Debezium, idempotent write, late-arriving events, snapshot training, checksum-based correctness. Phong cách "dạy lại chính mình" như `S:\ai20k\Day16-Track2-Assignment\reflections.md`.

---

## 1. Tổng quan — đường đi của một record

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                         DAY 17 PIPELINE ARCHITECTURE                         │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌─────────┐    ┌─────────┐    ┌─────────┐    ┌─────────┐    ┌─────────┐   │
│  │  POSTGRES│    │  KAFKA  │    │    S3   │    │         │    │         │   │
│  │ tickets │    │support. │    │transcripts│    │         │    │         │   │
│  │  (CDC)  │    │ events  │    │  (JSON) │    │         │    │         │   │
│  └────┬────┘    └────┬────┘    └────┬────┘    │         │    │         │   │
│       │              │              │         │         │    │         │   │
│       ▼              ▼              ▼         ▼         ▼    ▼         ▼   │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │ BRONZE (lake/ — immutable Parquet, 1 file / source / day)            │   │
│  │  _payload, _source, _op, _ingested_at, _batch_id,                    │   │
│  │  _kafka_partition, _kafka_offset                                     │   │
│  │  → Giữ NGUYÊN bản ghi trùng, tombstone, dữ liệu hỏng                 │   │
│  └────────────────────────────┬─────────────────────────────────────────┘   │
│                               │                                             │
│                               ▼                                             │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │ STAGING (typed views over Bronze)                                     │   │
│  │  ticket_changes_sql()  ← parse Debezium envelope                     │   │
│  │  event_records_sql()   ← raw Kafka records                           │   │
│  │  transcript_records_sql()                                            │   │
│  │  event_lateness_sql()  ← đo P50/P95/P99 từ Bronze                   │   │
│  └────────────────────────────┬─────────────────────────────────────────┘   │
│                               │                                             │
│           ┌───────────────────┼───────────────────┐                        │
│           ▼                   ▼                   ▼                        │
│  ┌──────────────────┐ ┌───────────────┐ ┌──────────────────┐              │
│  │ SILVER TABLES    │ │ QUALITY GATE  │ │ QUARANTINE       │              │
│  │                  │ │ (Pydantic)    │ │                  │              │
│  │ silver_tickets   │ │ validate_events│ │ quarantine_events│              │
│  │  (upsert by key) │ │ → valid / bad │ │ (bad record never│              │
│  │ silver_history   │ │   record-level│ │  halts the run)  │              │
│  │  (SCD Type 2)    │ └───────────────┘ └──────────────────┘              │
│  │ silver_events    │                                                   │
│  │  (MERGE dedup)   │                                                   │
│  │ silver_transcripts                                                      │
│  │  (latest export wins)                                                │
│  └────────┬─────────┘                                                   │
│           │                                                             │
│           ▼                                                             │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │ GOLD TABLES — "đúng hình dạng cho đúng người dùng"                   │   │
│  │                                                                       │   │
│  │ gold_feature_daily   ← routing agent    1 row = 1 user × 1 day      │   │
│  │   (event_time, lookback window to absorb late events)               │   │
│  │                                                                       │   │
│  │ gold_training_set    ← classifier       immutable snapshot v<day>  │   │
│  │   (rebuilt from Bronze AS OF that day, never edited)                │   │
│  │                                                                       │   │
│  │ gold_doc_chunks      ← RAG index        1 row = 1 chunk             │   │
│  │   (embedding cached by hash(text) + model_version)                  │   │
│  └────────────────────────────┬─────────────────────────────────────────┘   │
│                               │                                             │
│                               ▼                                             │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │ CHECKSUM — order-independent md5 (the grading instrument)            │   │
│  │  SELECT md5(string_agg(CAST(t AS VARCHAR), chr(10) ORDER BY ...))   │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘
```

**Key principle**: _Mọi thứ chạy lại được (idempotent), không sửa Silver/Gold bằng tay — rebuild từ Bronze._

---

## 2. Ba tầng dữ liệu — cam kết & kỹ thuật

### 2.1 Bronze — "Raw truth" (pipeline/bronze.py)

| Cam kết (slide)                         | Thực hiện trong code                                                                |
| --------------------------------------- | ----------------------------------------------------------------------------------- |
| **Immutable** — never UPDATE/DELETE     | `land_batch()`: nếu file Parquet đã tồn tại → `already-landed`, no-op (dòng 85-87)  |
| **Append-only, 1 file / (source, day)** | Hive-style partition `ingest_date=YYYY-MM-DD/part-0.parquet` (dòng 37)              |
| **Giữ duplicate, tombstone, bad data**  | CDC `op='d'` + tombstone (`value=null`) đều được land (dòng 72-75)                  |
| **Idempotent landing**                  | `COPY TO .tmp → os.replace(tmp, out)` atomic (dòng 94-96)                           |
| **Lineage đầy đủ**                      | `_payload` (JSON gốc), `_ingested_at` (Kafka timestamp), `_batch_id` (= ingest day) |

**Tại sao giữ cả rác?** Silver mới có ngữ cảnh đủ để validate, dedup, quarantine. Bronze là _single source of truth_ cho audit & replay.

### 2.2 Staging — parse Debezium đúng (pipeline/staging.py)

Đọc CDC Debezium là nửa việc CDC:

```
Kafka record structure:
  key   = {"ticket_id": "T-97"}
  value = {"before": {...} | null,     # trước khi thay đổi (null cho c/r)
           "after":  {...} | null,     # sau khi thay đổi  (null cho d)
           "source": {"lsn": ..., "ts_ms": ...},
           "op": "c" | "u" | "d" | "r"}
  tombstone sau delete: key={"ticket_id":"T-97"}, value=null
```

**Ticket key nằm ở đâu khi `after=null`?** → ở **Kafka `key`** (dòng `j->'value'->'after'->>'ticket_id'` sẽ NULL cho op='d').  
_Bug 3 gốc rễ_: `ticket_changes_sql()` chỉ extract từ `after` → delete mất key → bị filter `WHERE ticket_id IS NOT NULL` → không bao giờ đến Silver.  
_Sửa_: extract `ticket_id` từ `j->'key'->>'ticket_id'` (Bronze lưu nguyên `_payload` JSON chứa cả `key`).

LSN (`source.lsn`) là **sequence number** của Postgres WAL — **tăng đơn điệu toàn cục** → dùng để sort thay đổi mới hơn. `_changed_at` từ `ts_ms` (Debezium microsecond epoch).

### 2.3 Silver — "1 hàng = 1 thực thể, có khoá" (pipeline/silver.py)

| Bảng                    | Khoá                        | Chiến lược ghi                                                                                                                                   | Note                                              |
| ----------------------- | --------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------- |
| `silver_tickets`        | `ticket_id`                 | **Upsert theo khoá** — `QUALIFY row_number() OVER (PARTITION BY ticket_id ORDER BY _lsn DESC) = 1` rồi `INSERT` (BUG: cần `MERGE`/`ON CONFLICT`) | PII masked by `mask_pii()` macro                  |
| `silver_ticket_history` | (`ticket_id`, `valid_from`) | **SCD Type 2** — rebuild toàn bộ từ Bronze mỗi run (cheap ở cỡ này)                                                                              | `valid_to = lead(_changed_at)`, `is_current` flag |
| `silver_events`         | `event_id`                  | **MERGE** insert when not matched + `QUALIFY` dedup by `_ingested_at, _kafka_offset`                                                             | Bad records → `quarantine_events`                 |
| `silver_transcripts`    | `ticket_id`                 | **MERGE** update khi `exported_at` mới hơn                                                                                                       | PII masked                                        |

**Idempotent write pattern** (slide "Bốn cách viết idempotent"):

1. **Overwrite-partition** — xóa partition ngày rồi insert lại (Gold feature_daily)
2. **MERGE upsert** — match on key, insert or update (silver_tickets, events, transcripts)
3. **Rebuild full** — `CREATE OR REPLACE TABLE ... AS SELECT ...` (silver_history)
4. **Insert-only + dedup view** — events là immutable facts

**PII masking** (pipeline/silver.py:24-30): macro `mask_pii()` dùng regex email + phone VN (`+84|0`...). Chỉ che email/phone — **tên người không che** (câu hỏi suy ngẫm 2).

### 2.4 Gold — ba nghĩa của "clean" (pipeline/gold.py)

| Bảng                 | Mục đích                 | Kỹ thuật then chốt                                                                                                                                                                 |
| -------------------- | ------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `gold_feature_daily` | Routing agent (realtime) | **Lookback window**: `DELETE WHERE event_date BETWEEN day-LB .. day` → recompute từ Silver. **Late events phải rơi đúng `event_date` (event_time)** chứ không phải `_ingested_at`. |
| `gold_training_set`  | Classifier (offline)     | **Point-in-time snapshot** `v<day>` rebuilt from Bronze `upto=day`. Immutable — nếu tồn tại checksum khác nhau → raise `SnapshotImmutableError`.                                   |
| `gold_doc_chunks`    | RAG index                | **Chunk** 40 words overlap 8 → deterministic `text_hash` → **embedding cache** keyed by `(text_hash, model_version)`. Re-run embeds 0 chunks mới.                                  |

**Lookback = ceil(P99 lateness)** (slide "Data về muộn"): đo từ Bronze (`event_lateness_sql()`), không đoán. P99 = 3.00 ngày → `LOOKBACK_DAYS >= 3` (config.py:28 đang là 0 → bug 2).

---

## 3. Ba lỗi cài sẵn (đã đoán trước khi sửa)

### Lỗi 1 — Silver: `silver_tickets` append thay vì upsert (pipeline/silver.py:81-86)

|                     |                                                                                                                                                                                                                                                 |
| ------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Triệu chứng**     | `verify`: `silver_tickets has exactly one row per ticket_id (24 rows for 12 tickets)` — FAIL. T-91 hiện 3 state: `low/open`, `high/open`, `high/closed/bug` thay vì chỉ `high/closed/bug`.                                                      |
| **Nguyên nhân gốc** | `upsert_silver_tickets()` dùng `INSERT INTO silver_tickets SELECT ... FROM _latest_changes` (dòng 81-86). Không có `MERGE` / `ON CONFLICT` → mỗi batch append thêm dòng. Khi re-run ngày cũ sau ngày mới, LSN cũ thắng LSN mới (sai!).          |
| **Slide concept**   | "Silver — Có khoá", "Bốn cách viết idempotent" → MERGE upsert theo khoá `ticket_id`, tie-break bằng `_lsn DESC`.                                                                                                                                |
| **Hướng sửa**       | Thay `INSERT` bằng `MERGE INTO silver_tickets AS t USING _latest_changes AS s ON t.ticket_id = s.ticket_id WHEN MATCHED AND s._lsn > t._lsn THEN UPDATE ... WHEN NOT MATCHED THEN INSERT ...` (hoặc `ON CONFLICT DO UPDATE` nếu DuckDB hỗ trợ). |

### Lỗi 2 — Late data: `LOOKBACK_DAYS = 0` nhưng P99 = 3 ngày (pipeline/config.py:28)

|                     |                                                                                                                                                                                                                                                                                                                                                  |
| ------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **Triệu chứng**     | `verify`: `gold_feature_daily reconciles with a full recompute` FAIL (checksum lệch). `u05's offline events of 08-12 (arrived 08-15) are counted on 08-12` FAIL (got `(2,0)` expected `(5,1)`). `LOOKBACK_DAYS covers measured P99 lateness` FAIL.                                                                                               |
| **Nguyên nhân gốc** | `build_feature_daily()` recompute window `[day - LOOKBACK_DAYS, day]`. Với `LOOKBACK_DAYS=0`, chỉ recompute ngày `day`. Events của u05 (event_time 08-12, ingested 08-15) → batch 08-15 insert vào Silver → Gold chạy ngày 08-15 không nhìn ngược về 08-12 → feature 08-12 thiếu events. Full recompute (scan cả Silver) thì có → checksum khác. |
| **Slide concept**   | "Data về muộn": _Measure, don't guess_ — lookback = ceil(P99 của `(_ingested_at - event_time)` đo từ Bronze).                                                                                                                                                                                                                                    |
| **Hướng sửa**       | `config.LOOKBACK_DAYS = 3` (hoặc đọc dynamic từ `lateness_profile()`). Lưu ý: lookback áp dụng cho **mọi daily run**, không chỉ backfill.                                                                                                                                                                                                        |

### Lỗi 3 — Delete propagation: T-97 vẫn còn ở Silver/Training/RAG (pipeline/staging.py)

|                     |                                                                                                                                                                                                                                                                                       |
| ------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Triệu chứng**     | `verify`: `deleted ticket T-97 is a tombstone` FAIL (got `is_deleted=False`, PII còn nguyên). `latest training snapshot excludes T-97` FAIL (1 row). `deletes propagate to RAG index: no chunk of T-97` FAIL (2 chunks).                                                              |
| **Nguyên nhân gốc** | `ticket_changes_sql()` extract fields từ `value.after` (dòng 40-50). Khi `op='d'`, `after=null` → mọi cột NULL → `ticket_id IS NOT NULL` filter out (dòng 58). **Delete record không bao giờ vào staging → Silver không biết xoá.** Kafka key chứa `ticket_id` nhưng không được dùng. |
| **Slide concept**   | "CDC log-based", "Xoá phải lan" — delete record phải tạo tombstone `is_deleted=true` tại Silver, sau đó lan xuống Gold (training snapshot filter `l._op <> 'd'`, RAG join `WHERE NOT t.is_deleted`).                                                                                  |
| **Hướng sửa**       | Trong `ticket_changes_sql()`: extract `ticket_id` từ `j->'key'->>'ticket_id'` (luôn có) thay vì `j->'value'->'after'->>'ticket_id'`. Với `op='d'`, các cột khác để NULL, set `is_deleted = true`. Tombstone (`value=null`, `_op IS NULL`) đã bị filter ở dòng 56 — đúng.              |

---

## 4. Lateness đo được từ Bronze

```bash
$ python main.py --lateness
event lateness over 43 Bronze records (calendar days):
  p50=0.00 p95=2.90 p99=3.00 max=3
-> lookback must be >= ceil(p99) = 3 day(s); config.LOOKBACK_DAYS = 0
```

| Metric  | Giá trị   | Ý nghĩa                                   |
| ------- | --------- | ----------------------------------------- |
| **P50** | 0.00 ngày | Hầu hết events đến trong cùng ngày        |
| **P95** | 2.90 ngày | 5% events trễ ≥ ~3 ngày                   |
| **P99** | 3.00 ngày | 1% events trễ 3 ngày (u05 offline 3 ngày) |
| **Max** | 3 ngày    | Cực đại trong seed                        |

→ `LOOKBACK_DAYS = 3` là tối thiểu. Thực tế nên thêm buffer (vd 4-5 ngày) cho an toàn.

---

## 5. Checksum — công cụ chấm điểm

```python
# pipeline/checksum.py:20-26
def query_checksum(con, sql):
    return con.execute(f"""
        SELECT md5(coalesce(string_agg(CAST(t AS VARCHAR), chr(10)
                                       ORDER BY CAST(t AS VARCHAR)), ''))
        FROM ({sql}) AS t
    """).fetchone()[0]
```

- **Order-independent**: sort toàn bộ row thành string → md5.
- **Thay đổi 1 cell / 1 row / duplicate** → checksum đổi.
- Chạy được ngay trong DuckDB CLI: `SELECT md5(...) FROM gold_feature_daily;`
- **Rerun check** (`scripts/rerun_check.py`): fresh build (C0) → re-run ngày 08-12 3 lần (C1,C2,C3). PASS ⇔ **C0 = C1 = C2 = C3**.  
  _Tại sao cần C0?_ Pipeline có thể "ổn định sai": lần chạy lại đầu tiên làm hỏng, các lần sau hỏng y như nhau → C1=C2=C3 nhưng ≠ C0 vẫn FAIL.

---

## 6. Dùng DuckDB (lite) / dbt (track) — không phải Spark

| Tiêu chí           | DuckDB (lite path)                       | dbt-duckdb (track)                                                  | Spark                   |
| ------------------ | ---------------------------------------- | ------------------------------------------------------------------- | ----------------------- |
| **Cỡ dữ liệu lab** | 7 ngày × ~few KB                         | giống nhau                                                          | Overkill                |
| **Zero-setup**     | ✅ pip install                           | ✅ pip install                                                      | ❌ cluster              |
| **SQL dialect**    | DuckDB (rich: QUALIFY, MERGE, window)    | dbt macros + DuckDB                                                 | Spark SQL               |
| **Incremental**    | Hand-written MERGE / overwrite-partition | `incremental_strategy='merge'` + `merge_update_condition`           | Structured Streaming    |
| **Microbatch**     | `DELETE + INSERT` window                 | `incremental_strategy='microbatch'` `batch_size='day'` `lookback=3` | Native                  |
| **Testing**        | pytest + verify.py                       | `dbt test` + unit test + contract                                   | Great Expectations etc. |
| **Dễ debug**       | In-process, print SQL                    | `dbt compile` → SQL thuần                                           | Phân tán                |

**Khi nào lên Spark?** Data > memory đơn máy, multi-tenant, streaming latency < giây, team lớn cần governance. Lab 17: DuckDB đủ, tập trung vào **logic pipeline** chứ không phải engine.

---

## 7. dbt track — parity với lite path

```
make dbt      # land Bronze → dbt build (PASS=19: 3 models + 13 tests + 1 unit test)
make parity   # silver_tickets + gold_feature_daily: checksum lite vs dbt khớp nhau
```

| Model dbt            | Chiến lược                          | Key config                                                                   |
| -------------------- | ----------------------------------- | ---------------------------------------------------------------------------- |
| `silver_tickets`     | `incremental_strategy='merge'`      | `unique_key=['ticket_id']`, `merge_update_condition='src._lsn > tgt._lsn'`   |
| `gold_feature_daily` | `incremental_strategy='microbatch'` | `batch_size='day'`, `lookback=3`, contract `not_null` + `unique_combination` |
| Unit test            | test logic dedup + delete           | `given`/`expect` fixture trong `dbt_project/models/`                         |

Parity đảm bảo **hai cách cài đặt cho cùng một kết quả** — tránh logic lệch giữa Python và SQL.

---

## 8. Bonus (tối đa +10)

| Bonus                | Yêu cầu                                                                                                                                                          | Trạng thái zero-key                  |
| -------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------ |
| **B1 — LLM cache**   | `pipeline/llm_label.py`: cache theo `hash(input) + model + prompt_version`. Re-run 0 LLM call. Đổi prompt → relabel có chủ đích. Output sai schema → quarantine. | `FakeLLM` sẵn, chạy `make bonus-llm` |
| **B2a — Airflow 3**  | `docker compose -f docker/docker-compose.yml up` → 7 daily run thành công, chụp ảnh + log checksum                                                               | Cần Docker                           |
| **B2b — Brainstorm** | Viết `bonus/DESIGN.md` theo `docs/bonus/BONUS-CHALLENGE.md`                                                                                                      | Không cần infra                      |

---

## 9. Các khái niệm cốt lõi để nhớ lâu (self-quiz)

### 9.1 Idempotency patterns

| Pattern                  | Khi nào dùng                            | Lab 17 ở đâu                                            |
| ------------------------ | --------------------------------------- | ------------------------------------------------------- |
| Overwrite-partition      | Partition time-series, recompute window | `gold_feature_daily` (DELETE partition + INSERT)        |
| MERGE upsert             | Key-based entity, update in-place       | `silver_tickets`, `silver_events`, `silver_transcripts` |
| Rebuild full (CTAS)      | SCD2 history, cheap full scan           | `silver_ticket_history`                                 |
| Insert-only + dedup view | Immutable facts, dedup at read          | `silver_events` (QUALIFY + MERGE)                       |

### 9.2 Late-arriving events handling

1. **Measure lateness từ Bronze** (event_time vs \_ingested_at) → P99.
2. **Set lookback ≥ ceil(P99)**.
3. **Daily run recompute [day - lookback, day]** → overwrite-partition Gold.
4. **Backfill = same code path** (chạy tuần tự từng ngày).

### 9.3 Snapshot training — point-in-time correctness

- `v<day>` = trạng thái ticket **tính đến cuối ngày đó** (Bronze `upto=day`).
- `priority_at_creation` = priority khi ticket tạo (`_op IN ('c','r')` first LSN).
- Late feedback (arrive sau) → tạo **snapshot version mới** (`v<later_day>`), version cũ **bất biến**.
- Deleted ticket (`op='d'`) → filter out ở latest snapshot (`l._op <> 'd'`).

### 9.4 Embedding cache key

`cache_key = (text_hash, model_version, prompt_version)`

- `text_hash = sha256(chunk_text)` → deterministic.
- Đổi model/prompt → bump version → cache miss → recompute có chủ đích.
- Re-run same day → 0 embedding call mới.

---

## 10. Câu hỏi suy ngẫm (chuẩn bị REPORT)

### Q1: Snapshot bất biến vs quyền được xoá dữ liệu (GDPR/Right to be Forgotten)

- Snapshot `v2026-08-12`..`v2026-08-14` chứa text T-97 (đã xoá 08-15). Snapshot immutable → không sửa được.
- **Giải pháp thực tế**:
  1. **Kho snapshot riêng** (cold storage) với retention policy, không serving.
  2. **Serving layer** (Gold serving) chỉ đọc `latest snapshot` đã filter deleted.
  3. **Legal hold**: nếu law yêu cầu xoá hoàn toàn → rebuild warehouse từ Bronze đã scrub PII (cần Bronze có thể xoá được — nhưng Bronze cam kết immutable → trade-off).
  4. **Pseudo-anonymization tại Bronze**: hash `ticket_id` + salt per-day, giữ mapping riêng có TTL.

### Q2: PII beyond regex (tên, địa chỉ, CCCD, BHYT...)

- Regex chỉ che email/phone. Tên "Nguyễn Văn An" vẫn lọt.
- **Chốt PII ở tầng nào?**
  - **Bronze**: giữ nguyên (raw truth, audit).
  - **Silver**: **PII detection & masking** (NER model / Presidio / cloud DLP) trước khi ghi Silver. Quarantine nếu detect high-risk.
  - **Gold**: training set / doc chunks đã masked.
- **Đo coverage**: `pii_coverage = (entities_detected_and_masked) / (entities_in_gold_standard_sample)`. Target ≥ 99.9% cho production.

---

## 13. Nhật ký từng checkpoint (giọng intern)

> Dưới đây là nhật ký thực hành — đâu là cái mình nghĩ, đâu là cái mình hỏi AI, đâu là cái mình review diff, đâu là cái mình run verify. Workflow theo VIBE-CODING: *Spec → Prompt → Review diff → Run test → Commit/Rollback*.

### CP1 — Đọc đề và dựng baseline (20 phút)

**Trước khi chạm code, mình nghĩ:**

> *"Đọc README xong thấy 'repo này cố tình có 3 lỗi'. Đây không phải bug do mình tạo ra — seed đã thiết kế để fail từ đầu. Task rõ ràng: tìm 3 lỗi trong pipeline/, sửa, chứng minh rerun3 PASS. Nên chạy trước để thấy fail gì, mới biết sửa lỗi nào."*

**Setup env (Windows — tốn hơn dự kiến):**
1. `python -m venv .venv` sinh `bin/` không phải `Scripts/` — Python MSYS default.
2. Dùng `uv` có sẵn: `uv venv --python 3.11.15 .venv` → `Scripts/` đúng. Cài 66 packages trong 15s.
3. `python -m scripts.verify` → crash `UnicodeEncodeError` vì console cp1252 + output tiếng Việt.
4. **Fix**: `$env:PYTHONIOENCODING = "utf-8"`. *Lesson: trên Windows luôn bật PYTHONIOENCODING=utf-8 trước verify.*

**Baseline (trước fix):** VERIFY 8/18 FAIL, PYTEST 9 failed, LATENESS P99=3.00.

### CP2 — Sửa khoá Silver (25 phút)

**Đọc code trước khi prompt AI (VIBE pattern #2: validate trước generate):**

> *"Mở `pipeline/silver.py:64-88`. `upsert_silver_tickets()`: tạo `_latest_changes` bằng
> `QUALIFY row_number() OVER (PARTITION BY ticket_id ORDER BY _lsn DESC) = 1` —
> đây là **dedup trong batch** đúng (LSN cao nhất trong cùng batch). Nhưng dòng 81-86:
> `INSERT INTO silver_tickets SELECT ... FROM _latest_changes` — **append, không phải upsert!**
> 3 ngày → 24 rows cho 12 ticket. Re-run batch cũ → LSN cũ append tiếp → state cũ thắng mới."*

**Prompt AI (narrow, SDD pattern #1):**
> *"Write DuckDB MERGE: target silver_tickets, source _latest_changes. ON t.ticket_id=s.ticket_id.
> WHEN NOT MATCHED THEN INSERT. WHEN MATCHED AND s._lsn > t._lsn THEN UPDATE.
> Tie-break by LSN. If equal, skip."*

**Review diff (VIBE pattern — không skip):**
- Guard `s._lsn > t._lsn` đúng — LSN tăng đơn điệu WAL → mới thắng.
- Redelivery cùng LSN (impossible trên WAL) → không update → safe.
- `is_deleted`, `_batch_id` đều update để đồng bộ tombstone.

**Result:** Silver 5/5 PASS. T-97 vẫn FAIL vì chưa CP4 — đúng thứ tự: fix Silver trước mới thấy được delete.

### CP3 — Sửa dữ liệu đến muộn (25 phút)

> *"Comment code: 'each run only needs to recompute its own day' — nghe hợp lý. Nhưng
> P99=3 ngày! u05 offline 3 tối → event_time 08-12, ingested 08-15. LB=0 → daily run 08-15
> chỉ recompute partition 08-15, quên 08-12. Full recompute có event 08-12 → checksum khác."*

**Fix:** `config.LOOKBACK_DAYS = 3` (ceil(P99)). Apply cho **mọi daily run**, không chỉ backfill.
**Verify:** `lateness` → p99=3.00, LOOKBACK_DAYS=3 ✅.

### CP4 — Sửa CDC delete (25 phút)

> *"Đọc `data/cdc/tickets/2026-08-15.jsonl`: delete record có `key: {"ticket_id": "T-97"}`,
> `after: null`, `op: "d"`. Code staging.py:40 extract từ `value.after` → NULL → filter.
> Key phải lấy từ **Kafka `key`**, không phải `value.after`."*

**Fix staging.py:**
- `coalesce(j->'key'->>'ticket_id', j->'value'->'after'->>'ticket_id')` — key luôn có.
- `CASE WHEN _op = 'd' THEN NULL ELSE ... END` cho các field — không rò rỉ PII.
- Tombstone (`value=null`) đã filter ở `WHERE _op IS NOT NULL` — đúng.

**Verify ALL:**
- `python -m scripts.verify` → **18/18 ALL PASS** ✅
- `python -m pytest` → **34 passed** ✅
- `python -m scripts.rerun_check` → **PASS** (C0=C1=C2=C3) ✅

> *Challenge test evidence (commands from the 4 challenges):*
> - **Challenge 1**: `pytest tests/test_contracts.py -k "one_row_per_ticket or latest_state_wins"` → **2 passed**
> - **Challenge 2**: `python main.py --lateness` (P99=3.00) + `pytest -k "feature_daily or late_events or lookback"` → **3 passed**
> - **Challenge 3**: `python -m scripts.verify` (18/18) + `python -m pytest` (34 passed) + `python -m scripts.rerun_check` (PASS)
> - **Challenge 4**: `dbt build --event-time-start 2026-08-10 --event-time-end 2026-08-17` → **PASS=19**; `python -m scripts.parity` → **PARITY**

> *Lesson: "CDC delete ≠ Kafka tombstone" — delete record có key + after=null + op='d'.
> Tombstone Kafka (value=null) thì thực sự rỗng, chỉ để log compaction.*

### CP5 — dbt và parity (25 phút)

```
make dbt → PASS=19 (3 models + 13 tests + 1 unit test + 2 views)
make parity → PARITY — silver_tickets 3c15dfd43701, gold_feature_daily 8630e04a61d1
```

> *"dbt silver_tickets dùng merge_update_condition='src._lsn > tgt._lsn' — đúng logic CP2.
> gold_feature_daily microbatch + lookback=3 — đúng CP3. Unit test cho dedup+delete.
> Parity = confidence: 2 implementation cho cùng 1 checksum."*

### CP6 — Hoàn thiện bài nộp (30 phút)

**Checklist:**
- [x] checksums.txt PASS (C0=C1=C2=C3)
- [x] REPORT.md: 3 lỗi + công thức + lựa chọn tool + 2 câu hỏi + output thực tế
- [x] Verify 18/18, pytest 34/34, rerun3 PASS, lateness P99=3.00, dbt PASS=19, parity PARITY
- [x] Tên repo đúng, không chứa secret

> *Lesson: REPORT không phải decoration — coach đọc Report rồi hỏi follow-up.
> Mỗi dòng fix phải giải thích được: tại sao, concept nào, trade-off ra sao.*

### CP7 — Bonus B1 + B2b (+10)

- **B1 LLM step**: `pipeline/llm_label.py` rewrite — cache `hash(input)+model+prompt_version` trong `llm_label_cache`, quarantine off-schema (kể cả invalid để re-run vẫn 0 calls), ước tính cost trước, version `model`+`prompt_version` trên từng hàng Gold. `make bonus-llm` → **BONUS PASS** (6/6 OK). Provider thật `OpenAILLM(o4-mini)` gate bằng `OPENAI_API_KEY`, interface `complete()`+counter giống `FakeLLM`.
- **B2b DESIGN.md**: brainstorm flywheel chat VN → eval+SFT dataset, leakage-safe. 6 quyết định (schema drift, batch vs streaming, point-in-time session split, decontamination SHA-256+LSH, quarantine SLO, cost swap `text-embedding-3-small`) + 1 rejected alternative (toàn bộ LSH) + architecture sketch. ≥600 từ → **BONUS PASS**.
- *Lesson: user đã chỉnh chỉnh model là openai/o4-mini (không phải Gemini như agent tự bịa) — sửa `GeminiLLM→OpenAILLM`, `EMBEDDING_MODEL_VERSION="text-embedding-3-small"`. Bonus không trừ core.

---
