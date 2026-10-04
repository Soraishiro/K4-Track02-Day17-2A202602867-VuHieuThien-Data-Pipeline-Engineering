# K4-Track02-Day17 — Report cá nhân

**Họ tên / MSSV:** Vũ Hiếu Thiên / 2A202602867
**Repo:** https://github.com/Soraishiro/K4-Track02-Day17-2A202602867-VuHieuThien-Data-Pipeline-Engineering
**Commit bài nộp:** `a0ac75929f5e9ee2a1a6c3204091e3bd91217aa5` (hoàn chỉnh code + bonus + checksums.txt + REPORT; các check verify/pytest/rerun/dbt/parity/B1 đều chạy trên commit này)
**AI đã dùng và phạm vi hỗ trợ:** Dùng Kilo CLI agent để read code, đưa ra spec, review diff dòng-lẻ, chạy verify/pytest/rerun/dbt/parity. AI không tự implement — mỗi thay đổi review logic trước khi áp dụng.
**Nguồn tham khảo:** README.md, docs/CHECKPOINTS.md, docs/VIBE-CODING.md, docs/RUBRIC.md của repo đề bài.

## 1. Ba lỗi

|                          | Lỗi Silver                                                                                                                                                                                                     | Lỗi late data                                                                                                                                                                                                                                  | Lỗi xoá (CDC)                                                                                                                                                                                  |
| ------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Triệu chứng**          | `verify`: `silver_tickets has exactly one row per ticket_id (24 rows for 12 tickets)` — FAIL. T-91 hiện 3 state (`low/open`, `high/open`, `high/closed/bug`) thay vì `high/closed/bug`.                        | `verify`: `gold_feature_daily reconciles with a full recompute` FAIL (checksum lệch). `u05's offline events of 08-12` got `(2,0)` expected `(5,1)`. `LOOKBACK_DAYS=0 < 3`.                                                                     | `verify`: `deleted ticket T-97 is a tombstone` FAIL (`is_deleted=False`, PII còn). `latest training snapshot excludes T-97` FAIL (1 row). `RAG index: no chunk of T-97` FAIL (1 chunk).        |
| **Nguyên nhân gốc**      | `upsert_silver_tickets()` (silver.py:81) dùng `INSERT INTO ... SELECT FROM _latest_changes` — **không MERGE/upsert** giữa các batch. Mỗi daily run append. Re-run ngày cũ → LSN cũ có thể overwrite state mới. | `config.LOOKBACK_DAYS = 0` (config.py:28). `build_feature_daily()` recompute window `[day-0, day]` → chỉ ngày hiện tại. Events của u05 `event_time=08-12, ingested=08-15` rơi vào partition 08-12 nhưng daily run 08-15 không recompute 08-12. | `ticket_changes_sql()` (staging.py:40) extract `ticket_id` từ `j->'value'->'after'->>'ticket_id'`. Với `op='d'`, `after=null` → ticket_id NULL → filter `WHERE ticket_id IS NOT NULL` loại bỏ. |
| **Cách sửa**             | silver.py:81-86 → `MERGE INTO silver_tickets AS t USING _latest_changes AS s ON t.ticket_id = s.ticket_id WHEN NOT MATCHED THEN INSERT ... WHEN MATCHED AND s._lsn > t._lsn THEN UPDATE SET ...`               | config.py:28 → `LOOKBACK_DAYS = 3` (P99 = 3.00 calendar days từ `main.py --lateness`)                                                                                                                                                          | staging.py:40-51 → extract `ticket_id` từ `coalesce(j->'key'->>'ticket_id', j->'value'->'after'->>'ticket_id')`; với `op='d'` các field = NULL, `is_deleted=true`                              |
| **Khái niệm trên slide** | "Silver — Có khoá", "Bốn cách viết idempotent"                                                                                                                                                                 | "Data về muộn: đo P99 từ Bronze"                                                                                                                                                                                                               | "CDC log-based", "Xoá phải lan"                                                                                                                                                                |

## 2. Các con số

- P99 lateness đo từ Bronze: **3.00 ngày** → `LOOKBACK_DAYS = 3`
- `submission/checksums.txt`: **PASS** — `gold (combined): 39e115c510ecdf526800eac227158a4f` (C0 = C1 = C2 = C3)
- `make parity`: **PARITY** — silver_tickets `3c15dfd43701`, gold_feature_daily `8630e04a61d1` (lite = dbt)

## 3. Lựa chọn công cụ / kỹ thuật

- MERGE có LSN guard cho `silver_tickets`, overwrite-partition recompute window cho `gold_feature_daily`: vì phải idempotent — re-run batch cũ sau batch mới không được làm thay đổi state mới; LSN (WAL sequence) là tiêu chuẩn thứ tự thay đổi toàn cục của CDC, tie-break bằng LSN đảm bảo deterministic.
- Tombstone (`is_deleted=true`, PII NULL) thay vì xoá hàng Silver: vì CDC delete phải lan tỏa (training snapshot/history vẫn tham chiếu), Bronze immutable không xoá, giữ tombstone giúp audit "ai đã bị xoá và khi nào" bởi `lsn`.
- Snapshot training dựng lại từ Bronze `AS OF` ngày đó, không sửa snapshot cũ: vì point-in-time priority (`priority_at_creation`) và late feedback tạo version mới riêng (v14 ≠ v15); rebuild from Bronze = reproducible forever, độc lập thứ tự ingest.
- DuckDB (lite) / dbt-duckdb (track dbt) cho bài toán cỡ này, chứ không phải Spark: data < 1MB, zero-setup, DuckDB hỗ trợ đầy đủ MERGE/QUALIFY/microbatch; dbt chuẩn hoá incremental logic + contract/unit test + parity cross-check, không bù đắp chi phí cluster.

## 4. Hai câu hỏi suy ngẽm

1. **Snapshot `v2026-08-12`–`v2026-08-14` có text T-97** (đã xoá 08-15). Bất biến vs right-to-be-forgotten xung đột: snapshot được rebuild from Bronze `upto=day` — T-97 chưa xoá ở ngày đó nên hợp lệ. Giải pháp: serving layer chỉ dùng `v{last_day}` (đã filter deleted); legal-hold/xoá toàn bộ thì phải rebuild từ Bronze đã scrub PII. Trong lab: chỉ `v2026-08-15`+ mới filter T-97.
2. **Regex che email/phone nhưng tên "Nguyễn Văn An" lọt**: dùng NER/DLP (Presidio/cloud DLP) ở Silver để detect person_name, chuyển thành `[NAME]` hoặc hash. Chốt ở Silver — Bronze giữ raw, Gold chỉ nhận masked. Đo coverage qua gold standard sample: `detected_entities/match_count`.

## 5. Output (dán nguyên văn)

```text
$ .\.venv\Scripts\python.exe -m scripts.verify

=== verify.py — Day 17 pipeline contracts ===
  [OK ] Bronze  every daily batch landed as Parquet (7 days x 3 sources)
  [OK ] Bronze  re-landing a batch is a no-op (append-only, no duplicate file)
  [OK ] Bronze  Bronze keeps the raw truth: Kafka tombstone + redelivered events are still there
  [OK ] Silver  silver_tickets has exactly one row per ticket_id
  [OK ] Silver  T-91 shows its latest state: high / closed / bug
  [OK ] Silver  deleted ticket T-97 is a tombstone: is_deleted and no personal data left
  [OK ] Silver  no email / phone number survives past Bronze
  [OK ] Silver  silver_events has one row per event_id (Kafka redeliveries removed)
  [OK ] Silver  2 malformed events quarantined with a reason; the run did not halt
  [OK ] Gold    gold_feature_daily reconciles with a full recompute from Silver
  [OK ] Gold    u05's offline events of 08-12 (arrived 08-15) are counted on 08-12
  [OK ] Gold    LOOKBACK_DAYS covers measured P99 lateness (p99=3.00 days)
  [OK ] Gold    training set uses point-in-time priority (T-91 created as 'low')
  [OK ] Gold    late feedback creates a NEW snapshot version; the old one is untouched
  [OK ] Gold    latest training snapshot excludes the deleted ticket T-97
  [OK ] Gold    deletes propagate to the RAG index: no chunk of T-97
  [OK ] Gold    gold_doc_chunks: one row per chunk, and a re-run embeds 0 new chunks
  [OK ] Rerun   re-run 2026-08-12 three times -> Gold checksum identical to a fresh build

RESULT: 18/18 checks — ALL PASS
```

```text
$ .\.venv\Scripts\python.exe -m pytest
..................................                                       [100%]
= 34 passed in 3.25s =
```

```text
$ .\.venv\Scripts\python.exe -m scripts.rerun_check
# Lab 17 — re-run check for 2026-08-12

run                     gold_feature_daily    gold_training_set     gold_doc_chunks       gold (combined)
fresh build             8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #1 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #2 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #3 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f

RESULT: PASS — 3 re-runs, identical checksums
```

```text
$ .\.venv\Scripts\python.exe main.py --lateness
event lateness over 43 Bronze records (calendar days): p50=0.00 p95=2.90 p99=3.00 max=3
-> lookback must be >= ceil(p99) = 3 day(s); config.LOOKBACK_DAYS = 3
```

```text
$ ..\.venv\Scripts\dbt.exe build --profiles-dir . --event-time-start 2026-08-10 --event-time-end 2026-08-17
03:05:28  Finished running 3 incremental models, 13 data tests, 1 unit test, 2 view models in 0 hours 0 minutes and 2.16 seconds (2.16s).
03:05:28  Completed successfully
03:05:28  Done. PASS=19 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=19
```

```text
$ .\.venv\Scripts\python.exe -m scripts.parity
=== parity: lite pipeline vs dbt ===
  [OK ] silver_tickets       lite 3c15dfd43701  dbt 3c15dfd43701
  [OK ] gold_feature_daily   lite 8630e04a61d1  dbt 8630e04a61d1
RESULT: PARITY — both implementations agree
```

## 6. Bonus (tối đa +10, không trừ điểm nếu chọn không làm)

### B1 — LLM step (`pipeline/llm_label.py`, `make bonus-llm`) — PASS (+5)

Triển khai `label_tickets()` tuân 4 quy tắc trên slide "LLM là một bước transform":

1. **Cache key = `hash(input) + model + prompt_version`.** Bảng `llm_label_cache(text_hash, model, prompt_version)` tạo bởi `_ensure_cache_tables()`. Re-run cùng model + prompt → cache hit → **0 lời gọi LLM**; đổi `PROMPT_VERSION` → miss cố tình → re-label toàn bộ; đổi `model` cũng miss. Kết quả `re-run makes 0 LLM calls`.
2. **Validate trước khi Gold.** `parse_label()` chỉ chấp nhận JSON `{"label": bug|billing|other}`; trường hợp không parse được hoặc label ngoài tập thì thêm vào `llm_label_quarantine` (reason + raw_response), **không bao giờ** lên Gold. Quyết định đồng thời cache cả raw trả lời *vô lệ* để re-run vẫn 0 gọi.
3. **Chi phí ước tính TRƯỚC khi chạy.** `estimate_tokens()` → in dòng `cost estimate before running: ~484 tokens = $0.0010 per full run` trước vòng lặp.
4. **Version dữ liệu.** `gold_ticket_labels` lưu `(ticket_id, label, model, prompt_version)`; mỗi lần chạy xóa sạch model cũ và ghi lại phiên bản prompt hiện tại.

Provider mặc định `FakeLLM` (zero-key, deterministic, có chữ ký "xuất" → off-schema để test quarantine). Provider thật `OpenAILLM` chạy `o4-mini` khi có `OPENAI_API_KEY` — trùng giao diện `complete(prompt)` + counter `calls`, nên cache/quarantine logic không thay đổi.

```text
$ .\.venv\Scripts\python.exe -m scripts.bonus_llm
=== bonus: LLM labelling of 11 live tickets ===
  [OK ] first run labels every live ticket
  [OK ] re-run with same model + prompt makes 0 LLM calls
  [OK ] every Gold label is bug / billing / other
  [OK ] off-schema answers go to llm_label_quarantine
  [OK ] new prompt version re-labels on purpose
  [OK ] labels carry their prompt version
BONUS PASS
```

### B2b — Brainstorm thiết kế (`bonus/DESIGN.md`) — PASS (+5)

Chọn **bài toán**: flywheel log chat hỗ trợ (tiếng Việt) → eval set + SFT dataset, không rò rỉ future, không tự train trên chính lỗi của mình. 6 quyết định tải:

1. **Schema drift** (Web vs Mobile SDK khác schema) → schema-on-read + strict Silver contract, không ép producer.
2. **Batch vs streaming** → nightly microbatch idempotent (lặp lại `run_day` + `LOOKBACK_DAYS`), không Kappa vì bot chấp nhận độ trễ hàng giờ.
3. **Point-in-time** → tách theo `session_id` + event time (nợ lab `point_in_time_features`), không tách hàng ngẫu nhiên — tránh train/test leakage trong một phiên.
4. **Decontamination** → SHA-256(q+a) toàn cục làm mặc định, `minhash/LSH` dành riêng (được giữ lại) cho typo/emoji; bỏ qua toàn bộ chỉ bằng LSH vì mất head-of-distribution.
5. **Contract & quarantine** → bảng `flywheel_quarantine` + metric `quarantine_rate < 3%`, mô hình `llm_label_quarantine` của lab.
6. **Chi phí** → hash embedder mặc định, swap `EMBEDDING_MODEL_VERSION = text-embedding-3-small` (OpenAI, stable Apr 2026) chỉ khi held-out recall yêu cầu; cache key phiên bản embed giống cache LLM.

Bản đồ kiến trúc ASCII trong `DESIGN.md`; nguyên lý "one real answer beats ten shallow ones" – mỗi quyết định đều có X vs Y + lý do chọn X + ít nhất một phương án bị từ chối.
