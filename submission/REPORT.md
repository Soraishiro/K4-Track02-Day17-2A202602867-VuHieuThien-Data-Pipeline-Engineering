# K4-Track02-Day17 — Report cá nhân

**Họ tên / MSSV:** Vũ Hiếu Thiên / 2A202602867  
**Repo:** https://github.com/Soraishiro/K4-Track02-Day17-2A202602867-VuHieuThien-Data-Pipeline-Engineering  
**Commit bài nộp:** `a0ac75929f5e9ee2a1a6c3204091e3bd91217aa5`  
**Nguồn đối chiếu:** `README.md`, `docs/CHECKPOINTS.md`, `docs/VIBE-CODING.md`, `docs/RUBRIC.md`.

## 1. Kết quả

| Hạng mục | Kết quả |
|---|---|
| Pipeline contracts | **18/18 PASS** |
| Pytest | **34 passed** |
| Rerun 3 lần | **PASS**, `C0 = C1 = C2 = C3` |
| Lateness | **P99 = 3.00 ngày** |
| dbt build | **PASS=19, WARN=0, ERROR=0** |
| Lite ↔ dbt | **PARITY** |
| Bonus B1 | **PASS** |
| Bonus B2b | **PASS** |

## 2. Ba lỗi chính

| Lỗi | Dấu hiệu | Nguyên nhân | Sửa | Xác nhận |
|---|---|---|---|---|
| Silver bị trùng state | `silver_tickets` có 24 hàng cho 12 ticket. T-91 có 3 state. | `upsert_silver_tickets()` chỉ `INSERT`; không upsert giữa các batch. | Dùng `MERGE` theo `ticket_id`. Chỉ update khi `s._lsn > t._lsn`. | Silver còn đúng 1 hàng cho mỗi `ticket_id`; T-91 = `high / closed / bug`. |
| Gold bỏ sót late data | Feature của u05 ngày 08-12 là `(2,0)` thay vì `(5,1)`. | `LOOKBACK_DAYS=0`; run 08-15 không tính lại partition 08-12. | Đặt `LOOKBACK_DAYS=3`, bằng `ceil(P99 lateness)`. | Full recompute và daily pipeline cho cùng checksum. |
| CDC delete không lan xuống Gold | T-97 vẫn còn PII, training snapshot và RAG chunk. | Với `op='d'`, `after=null`; code lấy `ticket_id` từ `value.after`, nên bản ghi delete bị loại. | Lấy `ticket_id` từ Kafka `key`; tạo tombstone ở Silver. | T-97 có `is_deleted=true`, PII = NULL, không còn trong latest training snapshot và RAG. |

## 3. Các quyết định kỹ thuật

**Silver ticket.** Tôi dùng `MERGE` với LSN guard. `ticket_id` xác định thực thể. `_lsn` xác định thay đổi nào mới hơn. Vì vậy, chạy lại batch cũ không thể ghi đè state mới.

**Late data.** Tôi không chọn lookback theo cảm tính. Bronze cho `P99 = 3.00 ngày`, nên `LOOKBACK_DAYS = 3`. Mỗi daily run tính lại cửa sổ `[day-3, day]`.

**CDC delete.** Silver giữ tombstone thay vì xoá vật lý. Cách này giữ được dấu vết CDC và cho phép delete tiếp tục lan xuống các bảng Gold.

**Training snapshot.** Mỗi snapshot được dựng theo trạng thái dữ liệu tại ngày tương ứng. Snapshot cũ không bị sửa khi feedback đến muộn; pipeline tạo version mới.

**Engine.** DuckDB và dbt-duckdb đủ cho dữ liệu lab dưới 1 MB. Spark không đem lại lợi ích tương xứng với chi phí vận hành trong bài này.

## 4. Hai câu hỏi suy ngẫm

### 4.1 Snapshot bất biến và quyền xoá dữ liệu

Snapshot `v2026-08-12` đến `v2026-08-14` vẫn có T-97 vì tại thời điểm đó ticket chưa bị xoá. Từ `v2026-08-15`, T-97 bị loại khỏi snapshot phục vụ.

Nếu có yêu cầu xoá pháp lý trên toàn lịch sử, không thể chỉ dựa vào tính bất biến của snapshot. Khi đó cần cơ chế scrub dữ liệu nguồn và rebuild các snapshot liên quan.

### 4.2 PII ngoài email và số điện thoại

Regex hiện tại che email và số điện thoại nhưng không nhận ra tên người như “Nguyễn Văn An”.

Vị trí phù hợp để xử lý là Silver: dùng NER/DLP để nhận diện `person_name`, địa chỉ và mã định danh; sau đó mask hoặc hash trước khi dữ liệu đi xuống Gold. Có thể đo coverage trên một gold-standard sample.

## 5. Bằng chứng chạy

```text
$ .\.venv\Scripts\python.exe -m scripts.verify
RESULT: 18/18 checks — ALL PASS
```

```text
$ .\.venv\Scripts\python.exe -m pytest
..................................                                       [100%]
= 34 passed in 3.25s =
```

```text
$ .\.venv\Scripts\python.exe -m scripts.rerun_check
RESULT: PASS — 3 re-runs, identical checksums
gold (combined): 39e115c510ecdf526800eac227158a4f
```

```text
$ .\.venv\Scripts\python.exe main.py --lateness
event lateness over 43 Bronze records (calendar days):
p50=0.00 p95=2.90 p99=3.00 max=3
-> lookback must be >= ceil(p99) = 3 day(s); config.LOOKBACK_DAYS = 3
```

```text
$ ..\.venv\Scripts\dbt.exe build --profiles-dir . --event-time-start 2026-08-10 --event-time-end 2026-08-17
Done. PASS=19 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=19
```

```text
$ .\.venv\Scripts\python.exe -m scripts.parity
[OK ] silver_tickets       lite 3c15dfd43701  dbt 3c15dfd43701
[OK ] gold_feature_daily   lite 8630e04a61d1  dbt 8630e04a61d1
RESULT: PARITY — both implementations agree
```

## 6. Bonus

### B1 — LLM step: PASS (+5)

`label_tickets()` có bốn cơ chế:

1. Cache theo `hash(input) + model + prompt_version`.
2. Validate JSON trước khi ghi Gold; output sai schema vào quarantine.
3. In ước tính chi phí trước khi gọi model.
4. Lưu `model` và `prompt_version` cùng label để truy vết version.

Re-run cùng model và prompt tạo **0 LLM call**. Đổi prompt version sẽ chủ động chạy label lại.

```text
$ .\.venv\Scripts\python.exe -m scripts.bonus_llm
BONUS PASS
```

### B2b — Thiết kế data flywheel: PASS (+5)

Thiết kế dùng log chat tiếng Việt để tạo eval set và SFT dataset. Sáu quyết định chính gồm: schema-on-read, nightly microbatch, point-in-time split theo session, decontamination bằng SHA-256 kết hợp MinHash/LSH khi cần, quarantine SLO và versioned embedding cache.

## 7. Phạm vi AI hỗ trợ

Tôi dùng Kilo CLI agent để đọc code, đề xuất spec, review diff và chạy các bước verify/pytest/rerun/dbt/parity. Tôi review logic trước khi áp dụng thay đổi. AI không thay thế bước kiểm tra kết quả cuối.
