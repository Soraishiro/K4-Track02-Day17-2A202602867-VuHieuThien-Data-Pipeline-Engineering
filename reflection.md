# Lab 17 — Data Pipeline Engineering

Ghi chép này bắt đầu từ phần nền tảng trước, rồi mới đi vào ba lỗi của lab. Mình muốn hiểu được đường đi của dữ liệu và vai trò của từng công cụ trước khi đọc code chi tiết.

## 1. Từ dữ liệu rời rạc tới một data pipeline

Một hệ thống hỗ trợ khách hàng thường tạo dữ liệu ở nhiều chỗ.

Thông tin của ticket nằm trong database. Mỗi lần người dùng click hoặc gửi feedback lại tạo thêm event. Nội dung hội thoại có thể được lưu thành file ở một hệ thống khác. Khi cần làm báo cáo, tạo feature cho model hoặc dựng dữ liệu huấn luyện, những nguồn này phải được gom lại và xử lý theo cùng một quy trình.

Chuỗi công việc đó chính là **data pipeline**.

Ở mức dễ hiểu nhất, pipeline trong bài này làm ba việc:

1. nhận dữ liệu từ các nguồn;
2. xử lý để dữ liệu có cấu trúc và đáng tin cậy hơn;
3. tạo ra các bảng phục vụ cho từng nhu cầu phía sau.

Cách nhìn này khá gần với mô tả chung của IBM và AWS: pipeline nhận dữ liệu từ nguồn, xử lý qua một số bước rồi đưa tới nơi lưu trữ hoặc nơi sử dụng tiếp theo.

### Dữ liệu của lab đến từ đâu?

Lab mô phỏng một nền tảng hỗ trợ khách hàng với ba loại dữ liệu.

**Tickets** được xem như đang nằm trong Postgres, tức database chính của ứng dụng. Khi một ticket được tạo, cập nhật hoặc xoá, hệ thống cần biết thay đổi đó để cập nhật dữ liệu downstream.

Để làm việc này, lab dùng cách biểu diễn của **Debezium**. Debezium là công cụ Change Data Capture, viết tắt là **CDC**. Hiểu đơn giản, CDC ghi nhận những thay đổi đã xảy ra trong database theo thời gian. Debezium đưa các thay đổi của từng row thành một dòng sự kiện để hệ thống khác đọc tiếp.

**Support events** là các hành động như click hoặc feedback. Trong hệ thống thật, loại dữ liệu này thường đi qua một event stream như Kafka. Một event có thể bị gửi lại khi consumer retry, nên pipeline phải nhận ra trường hợp trùng.

**Transcripts** là nội dung hội thoại được xuất thành file. Trong kiến trúc thật, loại file này có thể nằm ở object storage như S3.

Lab không dựng ba hệ thống thật. Các dữ liệu nguồn đã được chuẩn bị sẵn dưới dạng JSON/JSONL trong thư mục `data/`. Nhờ vậy mình có thể tập trung vào cách xử lý dữ liệu mà vẫn giữ được cấu trúc gần với một hệ thống thực tế. Repo cũng ghi rõ Postgres/CDC, Kafka và S3 trong bài đều được mô phỏng bằng file.

Luồng tổng quát:

```text
tickets
events
transcripts
    │
    ▼
 Bronze
    │
    ▼
Staging + kiểm tra dữ liệu
    │
    ▼
 Silver
    │
    ▼
  Gold
```

Mỗi tầng giải quyết một bước khác nhau.

### Bronze — lưu lại dữ liệu đầu vào

Bronze là nơi dữ liệu được ghi xuống đầu tiên.

Mình coi đây là bản lưu gần với dữ liệu nguồn nhất. Record trùng, record delete hoặc record hỏng vẫn được giữ lại. Lý do rất thực tế: nếu logic xử lý phía sau cần sửa, mình còn dữ liệu gốc để chạy lại.

Dữ liệu Bronze trong lab được lưu bằng **Parquet**. Đây là một định dạng file dạng cột thường dùng cho dữ liệu phân tích. File được chia theo nguồn và ngày ingest để dễ đọc lại từng phần.

Các ý cần nhớ ở Bronze:

- **raw data**: dữ liệu còn gần với nguồn;
- **immutable**: file đã land thì không sửa nội dung cũ;
- **partition**: chia dữ liệu thành từng phần, ở đây chủ yếu theo ngày;
- **replay**: có thể đọc lại Bronze để dựng lại các tầng sau.

### Staging — đọc dữ liệu nguồn thành cấu trúc dễ xử lý

Dữ liệu nguồn thường mang theo cấu trúc riêng của từng hệ thống.

Ví dụ record CDC của Debezium có thông tin về trạng thái trước và sau một thay đổi, loại thao tác và metadata đi kèm. Staging bóc các trường này ra thành những cột mà SQL phía sau có thể dùng trực tiếp.

Đây cũng là lúc mình gặp khái niệm **LSN** trong dữ liệu Postgres CDC. LSN có thể hiểu là số thứ tự của thay đổi trong log của database. Khi cùng một ticket xuất hiện qua nhiều lần update, LSN giúp biết thay đổi nào xảy ra sau.

### Quality gate — kiểm tra từng record

Sau khi parse xong, một số record vẫn có thể sai dữ liệu.

Trong lab, Pydantic được dùng để validate event. Một event thiếu `user_id` hoặc có rating ngoài tập cho phép sẽ được ghi sang khu vực **quarantine** cùng lý do.

Quarantine là chỗ giữ record không đạt kiểm tra để mình xem lại sau. Nhờ đó những record hợp lệ vẫn tiếp tục được xử lý trong cùng một run.

Khái niệm cần nhớ ở đây là **data contract**: dữ liệu muốn đi qua bước tiếp theo phải đáp ứng một số điều kiện đã định nghĩa trước.

### Silver — dữ liệu đã có quy tắc rõ ràng

Sau Bronze và Staging, dữ liệu bắt đầu được tổ chức theo cách mà ứng dụng phía sau có thể dùng ổn định hơn.

Ví dụ `silver_tickets` cần trả lời một câu đơn giản: với mỗi `ticket_id`, trạng thái hiện tại là gì?

Muốn làm được vậy, bảng phải có **key**. `ticket_id` chính là key của ticket.

Khi cùng một ticket xuất hiện nhiều lần, mình gặp hai thao tác thường dùng:

- **dedup**: bỏ các bản ghi trùng trong tập dữ liệu đang xét;
- **upsert**: nếu key chưa có thì insert, nếu đã có thì update theo điều kiện.

Trong lab, update còn phải nhìn vào LSN để trạng thái mới hơn thắng trạng thái cũ.

Silver cũng giữ lịch sử ticket bằng **SCD Type 2**. Với cách này, mỗi giai đoạn tồn tại của một trạng thái được giữ thành một row riêng, kèm khoảng thời gian hiệu lực. Nhờ đó mình vừa có bảng current state, vừa có bảng history.

PII như email và số điện thoại cũng được che ở Silver trước khi dữ liệu đi xa hơn.

### Gold — dữ liệu được chuẩn bị cho từng mục đích sử dụng

Gold là tầng đầu ra của pipeline.

Mỗi bảng Gold trong lab phục vụ một nhu cầu khác nhau:

- `gold_feature_daily`: feature theo user và ngày cho routing agent;
- `gold_training_set`: dữ liệu snapshot cho classifier;
- `gold_doc_chunks`: các đoạn text dùng để kiểm tra logic chunk và cache cho luồng RAG.

Ở đây bắt đầu xuất hiện các khái niệm liên quan trực tiếp tới cách dữ liệu được dùng.

**Event time** là thời điểm sự kiện thật sự xảy ra.  
**Ingest time** là thời điểm pipeline nhận được sự kiện.

Hai thời điểm này có thể cách nhau vài ngày. Khi event đến muộn, pipeline cần quay lại tính lại một số ngày trước đó. Số ngày nhìn lại được gọi là **lookback**.

Với training data, lab dùng **snapshot**. Một snapshot là ảnh chụp trạng thái dữ liệu tại một mốc thời gian. Nếu dựng snapshot cho ngày 12/08 thì dữ liệu xuất hiện sau ngày đó chưa được phép làm thay đổi phiên bản cũ. Đây là ý chính của **point-in-time correctness**.

Với `gold_doc_chunks`, output được cache theo nội dung và version. Khi input và version giống nhau, lần chạy sau có thể dùng lại kết quả đã có.

---

## 2. Stack được dùng trong lab

Sau khi hiểu đường đi của dữ liệu, các công cụ trong repo dễ đặt đúng vị trí hơn.

### Python — phần điều phối chính

Python nối các bước của pipeline lại với nhau: land Bronze, chạy transform, build Silver/Gold và gọi các bước kiểm tra.

Lab còn dùng cùng một code path cho daily run và backfill. Cách này giúp giảm khả năng một nhánh chạy bình thường còn nhánh backfill dùng logic khác.

### DuckDB — engine SQL chạy local

DuckDB là nơi phần lớn câu SQL của pipeline được thực thi.

Mình dùng nó để:

- đọc Parquet;
- chạy window function;
- dedup record;
- `MERGE` dữ liệu vào bảng hiện có;
- aggregate feature;
- tính checksum.

`MERGE` là câu lệnh quan trọng trong bài. Khi target đã có cùng key, `MERGE` cho phép quyết định row đó nên được update hay giữ nguyên.

DuckDB phù hợp với lab vì toàn bộ dữ liệu rất nhỏ và chạy trực tiếp trên máy cá nhân.

### Pydantic — kiểm tra cấu trúc event

Pydantic nằm ở quality gate.

Schema mô tả một event hợp lệ cần những field nào và giá trị nào được chấp nhận. Record sai schema được đưa vào quarantine.

Nhờ vậy rule kiểm tra dữ liệu được viết thành code rõ ràng và có thể test được.

### dbt — viết transform theo dạng model SQL

Repo có thêm một track dbt dùng cùng dữ liệu Bronze.

dbt tổ chức mỗi phép biến đổi thành một **model**. Với bảng lớn, model có thể chạy theo kiểu **incremental**: lần sau xử lý phần dữ liệu mới hoặc phần cần cập nhật, thay vì dựng lại toàn bộ bảng.

Trong lab:

- `silver_tickets` dùng chiến lược `merge`;
- `gold_feature_daily` dùng **microbatch**, tức chia xử lý thành từng khoảng thời gian nhỏ;
- data test và unit test kiểm tra contract của model.

Sau khi chạy xong, `make parity` so kết quả của pipeline Python và dbt trên hai bảng chung. Repo dùng parity để kiểm tra hai cách triển khai có tạo ra cùng dữ liệu hay không.

### pytest và `scripts.verify` — kiểm tra logic

`pytest` chạy các test của code.

`scripts.verify` kiểm tra các contract ở mức pipeline, ví dụ:

- mỗi ticket có đúng một current state;
- late event đã được tính vào đúng ngày;
- ticket bị xoá đã biến mất khỏi output cần thiết.

Hai lớp kiểm tra này cho mình bằng chứng cụ thể sau mỗi lần sửa.

### Checksum — kiểm tra khả năng chạy lại

Lab còn tính checksum của các bảng Gold.

Checksum có thể hiểu như một dấu vân tay của dữ liệu. Nếu dữ liệu thay đổi thì checksum thay đổi theo.

Bài rerun tạo một fresh build trước, sau đó chạy lại cùng một ngày ba lần:

```text
fresh build -> C0
rerun #1    -> C1
rerun #2    -> C2
rerun #3    -> C3
```

Kết quả đạt yêu cầu khi:

```text
C0 = C1 = C2 = C3
```

Từ đây mình có khái niệm **idempotency**: chạy lại cùng một thao tác trên cùng dữ liệu không làm trạng thái cuối thay đổi thêm.

### Airflow — phần điều phối workflow trong bonus

Airflow xuất hiện trong một lựa chọn bonus của lab.

Nó dùng **DAG** để mô tả các task và thứ tự chạy. Với backfill, Airflow có thể tạo run cho nhiều ngày lịch sử rồi thực thi theo lịch đã định.

Trong bài nộp này mình chọn B2b brainstorm, nên Airflow chỉ nằm trong phần kiến thức tổng quan. Tài liệu bonus mô tả việc backfill bảy ngày và yêu cầu các run thực tế thành công.

### LLM step — một transform có cache và version

Bonus B1 thêm một bước gán nhãn bằng LLM.

Phần này giúp mình thấy một model call cũng cần được quản lý giống các transform khác:

- input giống nhau có thể cache;
- đổi model hoặc prompt version thì output được xem là một version mới;
- output sai schema đi quarantine;
- cost được ước tính trước khi chạy.

### Bản đồ stack sau khi ghép lại

```text
Nguồn dữ liệu
    │
    ├─ Debezium CDC: thay đổi của ticket
    ├─ Kafka-style events: click / feedback
    └─ S3-style files: transcript
    │
    ▼
Bronze
    └─ Parquet
    │
    ▼
Staging + Quality
    ├─ DuckDB SQL
    └─ Pydantic validation
    │
    ▼
Silver
    ├─ key
    ├─ dedup / upsert
    ├─ SCD2 history
    └─ PII masking
    │
    ▼
Gold
    ├─ feature + lookback
    ├─ training snapshot
    └─ chunk/cache
    │
    ├─ dbt: cách triển khai SQL thứ hai
    ├─ Airflow: orchestration ở bonus
    └─ pytest / verify / checksum: kiểm chứng kết quả
```

Đến đây các thuật ngữ chính của bài đã có vị trí cụ thể trong pipeline. Phần sau đi sâu vào từng lỗi và cách mình lần từ kết quả kiểm tra về đúng bước đang xử lý sai.

---

## 3. Bronze: vì sao phải giữ cả dữ liệu xấu

Lúc đầu mình thấy hơi ngược: đã biết record bị trùng hoặc hỏng thì sao không bỏ luôn ở Bronze?

Sau khi làm bài mình mới thấy lý do. Ở Bronze mình chưa có đủ ngữ cảnh để quyết định record nào nên bỏ. Một Kafka tombstone, một CDC delete hay một record bị gửi lại có thể trông “xấu”, nhưng chúng lại là bằng chứng cần thiết để dựng đúng state ở phía sau.

Vì vậy Bronze giữ nguyên:

- payload gốc;
- duplicate;
- tombstone;
- bad record;
- metadata ingest và batch.

`land_batch()` cũng phải idempotent. Nếu file của source/day đó đã tồn tại thì lần chạy lại không được tạo thêm một bản khác.

---

## 4. Staging và điểm dễ sai của CDC

Đây là chỗ mình vấp rõ nhất với T-97.

Một record Debezium delete có dạng gần như sau:

```text
key   = {"ticket_id": "T-97"}
value = {
  "before": {...},
  "after": null,
  "source": {"lsn": ...},
  "op": "d"
}
```

Ban đầu code lấy `ticket_id` từ `value.after`. Với create/update thì cách đó chạy được, nên nhìn qua rất khó phát hiện. Nhưng delete có `after=null`, vì vậy `ticket_id` thành NULL. Sau đó câu `WHERE ticket_id IS NOT NULL` loại luôn record delete.

Mình lần ngược từ lỗi verify: T-97 chưa bị xoá ở Silver. Từ đó kiểm tra staging, rồi mở record CDC thật của ngày 08-15. Khi nhìn thấy key vẫn còn `ticket_id` còn `after` đã null thì nguyên nhân gần như rõ ngay.

Cách sửa là ưu tiên Kafka key:

```sql
coalesce(
  j->'key'->>'ticket_id',
  j->'value'->'after'->>'ticket_id'
)
```

Đến đây mình phân biệt được hai thứ trước đó hay lẫn:

- CDC delete: còn `key`, có `op='d'`, `after=null`;
- Kafka tombstone: `value=null`, dùng cho log compaction.

Hai record này không có cùng vai trò.

LSN cũng quan trọng. `source.lsn` cho biết thứ tự thay đổi trong WAL. Khi chọn state mới nhất, mình ưu tiên LSN vì nó phản ánh thứ tự thay đổi của CDC.

---

## 5. Silver: dedup trong batch chưa phải là upsert

Bug Silver làm mình mất thời gian vì `_latest_changes` nhìn khá đúng.

Code đã có:

```sql
row_number() over (
  partition by ticket_id
  order by _lsn desc
)
```

Nhìn vào đây mình tưởng state đã được dedup. Nhưng đoạn này chỉ chọn bản mới nhất **trong batch đang xử lý**.

Ngay sau đó code lại:

```sql
INSERT INTO silver_tickets ...
```

Thế là batch nào chạy cũng append thêm một hàng. Khi chạy nhiều ngày, cùng một `ticket_id` có nhiều state. Nếu re-run batch cũ thì state cũ còn được thêm trở lại.

Mình tách bài toán thành hai lớp:

1. Trong source batch: chọn thay đổi mới nhất của mỗi ticket.
2. Giữa source batch và bảng Silver hiện có: chỉ cho phép state có LSN mới hơn ghi đè state cũ.

Vì vậy `MERGE` hợp lý hơn:

```text
ON t.ticket_id = s.ticket_id
WHEN MATCHED AND s._lsn > t._lsn THEN UPDATE
WHEN NOT MATCHED THEN INSERT
```

Guard `_lsn > t._lsn` là phần quan trọng. Nếu chạy lại batch cũ, source có LSN thấp hơn nên bảng không đổi. Nhờ vậy rerun mới thật sự idempotent.

Sau sửa này, T-91 chỉ còn state cuối là `high / closed / bug`.

---

## 6. Late-arriving data: vì sao partition cũ cần được tính lại

Bug thứ hai ban đầu nhìn giống lỗi aggregate.

`gold_feature_daily` của u05 ngày 08-12 chỉ có `(2,0)` trong khi full recompute cho `(5,1)`. Mình kiểm tra event thì thấy ba event còn lại có `event_time=08-12` nhưng đến hệ thống vào 08-15.

Pipeline chạy ngày 08-15 đã ingest được các event đó vào Silver. Vấn đề là Gold chỉ tính lại ngày 08-15 vì:

```text
LOOKBACK_DAYS = 0
```

Như vậy dữ liệu đã có trong Silver nhưng partition 08-12 không bao giờ được mở lại để tính.

Lúc này P99 lateness mới có ý nghĩa thực tế:

```text
p50 = 0.00 ngày
p95 = 2.90 ngày
p99 = 3.00 ngày
max = 3 ngày
```

Mình đặt `LOOKBACK_DAYS = 3`. Mỗi daily run sẽ tính lại từ `day-3` đến `day`. Khi event ngày 08-12 đến vào 08-15, partition 08-12 được recompute và kết quả khớp full build.

Điểm mình rút ra ở đây là lookback nên dựa trên lateness đo từ Bronze. Nếu tự chọn một con số “có vẻ đủ” thì pipeline có thể vẫn sai mà rất khó nhìn thấy.

---

## 7. Gold: cùng là dữ liệu sạch nhưng mỗi bảng cần một kiểu đúng khác nhau

Sau khi sửa ba bug, mình thấy Gold không thể dùng một chiến lược ghi chung cho mọi bảng.

### `gold_feature_daily`

Đây là dữ liệu theo ngày. Late event có thể làm thay đổi partition cũ, nên cách hợp lý là recompute một cửa sổ thời gian rồi overwrite các partition đó.

### `gold_training_set`

Training data cần point-in-time correctness. Snapshot `v2026-08-12` phải phản ánh những gì hệ thống biết đến ngày 08-12, không được âm thầm nhận feedback của ngày 08-15.

Vì vậy khi có dữ liệu mới, mình tạo snapshot version mới. Snapshot cũ giữ nguyên.

### `gold_doc_chunks`

RAG chunks cần tính ổn định. Text giống nhau với cùng model version thì embedding không nên chạy lại. Cache key dùng hash của text và version của model.

Ba bảng đều ở Gold nhưng bài toán hoàn toàn khác nhau. Cách ghi phải theo cách dữ liệu thay đổi, không theo tên tầng.

---

## 8. Vì sao checksum cần fresh build làm mốc

Rerun test có dạng:

```text
fresh build -> C0
rerun #1    -> C1
rerun #2    -> C2
rerun #3    -> C3
```

Điều kiện là:

```text
C0 = C1 = C2 = C3
```

Trước đây mình nghĩ chỉ cần `C1=C2=C3` là đủ. Nhưng pipeline có thể hỏng ở lần rerun đầu tiên rồi giữ nguyên trạng thái hỏng ở hai lần sau. Khi đó ba checksum sau vẫn bằng nhau.

`C0` là mốc để biết rerun không chỉ “ổn định”, mà còn ổn định so với trạng thái đúng ban đầu.

Kết quả cuối:

```text
gold (combined)
C0 = C1 = C2 = C3
39e115c510ecdf526800eac227158a4f
```

---

## 9. Lựa chọn DuckDB và dbt cho lab

Data của lab rất nhỏ, dưới 1 MB. Nếu dùng Spark thì phần lớn công sức sẽ chuyển sang setup và runtime, trong khi lỗi thật của bài nằm ở CDC, idempotency, late data và snapshot.

DuckDB cho mình:

- chạy local nhanh;
- có `MERGE`, window function và `QUALIFY`;
- dễ inspect SQL;
- dễ chạy lại test.

dbt được dùng như một implementation thứ hai cho cùng logic. Sau khi lite pipeline và dbt cho cùng checksum ở `silver_tickets` và `gold_feature_daily`, mình có thêm một lớp kiểm tra rằng hai cách viết không bị lệch nhau.

```text
silver_tickets       lite 3c15dfd43701  dbt 3c15dfd43701
gold_feature_daily   lite 8630e04a61d1  dbt 8630e04a61d1
```

---

## 10. PII: regex chỉ giải quyết phần dễ

Regex hiện tại che được email và số điện thoại. Tên người như “Nguyễn Văn An” vẫn đi qua.

Nếu làm thật, mình sẽ xử lý PII ở Silver. Bronze vẫn giữ raw để replay/audit; Silver là ranh giới trước khi dữ liệu đi vào training set hoặc RAG.

Một hướng hợp lý là:

1. regex cho pattern rõ như email, phone;
2. NER/DLP cho person name, địa chỉ, mã định danh;
3. quarantine record có rủi ro cao;
4. đo coverage trên sample đã gán nhãn thủ công.

Gold chỉ nên nhận dữ liệu đã qua bước này.

---

## 11. Snapshot bất biến và quyền được xoá

T-97 bị xoá ngày 08-15. Vì vậy snapshot 08-12 đến 08-14 vẫn có T-97 nếu mình dựng đúng theo trạng thái lịch sử.

Trong lab, điều này không mâu thuẫn: snapshot cũ đại diện cho quá khứ, còn serving layer dùng snapshot mới đã loại T-97.

Nhưng nếu có yêu cầu pháp lý phải xoá dữ liệu khỏi toàn bộ lịch sử thì “snapshot bất biến” không còn là luật tuyệt đối. Khi đó cần cơ chế scrub nguồn và rebuild dữ liệu liên quan. Đây là trade-off giữa khả năng tái lập lịch sử và yêu cầu xoá dữ liệu.

---

## 12. Bonus LLM: quản lý một bước transform có model

Phần bonus làm mình thấy LLM trong data pipeline nên được đối xử như một transform có version.

Cache key:

```text
hash(input) + model + prompt_version
```

Nếu ba thứ này không đổi thì rerun phải dùng cache và tạo 0 LLM call. Nếu prompt đổi, cache miss là có chủ đích vì output cũ không còn cùng version.

Output của model cũng không được đẩy thẳng vào Gold. `parse_label()` chỉ nhận:

```json
{ "label": "bug" }
```

với label thuộc `bug | billing | other`.

Nếu model trả sai schema thì record vào quarantine. Raw response đó vẫn được cache; lần rerun sau có thể nhận lại kết quả đã biết mà không phát sinh thêm một lần gọi model.

Kết quả:

```text
BONUS PASS
```

---
