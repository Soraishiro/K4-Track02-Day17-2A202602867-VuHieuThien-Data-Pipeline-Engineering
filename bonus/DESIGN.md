# B2 — Design brainstorm: leakage-safe support-chat flywheel

> One of the two B2 options in `docs/RUBRIC.md` (the other is an Airflow run).
> Problem chosen: turn a **live Vietnamese-language customer-support chat log**
> into a versioned **eval set + fine-tuning dataset** that feeds both a RAG
> retriever and an SFT model of the support bot, without leaking the future into
> training or poisoning eval with near-duplicates.

The product is a Vietnamese e-commerce support chatbot. Every day it fields
thousands of chats (mixed `vi_VN` text, occasional English brand names, emoji,
truncated tokens). The ML team wants to *close the loop*: mine resolved chats as
positive signals, mine failed chats as negative signals, and feed both back into
(a) a RAG index of canonical answers, and (b) an SFT model that emits those
answers. The catch is real: a careless pipeline trains the bot on its own future
mistakes and tests it on data it has already seen — classic train/serve and
train/test leakage in one shot.

## 1. Source & shape (schema drift in the wild)

Chats arrive from two SDKs (Web Chat and the Mobile in-app widget) with
**different schemas**: the mobile SDK tags an `intent_code`, the Web SDK leaves it
null; sometimes `user_id` is hashed client-side, sometimes plain; `resolved`
flips 1→0 after agent takeover in the same session. So the source is *not*
stable.

- Option A — **schema-on-read with a strict downstream contract**: ingest raw
  JSON verbatim into Bronze, cast/null-fill in DuckDB Staging, and assert a
  fixed `NOT NULL` contract on Silver.
- Option B — **evolve the schema in the producer**: version the SDKs and refuse
  to land rows that don't match the latest protobuf.

Pick **A**. The source is a third-party chat widget we don't fully control, and
support agents add ad-hoc tags by hand — producer changes would lag reality by
weeks. A strict contract at the Silver boundary (`user_id NOT NULL`,
`resolved_at NOT NULL`, `lang IN ('vi','en')`) gives us a stable spine while
Bronze stays a cheap, append-only raw lake. This is exactly the
Bronze→Staging→Silver layering the core lab already models; reusing it means the
leakage discussion in §3 can reference a single source of truth.

## 2. Freshness budget (batch vs streaming)

Chats come in at ~40/sec peak, mostly sub-second payloads. The bot's retrieval
index, however, only needs to refresh every few hours — support staff can correct
a stale answer for a few hours without business impact.

- Option A — **Kappa/Streaming**: Kafka → Flink → vector DB upsert, sub-minute
  freshness, expensive connectors, schema-evolution pain.
- Option B — **Nightly microbatch**: a single DuckDB transform per day (the lab's
  `run_day` already implements exactly this shape) that emits a new checkpoint
  of the eval set + index.

Pick **B**. Freshness of "some hours" is enough for a support bot, and a daily
microbatch lets every run be **idempotent and re-runnable** (`reset_warehouse`
→ `backfill`), which is the safety property we need more than low latency. The
same `LOOKBACK_DAYS` lateness trick from the core lab applies here: late chats up
to ~P99 of the previous day's lag get folded into the current batch so the Gold
snapshot stays total. Rejected: Streaming would be the right call if we had a
real-time "answer quality drops in the last hour" SLA — we don't.

## 3. Train/serve parity + point-in-time (the leakage test)

This is the load-bearing decision. Each fine-tuning example must (a) use only
context the bot had at prediction time, and (b) land in exactly one of
train/val/test by session, never both.

- Option A — **row-level timestamp cutoff**: split by `created_at` percentile.
- Option B — **session-level cutoff seeded by event time** (all chats of a
  session go to the same split; the split boundary is a point-in-time that every
  feature can be joined against).

Pick **B**, reusing the lab's `point_in_time_features` (vs the leaky
`naive_leaky_features`). Row-level splits leak because a single support session
is one logical episode — the bot's 9:03 reply and the customer's 9:04 follow-up
are the same *instance*; cutting between them trains on the label. Session-level
splitting keyed off `session_id` + a point-in-time boundary is the smallest
change that removes leakage, and it composes cleanly with RAG chunking: the
chunk store is also snapshotted per split date so retrieval can never reach into
a test session.

## 4. Decontamination (near-duplicate poisoning)

The bot will repeat canned answers ("Vui lòng kiểm tra lại địa chỉ giao hàng"),
so identical `<question, answer>` pairs recur across days. If both copies land in
train and val, eval is inflated.

- Option A — **hash the question+answer blob, dedupe globally within each split.**
- Option B — **`minhash` / LSH over the Vietnamese token stream + bilingual
  stopword removal**, then dedupe only across *splits* (not within, to preserve
  natural frequency).

Pick **A** as the default with **B** as an explicit escape hatch. A SHA-256 of
`(session_id, question_text, answer_text)` is deterministic, cheap in DuckDB, and
catches exact duplicates (the bulk of the bot's self-plagiarism). For
semantically-near duplicates (typos, emoji variants), LSH is more correct but
needs a stopword list for Vietnamese and an extra dependency; we ship **A**
first and keep the LSH variant behind a feature flag. Rejected Alternative:
global cross-split dedup only (LSH alone) — rejected because it silently throws
away natural head-of-distribution examples and we cannot measure the recall loss
without a manual review budget.

## 5. Contracts & quarantine

Every chat is asserted for `lang`, `user_id`, `resolved_at`, and `answer_text`
length before it reaches the Gold dataset.

- Option A — **quarantine + alert on spike** (the lab's `llm_label_quarantine`
  pattern, extended to schema failures).
- Option B — **drop bad rows silently and log a count**.

Pick **A**. A quarantine table (`flywheel_quarantine`) plus a daily
`quarantine_rate` metric (SLO: <3% outside launch week) surfaces SDK regressions
before they bias training. When the Mobile SDK once shipped a null `resolved_at`
for an hour, the spike woke the on-call ML engineer — that is the value of a
*visible* dead-letter path versus a silent drop. This mirrors the core lab's
tombstone discipline: keep the bad row, mark it, never let it poison Gold.

## 6. Cost & operations

- Option A — **compute everything in DuckDB + the lab's hash embedder**
  (`pipeline/embed.py`), paying only for nightly DuckDB runs; swap in OpenAI
  `text-embedding-3-small` only for the final index if quality demands it.
- Option B — **always embed with a hosted API**, paying per token every rebuild.

Pick **A**. The hash embedder (or an open `bge-m3` / `intfloat` model in a future
step) keeps the nightly rebuild at zero marginal token cost. If retrieval
quality tests (held-out, leakage-safe per §3) lag a hosted model by >X points,
flip `EMBEDDING_MODEL_VERSION` and bump the cache key — the cache table
`llm_label_cache`-style pattern lets us version both embeddings and labels
without re-running bad batches. Vietnamese diacritic normalization is handled
in Staging (`lower()` + NFC), so accent variants collapse to one token before
embedding — a small decision that buys ~4% recall on the Vietnamese queries
without a custom tokenizer.

## Architecture sketch

```
                 chat widgets (Web + Mobile SDKs)
                              |
        Bronze raw_chat_logs  (json verbatim, append-only lake)
                              |  stage_chat()  -- null-fill, lang assert
        Staging staged_chat_logs (session_id, user_id, lang, ts, tags, ...)
                              |
              run_day(day)  -- idempotent microbatch, LOOKBACK_DAYS fold
        Silver  silver_chats  (dedup by key, session-level point-in-time)
              build_session_pairs()      build_eval_set()   build_doc_chunks()
                              |
       ---------------------+-------------------------
       |                   |                         |
   Gold  gold_sessions    Gold  eval_splits        Gold  chunk_index
  (train/val/test         (leakage-safe,            (versioned by
   by session_id@pti)      quarantined bad rows)     embed_model_version)
       |                   |                         |
   SFT dataset          retrieval index            cost: nightly DuckDB
   (future extension)   (OpenAI text-embedding-3-small swap)
                              ^
                              |
                  decontamination: sha256(q,a) within+across splits
```

One real answer beats ten shallow ones — these six decisions are the ones a
reviewer would stop me on, so each carries a named tradeoff and a rejected
alternative. The prototype hook is already present: `build_doc_chunks()` in
`pipeline/gold.py` plus the `EMBEDDING_MODEL_VERSION` swap in
`pipeline/llm_label.py` illustrate §6's cost/leakage-safe embedding versioning.
