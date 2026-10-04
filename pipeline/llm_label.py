"""BONUS — LLM is just another transform step. Slide "LLM là một bước transform".

The support team wants an LLM pre-triage label on every live ticket
(`gold_ticket_labels`), to compare with the human `category`. An LLM step is a
transform like any other — except it is expensive, slow and not deterministic,
so the slide's four rules apply:

  1. key = hash(input) + model + prompt version  ->  a re-run makes 0 LLM calls;
     changing the prompt re-labels everything ON PURPOSE
  2. force structured output, validate it; invalid -> quarantine, never Gold
  3. estimate the cost BEFORE running (rows x tokens x price)
  4. LLM labels are versioned data (model + prompt_version stored on every row)

This module implements the cache (rule 1), schema validation + quarantine
(rule 2) and an `OpenAILLM` provider (rule 4) that is opt-in via .env — the
default stays zero-key (`FakeLLM`) so the grader never needs an API key.

Generation model for the real provider: openai/o4-mini (this lab's host model).
Embedding model referenced elsewhere: OpenAI text-embedding-3-small.
"""
from __future__ import annotations

import hashlib
import json
import os
import re

import duckdb

MODEL = "fake-llm-2026-09"
PROMPT_VERSION = "triage-v1"
ALLOWED_LABELS = ("bug", "billing", "other")
PRICE_PER_1K_TOKENS_USD = 0.002          # pretend price for the cost estimate

# Real embedding model available in this environment (OpenAI). The zero-key
# hash embedder in pipeline/embed.py is used by the grader; swap this in for
# B2 / the optional extension exercise and bump EMBEDDING_MODEL_VERSION.
EMBEDDING_MODEL_VERSION = "text-embedding-3-small"


PROMPT_TEMPLATE = """You triage customer-support tickets.
Answer ONLY with JSON: {{\"label\": \"bug\" | \"billing\" | \"other\"}}.
Ticket: {text}"""


class FakeLLM:
    """Deterministic stand-in for a chat model. Counts calls and tokens."""

    def __init__(self, model: str = MODEL) -> None:
        self.model = model
        self.calls = 0
        self.tokens = 0

    def complete(self, prompt: str) -> str:
        self.calls += 1
        self.tokens += len(prompt.split()) + 8
        text = prompt.lower()
        if "xuất" in text:
            return 'Sure! Here is the label: {"label": "export"}'   # off-schema
        if re.search(r"crash|lỗi|sso|đăng nhập|chatbot", text):
            return '{"label": "bug"}'
        if re.search(r"tiền|hoá đơn|thanh toán|gói|vat", text):
            return '{"label": "billing"}'
        return '{"label": "other"}'


class OpenAILLM:
    """Real provider — used only when OPENAI_API_KEY is set in .env.

    Host generation model for this lab: openai/o4-mini. Counts as one
    LLM call per uncached ticket, exactly like FakeLLM.
    """

    def __init__(self, model: str = "o4-mini") -> None:
        import openai
        self.client = openai.OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        self.model = model
        self.calls = 0
        self.tokens = 0

    def complete(self, prompt: str) -> str:
        self.calls += 1
        self.tokens += len(prompt.split()) + 8
        resp = self.client.chat.completions.create(
            model=self.model, messages=[{"role": "user", "content": prompt}])
        return resp.choices[0].message.content


def _hash(text: str) -> str:
    """Cache key for one input = stable digest of the text. Combined with model
    and prompt_version in the SQL key, this is the lab's 'hash(input) + model +
    prompt version' cache identity."""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def estimate_tokens(texts: list[str]) -> int:
    return sum(len(PROMPT_TEMPLATE.format(text=t).split()) + 8 for t in texts)


def parse_label(raw: str) -> str | None:
    """Pull {"label": ...} out of the model's answer; None if not valid."""
    m = re.search(r"\{.*\}", raw, flags=re.S)
    if not m:
        return None
    try:
        label = json.loads(m.group(0)).get("label")
    except json.JSONDecodeError:
        return None
    return label if label in ALLOWED_LABELS else None


def live_tickets(con: duckdb.DuckDBPyConnection) -> list[tuple[str, str]]:
    return con.execute("""
        SELECT ticket_id, subject || '. ' || body AS text
        FROM silver_tickets
        WHERE NOT is_deleted
        ORDER BY ticket_id
    """).fetchall()


def _ensure_cache_tables(con: duckdb.DuckDBPyConnection) -> None:
    """Cache + quarantine tables, created once and reused across re-runs.

    The cache key is (text_hash, model, prompt_version) so:
      * same model + same prompt + same input  -> cache hit, 0 LLM calls
      * a new prompt_version forces a cache miss on purpose -> re-label
      * swapping the model forces re-label too (different embedding space)
    """
    con.execute("""CREATE TABLE IF NOT EXISTS llm_label_cache (
        text_hash VARCHAR, model VARCHAR, prompt_version VARCHAR,
        label VARCHAR, raw_response VARCHAR,
        PRIMARY KEY (text_hash, model, prompt_version))""")
    con.execute("""CREATE TABLE IF NOT EXISTS llm_label_quarantine (
        ticket_id VARCHAR, text_hash VARCHAR, model VARCHAR,
        prompt_version VARCHAR, reason VARCHAR, raw_response VARCHAR)""")


def label_tickets(con: duckdb.DuckDBPyConnection, llm: FakeLLM | OpenAILLM) -> dict:
    """Cached, schema-validated LLM triage — the BONUS B1 implementation.

    For every live ticket:
      1. compute text_hash; if (hash, model, prompt_version) is cached -> reuse label
      2. only if uncached, call the LLM (rule 1: 0 calls on a re-run)
      3. parse + validate the label against ALLOWED_LABELS (rule 2: off-schema -> quarantine)
      4. store the label + model + prompt_version on every row (rule 4: versioned)
    """
    _ensure_cache_tables(con)
    rows = list(live_tickets(con))
    est = estimate_tokens([t for _, t in rows])
    print(f"cost estimate before running: ~{est} tokens = "
          f"${est / 1000 * PRICE_PER_1K_TOKENS_USD:.4f} per full run")

    # ensure the Gold labels table exists before truncating it
    con.execute("""CREATE TABLE IF NOT EXISTS gold_ticket_labels (
        ticket_id VARCHAR, label VARCHAR, model VARCHAR, prompt_version VARCHAR)""")
    # this model's latest snapshot replaces every previous version so the Gold
    # table holds a single, current prompt_version per model
    con.execute("DELETE FROM gold_ticket_labels WHERE model = ?", [llm.model])

    # pull the already-cached outcomes for *this* model + prompt version.
    # The cache stores BOTH valid labels and quarantined raw answers: a cached
    # outcome (even a rejected one) is a cache HIT -> 0 LLM calls on a re-run.
    cached = {r[0]: r[1] for r in con.execute(
        "SELECT text_hash, label FROM llm_label_cache "
        "WHERE model = ? AND prompt_version = ?", [llm.model, PROMPT_VERSION]).fetchall()}

    labels: list[tuple[str, str, str, str]] = []
    quarantined: list[tuple[str, str, str, str, str]] = []
    cached_hit = 0
    for ticket_id, text in rows:
        h = _hash(text)
        if h in cached:
            cached_hit += 1
            label = cached[h]
            if label is not None:
                labels.append((ticket_id, label, llm.model, PROMPT_VERSION))
            else:
                quarantined.append((ticket_id, h, llm.model, PROMPT_VERSION,
                                    "label not in allowed set (cached)"))
            continue
        raw = llm.complete(PROMPT_TEMPLATE.format(text=text))
        label = parse_label(raw)
        if label is None:
            # cache the rejected answer so a re-run does 0 LLM calls for it
            con.execute(
                "INSERT OR REPLACE INTO llm_label_cache "
                "(text_hash, model, prompt_version, label, raw_response) "
                "VALUES (?, ?, ?, ?, ?)",
                [h, llm.model, PROMPT_VERSION, None, raw])
            quarantined.append((ticket_id, h, llm.model, PROMPT_VERSION,
                                "label not in allowed set"))
            continue
        con.execute(
            "INSERT OR REPLACE INTO llm_label_cache "
            "(text_hash, model, prompt_version, label, raw_response) "
            "VALUES (?, ?, ?, ?, ?)",
            [h, llm.model, PROMPT_VERSION, label, raw])
        labels.append((ticket_id, label, llm.model, PROMPT_VERSION))

    if labels:
        con.executemany("INSERT INTO gold_ticket_labels VALUES (?, ?, ?, ?)", labels)

    if quarantined:
        con.executemany(
            "INSERT INTO llm_label_quarantine "
            "(ticket_id, text_hash, model, prompt_version, reason, raw_response) "
            "VALUES (?, ?, ?, ?, ?, NULL)", quarantined)

    return {"labeled": len(labels), "cached": cached_hit, "calls": llm.calls}
