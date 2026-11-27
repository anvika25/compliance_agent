# Databricks notebook source
# MAGIC %md
# MAGIC # 06 · Policy RAG assistant  *(Implemented: Vector Search + BM25 fallback; LLM optional)*
# MAGIC
# MAGIC ```
# MAGIC raw_files/policies/*.md ─► chunk by section + metadata ─► reference.policy_chunks (Delta, CDF on)
# MAGIC        ─► Vector Search delta-sync index serving.policy_chunks_index (managed embeddings)   [or BM25 fallback]
# MAGIC        ─► classification filter ─► top-k chunks ─► LLM (or extractive quote) ─► citation check ─► answer
# MAGIC        ─► monitoring.rag_query_log            evaluation ─► ml.rag_evaluation + MLflow
# MAGIC ```
# MAGIC Logic lives in `src/examiner/rag.py` (unit-tested). This notebook wires it to Databricks services.
# MAGIC
# MAGIC **Widgets**
# MAGIC * `retriever`: `vector_search` or `bm25`
# MAGIC * `embedding_endpoint`: your embedding endpoint from **Serving**
# MAGIC * `llm_endpoint`: a chat endpoint from **Serving**. Leave blank for extractive (quote-only) answers.
# MAGIC * `clearance`: simulates the requesting user's access level. Production would derive it from group membership (see the last section).

# COMMAND ----------

# MAGIC %pip install -q databricks-ai-search
# MAGIC %restart_python

# COMMAND ----------

dbutils.widgets.dropdown("retriever", "vector_search", ["vector_search", "bm25"], "Retriever")
dbutils.widgets.text("embedding_endpoint", "databricks-gte-large-en", "Embedding endpoint")
dbutils.widgets.text("llm_endpoint", "", "Chat endpoint (blank = extractive)")
dbutils.widgets.text("vs_endpoint", "examiner_vs", "Vector Search endpoint")
dbutils.widgets.text("vs_min_score", "0.55", "VS min similarity for an answer")
dbutils.widgets.dropdown("clearance", "CONFIDENTIAL", ["PUBLIC", "INTERNAL", "CONFIDENTIAL", "RESTRICTED"], "Requester clearance")

RETRIEVER = dbutils.widgets.get("retriever")
EMBED_EP = dbutils.widgets.get("embedding_endpoint").strip()
LLM_EP = dbutils.widgets.get("llm_endpoint").strip()
VS_EP = dbutils.widgets.get("vs_endpoint").strip()
VS_MIN = float(dbutils.widgets.get("vs_min_score"))
CLEARANCE = dbutils.widgets.get("clearance")

import os, sys, glob, json, time, uuid, importlib
from datetime import datetime, timezone
import pandas as pd
from delta.tables import DeltaTable

REPO_ROOT = os.path.abspath(os.path.join(os.getcwd(), ".."))
sys.path.insert(0, os.path.join(REPO_ROOT, "src"))
import examiner.rag as rag
importlib.reload(rag)

C = "examiner_compliance"
CHUNKS_T = f"{C}.reference.policy_chunks"
INDEX_NAME = f"{C}.serving.policy_chunks_index"
POLICY_DIR = f"/Volumes/{C}/reference/raw_files/policies"
USER = spark.sql("SELECT current_user()").first()[0]

# COMMAND ----------

# MAGIC %md ## 1 · Extract, chunk, enrich → `reference.policy_chunks`  (idempotent MERGE on chunk_id)

# COMMAND ----------

chunks = []
for path in sorted(glob.glob(f"{POLICY_DIR}/*.md")):
    with open(path) as f:
        chunks += rag.chunk_document(f.read(), source_file=path)
print(f"{len(chunks)} chunks from {len({c['doc_id'] for c in chunks})} documents")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CHUNKS_T} (
  chunk_id STRING NOT NULL, doc_id STRING, title STRING, version STRING, effective_date STRING, owner STRING,
  access_classification STRING, section_number STRING, section_title STRING, heading_path STRING,
  chunk_text STRING, retrieval_text STRING, char_count INT, source_file STRING, content_sha256 STRING,
  ingested_at TIMESTAMP)
COMMENT 'Approved policy text chunked by section. Source for the Vector Search index.'
TBLPROPERTIES (delta.enableChangeDataFeed = true)""")       # CDF is required for a delta-sync index

target_schema = spark.table(CHUNKS_T).schema
src = (spark.createDataFrame(pd.DataFrame(chunks))
       .selectExpr("*", "current_timestamp() AS ingested_at")
       .selectExpr(*[f"CAST({f.name} AS {f.dataType.simpleString()}) AS {f.name}" for f in target_schema]))
(DeltaTable.forName(spark, CHUNKS_T).alias("t")
    .merge(src.alias("s"), "t.chunk_id = s.chunk_id")
    .whenMatchedUpdateAll(condition="t.content_sha256 <> s.content_sha256 OR t.version <> s.version")
    .whenNotMatchedInsertAll()
    .whenNotMatchedBySourceDelete()                  # a removed section must not stay retrievable
    .execute())
display(spark.table(CHUNKS_T).groupBy("doc_id", "version", "access_classification").count().orderBy("doc_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Vector Search index  *(skipped when `retriever = bm25`)*
# MAGIC The first run creates the endpoint (**about 5–15 min**) and the index (a few min). Later runs just `sync()`.
# MAGIC Embeddings are computed by Databricks from `retrieval_text` with your embedding endpoint ("managed embeddings").

# COMMAND ----------

index = None
if RETRIEVER == "vector_search":
    try:
        from databricks.ai_search.client import VectorSearchClient           # new package name
    except ImportError:
        from databricks.vector_search.client import VectorSearchClient       # older name
    vsc = VectorSearchClient(disable_notice=True)

    existing = [e["name"] for e in vsc.list_endpoints().get("endpoints", [])]
    if VS_EP not in existing:
        print("Creating endpoint", VS_EP, "(this takes a while)...")
        vsc.create_endpoint(name=VS_EP, endpoint_type="STANDARD")
    vsc.wait_for_endpoint(VS_EP, verbose=True)

    indexes = [i["name"] for i in vsc.list_indexes(VS_EP).get("vector_indexes", [])]
    if INDEX_NAME not in indexes:
        index = vsc.create_delta_sync_index(
            endpoint_name=VS_EP, index_name=INDEX_NAME, source_table_name=CHUNKS_T,
            pipeline_type="TRIGGERED", primary_key="chunk_id",
            embedding_source_column="retrieval_text", embedding_model_endpoint_name=EMBED_EP)
    else:
        index = vsc.get_index(VS_EP, INDEX_NAME)
        index.sync()
    index.wait_until_ready(verbose=True, timeout=1800)
    print(index.describe().get("status", {}))

# COMMAND ----------

# MAGIC %md ## 3 · Retrievers (same output shape) and the answer generator

# COMMAND ----------

bm25 = rag.BM25Index(chunks)
RETURN_COLS = ["chunk_id", "doc_id", "title", "version", "effective_date", "owner", "access_classification",
               "section_number", "section_title", "heading_path", "chunk_text"]

def allowed_classes(clearance):
    rank = rag.CLEARANCE_RANK[clearance]
    return [c for c, r in rag.CLEARANCE_RANK.items() if r <= rank]

def vs_retrieve(question, k, clearance):
    res = index.similarity_search(
        query_text=question, columns=RETURN_COLS, num_results=k,
        filters={"access_classification": allowed_classes(clearance)})      # filter inside the index query
    names = [c["name"] for c in res["manifest"]["columns"]]
    rows = [dict(zip(names, r)) for r in res.get("result", {}).get("data_array", [])]
    rows = [r for r in rows if r["score"] >= VS_MIN]
    return [{**r, "score": round(float(r["score"]), 4), "source_label": f"S{i}"} for i, r in enumerate(rows, 1)]

def bm25_retrieve(question, k, clearance):
    return bm25.search(question, k=k, clearance=clearance)

retrieve = vs_retrieve if (RETRIEVER == "vector_search" and index is not None) else bm25_retrieve

llm = None
if LLM_EP:
    from mlflow.deployments import get_deploy_client
    _client = get_deploy_client("databricks")
    def llm(messages):
        resp = _client.predict(endpoint=LLM_EP, inputs={"messages": messages, "temperature": 0.0, "max_tokens": 500})
        return resp["choices"][0]["message"]["content"]

print("retriever:", retrieve.__name__, "| generator:", LLM_EP or "extractive (quote-only)")

# COMMAND ----------

# MAGIC %md ## 4 · `policy_search()`: the function the Examiner agent will call. Every call is logged.

# COMMAND ----------

LOG_T = f"{C}.monitoring.rag_query_log"
spark.sql(f"""CREATE TABLE IF NOT EXISTS {LOG_T} (
  query_id STRING, asked_at TIMESTAMP, asked_by STRING, clearance STRING, question STRING, retriever STRING,
  generator STRING, status STRING, answer STRING, citations STRING, retrieved STRING, citation_check_passed BOOLEAN,
  invalid_citations ARRAY<STRING>, latency_ms BIGINT, error STRING)
COMMENT 'Every policy question: prompt, retrieval, answer, citations, latency. Feeds the platform dashboard.'""")

def policy_search(question: str, clearance: str = CLEARANCE, k: int = 4, log: bool = True) -> dict:
    t0, err = time.time(), None
    try:
        out = rag.answer_question(question, retrieve, clearance=clearance, llm=llm, k=k)
    except Exception as e:                                    # LLM outage -> grounded extractive answer, flagged
        err = f"{type(e).__name__}: {e}"[:500]
        out = rag.answer_question(question, retrieve, clearance=clearance, llm=None, k=k)
        out["mode"] = "extractive_fallback"
    out["latency_ms"] = int((time.time() - t0) * 1000)
    out["query_id"] = f"Q-{uuid.uuid4().hex[:12]}"
    if log:
        spark.createDataFrame([(
            out["query_id"], datetime.now(timezone.utc), USER, clearance, question, retrieve.__name__,
            LLM_EP or "extractive", out["status"], out["answer"], json.dumps(out["citations"]),
            json.dumps(out["retrieved"]), out["citation_check_passed"], out["invalid_citations"],
            out["latency_ms"], err)], spark.table(LOG_T).schema).write.mode("append").saveAsTable(LOG_T)
    return out

def show(out):
    print(f"Q: {out['question']}\n[{out['status']} | {out['mode']} | {out['latency_ms']} ms]\n{out['answer']}")
    for c in out["citations"]:
        print(f"   - {c['doc_id']} v{c['version']} §{c['section']} ({c['access_classification']})")
    print()

for q in ["What evidence is required before a compliance exception can be closed?",
          "What is the review frequency for privileged access?",
          "What controls apply to transaction approval and release?",
          "What is the escalation process for a critical exception?",
          "What human-review requirements apply to AI-generated findings?"]:
    show(policy_search(q))

# COMMAND ----------

# MAGIC %md ### Guardrail checks: out-of-scope question, and a user without CONFIDENTIAL clearance

# COMMAND ----------

show(policy_search("What is the employee dress code for the trading floor?"))
show(policy_search("What is the review frequency for privileged access?", clearance="INTERNAL"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · Evaluation → `ml.rag_evaluation` + MLflow
# MAGIC 8 questions: the brief's 5, one extra, one out of scope, and one access-denied. Recorded per question: expected answer,
# MAGIC expected vs retrieved sections, generated response, **citation accuracy** (share of citations that point to an expected
# MAGIC section) and **groundedness** (share of answer sentences supported by the retrieved text, a lexical check).
# MAGIC The current retriever is compared with the BM25 baseline.
# MAGIC **Tuning:** if Q7 (dress code) is *answered* under `vector_search`, raise `vs_min_score` (try 0.6–0.7) until it returns
# MAGIC NOT_FOUND while Q1–Q6 still pass. That threshold is your "unsupported-answer" control.

# COMMAND ----------

import mlflow
mlflow.set_experiment(f"/Users/{USER}/examiner_rag_eval")
EVAL_RUN = f"RAGEVAL-{datetime.now(timezone.utc):%Y%m%dT%H%M%S}"
rows = []
retrievers = {retrieve.__name__: retrieve} | ({"bm25_retrieve": bm25_retrieve} if retrieve is not bm25_retrieve else {})
for name, fn in retrievers.items():
    for case in rag.EVAL_SET:
        t0 = time.time()
        res = rag.answer_question(case["question"], fn, clearance=case.get("clearance", "CONFIDENTIAL"), llm=llm)
        ev = rag.evaluate_case(case, res)
        rows.append({
            "eval_run_id": EVAL_RUN, "retriever": name, "generator": LLM_EP or "extractive",
            "question_id": case["id"], "question": case["question"], "clearance": case.get("clearance", "CONFIDENTIAL"),
            "expected_answer": case["expected_answer"], "expected_sections": ", ".join(case["expected_sections"]) or "(none: expect NOT_FOUND)",
            "retrieved_sections": ", ".join(r["chunk_id"] for r in res["retrieved"]),
            "generated_response": res["answer"], "status": res["status"],
            "retrieval_hit": ev["retrieval_hit"], "citation_accuracy": ev["citation_accuracy"],
            "groundedness": ev["groundedness"], "facts_found": ev["facts_found"], "answer_correct": ev["answer_correct"],
            "latency_ms": int((time.time() - t0) * 1000), "evaluated_at": datetime.now(timezone.utc)})

eval_pdf = pd.DataFrame(rows)
spark.createDataFrame(eval_pdf).write.mode("append").option("mergeSchema", True).saveAsTable(f"{C}.ml.rag_evaluation")

with mlflow.start_run(run_name=EVAL_RUN):
    mlflow.log_params({"retriever": retrieve.__name__, "generator": LLM_EP or "extractive", "embedding_endpoint": EMBED_EP,
                       "vs_min_score": VS_MIN, "k": 4, "n_questions": len(rag.EVAL_SET)})
    for name, g in eval_pdf.groupby("retriever"):
        answerable = g[~g.expected_sections.str.startswith("(none")]
        mlflow.log_metrics({
            f"{name}_answer_accuracy": g.answer_correct.mean(),
            f"{name}_retrieval_hit_rate": answerable.retrieval_hit.astype(float).mean(),
            f"{name}_citation_accuracy": answerable.citation_accuracy.mean(),
            f"{name}_groundedness": answerable.groundedness.mean(),
            f"{name}_unsupported_rate": (g.status == "UNSUPPORTED").mean()})
    mlflow.log_table(eval_pdf.drop(columns=["evaluated_at"]), "rag_evaluation.json")

display(spark.table(f"{C}.ml.rag_evaluation").filter(f"eval_run_id = '{EVAL_RUN}'")
        .select("retriever", "question_id", "status", "retrieval_hit", "citation_accuracy", "groundedness",
                "answer_correct", "retrieved_sections", "generated_response"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 · Production design  *(Designed)*
# MAGIC * **Clearance from identity, not a widget.** Put a row filter on `reference.policy_chunks`, e.g.
# MAGIC   `access_classification = 'INTERNAL' OR is_account_group_member('compliance_confidential')`, and have the agent query
# MAGIC   the index with the caller's credentials (on-behalf-of-user auth), so the filter can't be bypassed.
# MAGIC * **Serving.** Wrap `policy_search` as a Unity Catalog function or an MLflow `ChatAgent`, deploy with Agent Framework to
# MAGIC   Model Serving, and route LLM traffic through **AI Gateway** (rate limits, PII guardrails, payload/usage logging).
# MAGIC * **Evaluation.** Add an LLM-judge groundedness/relevance scorer (MLflow GenAI evaluation) next to the lexical metrics,
# MAGIC   and a reviewer-labelled question set.
# MAGIC * **Freshness.** A Workflow task reruns sections 1–2 when files land in `raw_files/policies/`. The MERGE deletes
# MAGIC   removed sections, and old versions stay in Delta history for audit.
