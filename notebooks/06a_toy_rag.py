# Databricks notebook source
# MAGIC %md
# MAGIC # 06a · Toy exercise: chunking and retrieval  *(practice, about 15 min)*
# MAGIC
# MAGIC RAG = **R**etrieve the right passage, then **G**enerate an answer *only* from it. Almost every RAG failure is a
# MAGIC retrieval failure, so this exercise is all about the R.
# MAGIC
# MAGIC You will:
# MAGIC 1. **Chunk** a tiny policy into sections and see why chunk boundaries matter.
# MAGIC 2. Retrieve with **keywords (BM25)** and watch it fail on a paraphrased question.
# MAGIC 3. Retrieve with **embeddings** and watch it succeed.
# MAGIC 4. Add an **access-classification filter** and see a user lose access to a confidential chunk.

# COMMAND ----------

import os, sys
sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "src")))
import importlib, examiner.rag as rag
importlib.reload(rag)

TOY_DOC = """---
doc_id: TOY-001
title: Toy Access Policy
version: "1.0"
effective_date: 2026-01-01
owner: Toy Owner
access_classification: CONFIDENTIAL
---
## 1. Purpose
Explains how access is managed.
## 2. Reviews
Privileged access must be recertified at least every 90 days by the application owner.
## 3. Leavers
All access must be revoked within 1 business day of termination.
"""
TOY_DOC_2 = TOY_DOC.replace("TOY-001", "TOY-002").replace("Toy Access Policy", "Toy Expenses Policy") \
    .replace("CONFIDENTIAL", "INTERNAL") \
    .replace("Privileged access must be recertified at least every 90 days by the application owner.",
             "Expense claims must be submitted within 30 days with receipts.") \
    .replace("All access must be revoked within 1 business day of termination.",
             "Leavers must return corporate cards on their last day.")

chunks = rag.chunk_document(TOY_DOC, "toy1.md") + rag.chunk_document(TOY_DOC_2, "toy2.md")
for c in chunks:
    print(f"{c['chunk_id']:<14} {c['access_classification']:<13} | {c['chunk_text']}")

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 1 — what you just saw.** One chunk per numbered section. Each chunk carries the document's title, version,
# MAGIC owner and classification, so any answer can cite *exactly* where it came from. Look at `retrieval_text`: the title and
# MAGIC heading are prefixed, so a chunk that says only "every 90 days" is still findable by "review".

# COMMAND ----------

print(chunks[1]["retrieval_text"])

# COMMAND ----------

# MAGIC %md ### Step 2 — keyword retrieval (BM25): good on matching words, blind to synonyms

# COMMAND ----------

bm25 = rag.BM25Index(chunks)
for q in ["How often is privileged access recertified?",          # same words as the policy
          "How frequently do we re-check what admins can do?"]:   # same meaning, different words
    hits = bm25.search(q, k=2, clearance="CONFIDENTIAL", min_score=0.01, min_coverage=0)
    print(q, "\n   ", [(h["chunk_id"], h["score"]) for h in hits] or "NO MATCH", "\n")

# COMMAND ----------

# MAGIC %md ### Step 3 — embedding retrieval: compares meaning, not words
# MAGIC `ai_query` calls the embedding model behind Model Serving and returns a vector (about 1,024 numbers) per text.
# MAGIC Similar meaning gives vectors pointing the same way (cosine similarity close to 1).
# MAGIC *If this cell errors, put your embedding endpoint name in `EMBED`. Check **Serving** in the sidebar.*

# COMMAND ----------

import numpy as np
EMBED = "databricks-gte-large-en"

def embed(texts):
    df = spark.createDataFrame([(t,) for t in texts], "t STRING")
    return np.array([r.v for r in df.selectExpr(f"ai_query('{EMBED}', t) AS v").collect()])

chunk_vecs = embed([c["retrieval_text"] for c in chunks])
for q in ["How often is privileged access recertified?", "How frequently do we re-check what admins can do?"]:
    qv = embed([q])[0]
    sims = chunk_vecs @ qv / (np.linalg.norm(chunk_vecs, axis=1) * np.linalg.norm(qv))
    ranked = sorted(zip(sims, [c["chunk_id"] for c in chunks]), reverse=True)[:2]
    print(q, "\n   ", [(cid, round(float(s), 3)) for s, cid in ranked], "\n")

# COMMAND ----------

# MAGIC %md
# MAGIC **What to notice:** BM25 misses the paraphrase; embeddings rank `TOY-001::s2` first for both questions.
# MAGIC That's why the real notebook uses **Vector Search** (embeddings) and keeps BM25 as the fallback. Also look at the
# MAGIC *unrelated* chunks' similarity: it isn't 0. You still need a **minimum score** to say "not found".

# COMMAND ----------

# MAGIC %md ### Step 4 — access classification is enforced *before* ranking

# COMMAND ----------

q = "How often is privileged access recertified?"
for clearance in ["CONFIDENTIAL", "INTERNAL"]:
    res = rag.answer_question(q, lambda qq, k, cl: bm25.search(qq, k=k, clearance=cl), clearance=clearance)
    print(f"{clearance:<13} -> {res['status']}: {res['answer']}\n")

# COMMAND ----------

# MAGIC %md
# MAGIC **Check yourself before moving on:**
# MAGIC 1. Why chunk by *section* rather than every 500 characters? *(So a citation points to a real section number, and a
# MAGIC    requirement isn't cut in half.)*
# MAGIC 2. Why filter by classification *before* ranking instead of hiding results afterwards? *(A confidential chunk can then
# MAGIC    never reach the LLM prompt, and so can never leak into an answer.)*
# MAGIC 3. What should the assistant say when nothing scores high enough? *(That it can't find it. Never a guess.)*
