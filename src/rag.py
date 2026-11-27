"""
Policy RAG for Examiner: parse -> chunk -> enrich -> index -> retrieve -> grounded answer -> evaluate.

Pure Python (no Spark, no network), so every step is unit-testable. The notebook can swap the local BM25
retriever for Databricks Vector Search and the extractive answerer for a Model Serving LLM; both return
the same shapes, so citation checks and evaluation are identical either way.

Guardrails implemented here:
  * access classification filter BEFORE ranking (a user never retrieves a chunk above their clearance)
  * NOT_FOUND when retrieval support is too weak, instead of guessing
  * answers may only cite retrieved sources ([S1]..[Sn]); unknown citations are stripped and flagged
  * every citation renders as  [DOC-ID vX §section Title]
"""
from __future__ import annotations

import hashlib
import math
import re
from collections import Counter

CLEARANCE_RANK = {"PUBLIC": 0, "INTERNAL": 1, "CONFIDENTIAL": 2, "RESTRICTED": 3}
NOT_FOUND = ("I could not find this in the approved policy documents available to you. "
             "No requirement is stated here, to avoid inventing one. Please consult the policy owner.")

STOPWORDS = set("""a an and are as at be before by can do does for from has have how i if in is it its of on or
should that the their there this to under what when where which who will with would must may any all our we
you your apply applies""".split())


# --------------------------------------------------------------------------- parsing & chunking
def parse_front_matter(text: str) -> tuple[dict, str]:
    if not text.startswith("---"):
        return {}, text
    _, head, body = text.split("---", 2)
    meta = {}
    for line in head.strip().splitlines():
        if ":" in line:
            k, v = line.split(":", 1)
            meta[k.strip()] = v.strip().strip('"').strip("'")
    return meta, body


HEADING = re.compile(r"^(#{2,3})\s+(\d+(?:\.\d+)*)\.?\s+(.*)$")


def chunk_document(text: str, source_file: str, max_chars: int = 1200, overlap_chars: int = 150) -> list[dict]:
    """One chunk per numbered section (## or ###). Long sections split on paragraphs with overlap.

    Each chunk carries full document metadata (title, version, effective date, owner, classification)
    and a `retrieval_text` that prefixes title + heading path. That makes short sections findable by topic.
    """
    meta, body = parse_front_matter(text)
    required = ["doc_id", "title", "version", "effective_date", "owner", "access_classification"]
    missing = [k for k in required if not meta.get(k)]
    if missing:
        raise ValueError(f"{source_file}: missing front-matter fields {missing}")

    sections, parents, current = [], {}, None
    for line in body.splitlines():
        m = HEADING.match(line.strip())
        if m:
            level, number, title = len(m.group(1)), m.group(2), m.group(3).strip()
            if level == 2:
                parents = {"number": number, "title": title}
            path = title if level == 2 else f"{parents.get('number', '')} {parents.get('title', '')} > {number} {title}"
            current = {"section_number": number, "section_title": title, "heading_path": path.strip(), "lines": []}
            sections.append(current)
        elif current is not None:
            current["lines"].append(line)

    chunks = []
    for sec in sections:
        content = "\n".join(sec["lines"]).strip()
        if not content:
            continue                                     # e.g. "## 4." heading immediately followed by "### 4.1"
        pieces = _split(content, max_chars, overlap_chars)
        for i, piece in enumerate(pieces, 1):
            suffix = f"-p{i}" if len(pieces) > 1 else ""
            chunk_id = f"{meta['doc_id']}::s{sec['section_number']}{suffix}"
            retrieval_text = f"{meta['title']}. Section {sec['section_number']} {sec['heading_path']}. {piece}"
            chunks.append({
                "chunk_id": chunk_id, "doc_id": meta["doc_id"], "title": meta["title"], "version": meta["version"],
                "effective_date": meta["effective_date"], "owner": meta["owner"],
                "access_classification": meta["access_classification"].upper(),
                "section_number": sec["section_number"], "section_title": sec["section_title"],
                "heading_path": sec["heading_path"], "chunk_text": piece, "retrieval_text": retrieval_text,
                "char_count": len(piece), "source_file": source_file,
                "content_sha256": hashlib.sha256(piece.encode()).hexdigest(),
            })
    return chunks


def _split(text: str, max_chars: int, overlap: int) -> list[str]:
    if len(text) <= max_chars:
        return [text]
    paras, out, cur = [p for p in text.split("\n") if p.strip()], [], ""
    for p in paras:
        if cur and len(cur) + len(p) + 1 > max_chars:
            out.append(cur.strip())
            cur = cur[-overlap:] + "\n" + p                # carry tail of previous chunk for context
        else:
            cur = (cur + "\n" + p) if cur else p
    if cur.strip():
        out.append(cur.strip())
    return out


def citation_label(chunk: dict) -> str:
    return f"[{chunk['doc_id']} v{chunk['version']} §{chunk['section_number']} {chunk['section_title']}]"


# --------------------------------------------------------------------------- BM25 retriever
def tokenize(text: str) -> list[str]:
    toks = []
    for t in re.findall(r"[a-z0-9]+", text.lower()):
        if t in STOPWORDS:
            continue
        for suf in ("ure", "ing", "ed", "es", "s"):         # closure/closed -> clos, reviewed -> review
            if len(t) > 4 and t.endswith(suf):
                t = t[: -len(suf)]
                break
        toks.append(t)
    return toks


class BM25Index:
    """Small, dependency-free BM25 (Okapi). Fine for tens of thousands of chunks; Vector Search beyond that."""

    def __init__(self, chunks: list[dict], k1: float = 1.5, b: float = 0.75):
        self.chunks, self.k1, self.b = chunks, k1, b
        self.tfs = [Counter(tokenize(c["retrieval_text"])) for c in chunks]
        self.lens = [sum(tf.values()) for tf in self.tfs]
        self.avgdl = sum(self.lens) / max(len(self.lens), 1)
        df = Counter(t for tf in self.tfs for t in tf)
        n = len(chunks)
        self.idf = {t: math.log(1 + (n - d + 0.5) / (d + 0.5)) for t, d in df.items()}

    BOILERPLATE = {"purpose", "scope"}           # generic sections that match many questions; rank them lower
    BOILERPLATE_WEIGHT = 0.6

    def score(self, query: str) -> list[float]:
        q = tokenize(query)
        out = []
        for c, tf, dl in zip(self.chunks, self.tfs, self.lens):
            s = 0.0
            for t in q:
                if t in tf:
                    f = tf[t]
                    s += self.idf[t] * f * (self.k1 + 1) / (f + self.k1 * (1 - self.b + self.b * dl / self.avgdl))
            if c["section_title"].strip().lower() in self.BOILERPLATE:
                s *= self.BOILERPLATE_WEIGHT
            out.append(s)
        return out

    def coverage(self, query: str) -> list[float]:
        """idf-weighted share of the query's terms that appear in each chunk (0-1)."""
        q = set(tokenize(query))
        total = sum(self.idf.get(t, 0) for t in q) or 1.0
        return [sum(self.idf.get(t, 0) for t in q if t in tf) / total for tf in self.tfs]

    def search(self, query: str, k: int = 4, clearance: str = "INTERNAL",
               min_score: float = 2.0, min_coverage: float = 0.5) -> list[dict]:
        """Top-k chunks the caller is cleared for. Returns [] when even the best chunk covers less than
        `min_coverage` of the question's key terms. The caller then answers NOT_FOUND instead of guessing."""
        allowed = CLEARANCE_RANK.get(clearance.upper(), 0)
        scored = [(s, cov, c) for s, cov, c in zip(self.score(query), self.coverage(query), self.chunks)
                  if CLEARANCE_RANK.get(c["access_classification"], 99) <= allowed]   # filter BEFORE ranking
        scored = sorted(scored, key=lambda x: -x[0])
        if not scored or scored[0][1] < min_coverage:
            return []
        scored = [x for x in scored if x[0] >= min_score][:k]
        return [{**c, "score": round(s, 3), "coverage": round(cov, 3), "source_label": f"S{i}"}
                for i, (s, cov, c) in enumerate(scored, 1)]


# --------------------------------------------------------------------------- answering
SYSTEM_PROMPT = """You are the policy assistant of Examiner, a compliance decision-support tool.
Rules:
1. Answer ONLY from the numbered SOURCES provided. Do not use outside knowledge.
2. End every sentence with the source tag(s) it relies on, e.g. [S1] or [S1][S3].
3. Quote numbers, timeframes and thresholds exactly as written in the sources.
4. If the sources do not contain the answer, reply with exactly: NOT_FOUND
5. Do not add requirements, recommendations or legal conclusions that the sources do not state.
6. You support a human reviewer; you never make a final compliance determination."""


def build_messages(question: str, retrieved: list[dict]) -> list[dict]:
    sources = "\n\n".join(
        f"[{c['source_label']}] {c['title']} v{c['version']} (effective {c['effective_date']}), "
        f"section {c['section_number']} {c['section_title']}:\n{c['chunk_text']}" for c in retrieved)
    return [{"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": f"SOURCES:\n{sources}\n\nQUESTION: {question}"}]


def extractive_answer(question: str, retrieved: list[dict], max_chars: int = 700, near_ratio: float = 0.85) -> str:
    """Fallback when no LLM endpoint is available. Quotes the best-matching section verbatim (trimmed at a
    sentence boundary), plus the best sentence of any other section scoring within `near_ratio` of the top.
    Nothing is paraphrased, so the answer is grounded by construction."""
    if not retrieved:
        return "NOT_FOUND"
    q = set(tokenize(question))

    def sentences(text):
        return [s.strip() for s in re.split(r"(?<=[.;:])\s+|\n", text) if len(s.strip()) >= 12]

    top = retrieved[0]
    quoted, n = [], 0
    for s in sentences(top["chunk_text"]):
        if n + len(s) > max_chars and quoted:
            break
        quoted.append(s); n += len(s)
    parts = [" ".join(quoted) + f" [{top['source_label']}]"]
    for c in retrieved[1:3]:
        if c["score"] >= near_ratio * top["score"]:
            best = max(sentences(c["chunk_text"]), key=lambda s: len(q & set(tokenize(s))), default=None)
            if best and q & set(tokenize(best)):
                parts.append(f"{best} [{c['source_label']}]")
    return " ".join(parts)


def finalize(raw_answer: str, retrieved: list[dict]) -> dict:
    """Validate [S#] citations, render them, decide status."""
    labels = {c["source_label"]: c for c in retrieved}
    if not retrieved or raw_answer.strip().upper().startswith("NOT_FOUND"):
        return {"status": "NOT_FOUND", "answer": NOT_FOUND, "citations": [], "citation_check_passed": True,
                "invalid_citations": []}
    cited = re.findall(r"\[(S\d+)\]", raw_answer)
    invalid = sorted({c for c in cited if c not in labels})
    answer = raw_answer
    for lbl in invalid:
        answer = answer.replace(f"[{lbl}]", "")
    used = [labels[c] for c in dict.fromkeys(cited) if c in labels]
    for lbl, ch in labels.items():
        answer = answer.replace(f"[{lbl}]", citation_label(ch))
    return {
        "status": "ANSWERED" if used else "UNSUPPORTED",
        "answer": answer.strip() if used else "The draft answer had no valid citations and was withheld. " + NOT_FOUND,
        "citations": [{"doc_id": c["doc_id"], "title": c["title"], "version": c["version"],
                       "section": f"{c['section_number']} {c['section_title']}", "chunk_id": c["chunk_id"],
                       "access_classification": c["access_classification"]} for c in used],
        "citation_check_passed": not invalid and bool(used),
        "invalid_citations": invalid,
    }


def answer_question(question: str, retriever, clearance: str = "INTERNAL", llm=None, k: int = 4) -> dict:
    """retriever(question, k, clearance) -> chunks; llm(messages) -> str or None for extractive mode."""
    retrieved = retriever(question, k, clearance)
    if not retrieved:
        raw, mode = "NOT_FOUND", "no_retrieval"
    elif llm is not None:
        raw, mode = llm(build_messages(question, retrieved)), "llm"
    else:
        raw, mode = extractive_answer(question, retrieved), "extractive"
    out = finalize(raw, retrieved)
    out.update(question=question, clearance=clearance, mode=mode, raw_answer=raw,
               retrieved=[{"chunk_id": c["chunk_id"], "score": c["score"], "source_label": c["source_label"]}
                          for c in retrieved])
    out["_retrieved_chunks"] = retrieved
    return out


# --------------------------------------------------------------------------- evaluation
EVAL_SET = [
    {"id": "Q1", "question": "What evidence is required before a compliance exception can be closed?",
     "expected_sections": ["POL-REM-005::s4"], "expected_facts": ["root cause", "independent validation"],
     "expected_answer": "Root cause analysis, evidence the remediation was completed, independent second-line "
                        "validation, and sign-off by the issue owner and Compliance approver (POL-REM-005 §4)."},
    {"id": "Q2", "question": "What is the review frequency for privileged access?",
     "expected_sections": ["POL-ACC-002::s4.1"], "expected_facts": ["90 days"], "clearance": "CONFIDENTIAL",
     "expected_answer": "At least every 90 days (POL-ACC-002 §4.1)."},
    {"id": "Q3", "question": "What controls apply to transaction approval and release?",
     "expected_sections": ["POL-AML-001::s3.2"], "expected_facts": ["10,000", "initiator"],
     "expected_answer": "Transactions of CAD 10,000 or more need approval by someone other than the initiator "
                        "before release, with an approval reference; self-approval is prohibited (POL-AML-001 §3.2)."},
    {"id": "Q4", "question": "What is the escalation process for a critical exception?",
     "expected_sections": ["POL-REM-005::s6"], "expected_facts": ["24 hours"],
     "expected_answer": "Report to the control owner and CCO within 24 hours; Critical/High issues more than 30 days "
                        "overdue go to the Risk Committee (POL-REM-005 §6)."},
    {"id": "Q5", "question": "What human-review requirements apply to AI-generated findings?",
     "expected_sections": ["POL-HRV-007::s2", "POL-HRV-007::s3", "POL-HRV-007::s4"], "expected_facts": ["review"],
     "expected_answer": "Every AI finding must be reviewed by a qualified reviewer before use; the reviewer records "
                        "decision, name and timestamp; AI may not close issues (POL-HRV-007 §2-4)."},
    {"id": "Q6", "question": "How quickly must access be removed after an employee is terminated?",
     "expected_sections": ["POL-ACC-002::s4.3"], "expected_facts": ["1 business day"], "clearance": "CONFIDENTIAL",
     "expected_answer": "Within 1 business day of the termination date (POL-ACC-002 §4.3)."},
    {"id": "Q7", "question": "What is the employee dress code for the trading floor?",
     "expected_sections": [], "expected_facts": [], "expect_not_found": True,
     "expected_answer": "Not covered by the approved documents."},
    {"id": "Q8", "question": "What is the review frequency for privileged access?",
     "expected_sections": [], "expected_facts": [], "clearance": "INTERNAL", "expect_not_found": True,
     "expected_answer": "Not available at INTERNAL clearance (source is CONFIDENTIAL)."},
]


def groundedness(answer: str, retrieved: list[dict]) -> float:
    """Share of answer sentences whose content words are >= 60% present in the retrieved text (lexical check)."""
    source = set(tokenize(" ".join(c["chunk_text"] for c in retrieved)))
    text = re.sub(r"\[[^\]]+\]", "", answer)
    sents = [s for s in re.split(r"(?<=[.;])\s+", text) if len(tokenize(s)) >= 3]
    if not sents:
        return 1.0
    ok = sum(1 for s in sents if len(set(tokenize(s)) & source) / len(set(tokenize(s))) >= 0.6)
    return round(ok / len(sents), 3)


def evaluate_case(case: dict, result: dict) -> dict:
    retrieved_ids = [r["chunk_id"] for r in result["retrieved"]]
    cited_ids = [c["chunk_id"] for c in result["citations"]]
    exp = case["expected_sections"]
    is_exp = lambda cid: any(cid == e or cid.startswith(e + "-p") for e in exp)
    if case.get("expect_not_found"):
        correct = result["status"] == "NOT_FOUND"
        return {"retrieval_hit": None, "citation_accuracy": 1.0 if not cited_ids else 0.0,
                "groundedness": 1.0, "facts_found": None, "answer_correct": correct}
    facts = [f for f in case["expected_facts"] if f.lower() in result["answer"].lower()]
    return {
        "retrieval_hit": any(is_exp(c) for c in retrieved_ids),
        "citation_accuracy": round(sum(is_exp(c) for c in cited_ids) / len(cited_ids), 3) if cited_ids else 0.0,
        "groundedness": groundedness(result["answer"], result["_retrieved_chunks"]) if result["status"] == "ANSWERED" else 0.0,
        "facts_found": f"{len(facts)}/{len(case['expected_facts'])}",
        "answer_correct": result["status"] == "ANSWERED" and len(facts) == len(case["expected_facts"]),
    }
