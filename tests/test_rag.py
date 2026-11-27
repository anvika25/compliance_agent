import os
import sys

import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(ROOT, "src"))
from examiner import rag  # noqa: E402

POLICY_DIR = os.path.join(ROOT, "resources", "policies")


@pytest.fixture(scope="module")
def chunks():
    out = []
    for f in sorted(os.listdir(POLICY_DIR)):
        out += rag.chunk_document(open(os.path.join(POLICY_DIR, f)).read(), f)
    return out


@pytest.fixture(scope="module")
def index(chunks):
    return rag.BM25Index(chunks)


def retriever(index):
    return lambda q, k, clearance: index.search(q, k=k, clearance=clearance)


def test_every_chunk_has_metadata(chunks):
    assert len({c["doc_id"] for c in chunks}) == 7
    for c in chunks:
        for f in ["title", "version", "effective_date", "owner", "access_classification", "section_number"]:
            assert c[f], (c["chunk_id"], f)


def test_empty_parent_headings_are_skipped(chunks):
    ids = {c["chunk_id"] for c in chunks}
    assert "POL-ACC-002::s4.1" in ids and "POL-ACC-002::s4" not in ids


def test_long_sections_split_with_overlap():
    doc = "---\ndoc_id: D\ntitle: T\nversion: 1\neffective_date: 2026-01-01\nowner: O\naccess_classification: INTERNAL\n---\n## 1. Big\n" + \
          "\n".join(f"Paragraph {i} " + "word " * 40 for i in range(10))
    cs = rag.chunk_document(doc, "d.md", max_chars=500, overlap_chars=80)
    assert len(cs) > 1 and all(c["char_count"] <= 700 for c in cs)
    assert cs[0]["chunk_id"].endswith("-p1")


def test_missing_metadata_is_rejected():
    with pytest.raises(ValueError):
        rag.chunk_document("---\ntitle: T\n---\n## 1. A\ntext", "bad.md")


def test_classification_filter_applies_before_ranking(index):
    internal = index.search("privileged access review frequency", clearance="INTERNAL")
    confidential = index.search("privileged access review frequency", clearance="CONFIDENTIAL")
    assert all(c["access_classification"] == "INTERNAL" for c in internal)
    assert confidential[0]["chunk_id"] == "POL-ACC-002::s4.1"


def test_unknown_citations_are_stripped(index):
    retrieved = index.search("closure evidence", k=2)
    out = rag.finalize("Root cause is required [S1]. Something invented [S9].", retrieved)
    assert out["invalid_citations"] == ["S9"] and not out["citation_check_passed"]
    assert "[S9]" not in out["answer"] and "POL-REM-005" in out["answer"]


def test_answer_without_citations_is_withheld(index):
    out = rag.finalize("Access must be reviewed monthly.", index.search("access review", k=2, clearance="CONFIDENTIAL"))
    assert out["status"] == "UNSUPPORTED"


@pytest.mark.parametrize("case", rag.EVAL_SET, ids=[c["id"] for c in rag.EVAL_SET])
def test_eval_set_extractive(index, case):
    res = rag.answer_question(case["question"], retriever(index), clearance=case.get("clearance", "INTERNAL"))
    ev = rag.evaluate_case(case, res)
    assert ev["answer_correct"], (case["id"], res["answer"], res["retrieved"])
    if not case.get("expect_not_found"):
        assert ev["retrieval_hit"] and ev["citation_accuracy"] >= 0.5 and ev["groundedness"] == 1.0
