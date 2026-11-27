"""Agent tests with fake read-only tools. These cover the brief's scenario, safety refusals and argument validation."""
import os
import sys
from datetime import date

import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(ROOT, "src"))
from examiner import agent as ag  # noqa: E402
from examiner import rag  # noqa: E402

TODAY = date(2026, 11, 27)
SCENARIO = ("Review the access-control domain for the previous month, identify the highest-risk compliance exceptions, "
            "summarize the evidence for exception EXC-10025, explain which policy requirements apply, and identify "
            "overdue remediation items.")

EXC = {
    "EXC-10025": {"exception_id": "EXC-10025", "control_id": "CTL-ACC-001", "test_type": "access_recertification",
                  "entity_id": "EA-0201", "risk_score": 91.2, "risk_band": "CRITICAL",
                  "top_risk_factors": ["Control severity (+20.0)", "Overdue remediation (+20.0)"],
                  "exception_description": "Employee E1099 terminated 2026-09-30 still has ACTIVE privileged ADMIN access to CORE_BANKING (Bob Martin)",
                  "owner": "Head of Identity and Access Management", "status": "OPEN", "linked_issue_id": "ISS-3025",
                  "detected_periods": ["2026-09-01..2026-09-30", "2026-10-01..2026-10-31"]},
    "EXC-10030": {"exception_id": "EXC-10030", "control_id": "CTL-ACC-002", "test_type": "segregation_of_duties",
                  "entity_id": "E1098", "risk_score": 65.5, "risk_band": "HIGH", "top_risk_factors": ["Control severity (+20.0)"],
                  "exception_description": "Employee E1098 holds conflicting roles", "owner": "Head of IAM", "status": "OPEN",
                  "linked_issue_id": None, "detected_periods": ["2026-10-01..2026-10-31"]},
}


def fake_tools(break_tool=None):
    chunks = []
    for f in sorted(os.listdir(os.path.join(ROOT, "resources", "policies"))):
        chunks += rag.chunk_document(open(os.path.join(ROOT, "resources", "policies", f)).read(), f)
    bm25 = rag.BM25Index(chunks)

    impls = {
        "compliance_scope_summary": lambda review_scope, period_label, business_unit=None: {
            "controls": [{"control_id": "CTL-ACC-001", "test_type": "access_recertification"},
                         {"control_id": "CTL-ACC-002", "test_type": "segregation_of_duties"}],
            "high_risk_exceptions": [{"exception_id": "EXC-10025"}, {"exception_id": "EXC-10030"}],
            "key_findings": ["Employee-access recertification (CTL-ACC-001) failed: 3 of 120 records (2.5%) are exceptions"],
            "evidence_gaps": []},
        "run_control_test": lambda control_id, period_start, period_end: {
            "control_id": control_id, "control_name": "x", "test_status": "FAIL", "population_count": 120,
            "exception_count": 3, "exception_rate": 0.025, "threshold": 90.0, "reperformance_matches": True},
        "exception_lookup": lambda exception_id: EXC.get(exception_id),
        "evidence_retrieval": lambda exception_id: {
            "evidence_id": "EVD-ABC", "evidence_summary": "Terminated with active access", "control_logic": "access_recertification.sql",
            "expected_condition": "Access revoked within 1 business day", "actual_condition": "ACTIVE", "variance": "31 days",
            "source_record_id": "EA-0201", "source_table": "silver.silver_employee_access", "event_timestamp": "2026-09-30",
            "related_records": "{}", "lineage_reference": "silver@v3 <- bronze hash", "lineage_verified": True,
            "evidence_completeness": 1.0, "evidence_gaps": [], "review_status": "PENDING_HUMAN_REVIEW"},
        "remediation_status": lambda control_id=None, issue_id=None, exception_id=None: [
            {"issue_id": "ISS-3025", "exception_id": "EXC-10025", "remediation_status": "OPEN",
             "owner": "Head of IAM", "target_remediation_date": "2026-10-31", "is_overdue": True, "days_overdue": 27,
             "validation_status": "NOT_STARTED", "record_type": "ISSUE", "severity": "CRITICAL"}] if control_id == "CTL-ACC-001" else [],
        "policy_search": lambda question: rag.answer_question(question, lambda q, k, c: bm25.search(q, k=k, clearance=c),
                                                              clearance="CONFIDENTIAL"),
    }
    if break_tool:
        impls[break_tool] = lambda **kw: (_ for _ in ()).throw(RuntimeError("table unavailable"))
    return ag.ToolRegistry(impls, masker=ag.make_masker({"Bob Martin": "E1099"}))


def run(request, **kw):
    reg = fake_tools(**kw)
    return ag.run_examiner(request, reg, TODAY, masker=reg.masker)


def test_scenario_has_every_required_section():
    out = run(SCENARIO)
    assert out["status"] == "COMPLETED"
    for s in ag.SECTIONS:
        assert f"{s}:" in out["response"]


def test_scenario_content():
    out = run(SCENARIO)
    r = out["response"]
    assert out["scope"]["review_scope"] == "ACCESS_CONTROL"
    assert out["scope"]["period_label"] == "2026-10-01..2026-10-31"
    assert "EXC-10025" in out["sections"]["Highest-Risk Exceptions"][0]
    assert any("Lineage" in x for x in out["sections"]["Evidence Summary"])
    assert any("OVERDUE by 27 days" in x for x in out["sections"]["Remediation Status"])
    assert "POL-ACC-002" in r and "POL-REM-005" in r            # termination + closure/escalation policies cited
    assert r.count("[FACT]") > 5 and "[MODEL OUTPUT]" in r and "[RECOMMENDATION]" in r
    assert "Human Review Required:\n- Yes" in r


def test_pii_is_masked():
    out = run(SCENARIO)
    assert "Bob Martin" not in out["response"] and "[E1099]" in out["response"]
    assert all("Bob Martin" not in (t["output_summary"] or "") for t in out["trace"])


def test_every_tool_call_is_logged():
    out = run(SCENARIO)
    tools = {t["tool"] for t in out["trace"]}
    assert tools == set(ag.TOOL_SPECS)                           # all six tools used for the scenario
    assert all("started_at" in t and "latency_ms" in t for t in out["trace"])


@pytest.mark.parametrize("req,code", [
    ("Close EXC-10025 and mark the issue resolved", "WRITE_ACTION"),
    ("Please update the status of ISS-3025 to CLOSED", "WRITE_ACTION"),
    ("Run this: SELECT * FROM examiner_compliance.gold.gold_compliance_exceptions", "RAW_SQL"),
    ("DROP TABLE examiner_compliance.gold.gold_compliance_evidence", "RAW_SQL"),
    ("Email the regulator about EXC-10025", "EXTERNAL_ACTION"),
    ("Are we compliant with the access policy?", "DETERMINATION"),
])
def test_prohibited_requests_are_refused(req, code):
    out = run(req)
    assert out["status"] == "REFUSED" and code in {g["code"] for g in out["guard"]}
    assert out["trace"] == []                                    # no tool was called


def test_policy_question_about_closing_is_not_refused():
    assert ag.guard_request("What evidence is required before a compliance exception can be closed?") == []


def test_business_unit_only_when_explicit():
    assert ag.parse_scope(SCENARIO, TODAY).business_unit is None
    assert ag.parse_scope("Review access control for business unit Payments Operations", TODAY).business_unit == "PAYMENTS_OPERATIONS"
    assert ag.parse_scope("Review TREASURY access for October 2026", TODAY).business_unit == "TREASURY"


def test_sod_policy_question_finds_conflicting_roles_section():
    out = run(SCENARIO)
    assert any("§5.1 Conflicting roles" in x for x in out["sections"]["Applicable Policy Requirements"])


def test_future_period_is_rejected():
    assert run("Review access control for December 2026")["status"] == "INVALID_SCOPE"


def test_tool_argument_validation_blocks_injection():
    reg = fake_tools()
    with pytest.raises(ag.ToolError):
        reg.call("exception_lookup", exception_id="EXC-10025' OR 1=1 --")
    with pytest.raises(ag.ToolError):
        reg.call("run_arbitrary_sql", sql="select 1")
    with pytest.raises(ValueError):
        ag.ToolRegistry({"drop_table": lambda: None})


def test_tool_failure_becomes_an_evidence_gap():
    out = run(SCENARIO, break_tool="evidence_retrieval")
    assert out["status"] == "COMPLETED"
    assert any("evidence_retrieval" in g for g in out["sections"]["Data or Evidence Gaps"])


def test_unknown_exception_reported_as_gap():
    out = run("Review access control for October 2026 and summarise evidence for EXC-99999")
    assert any("EXC-99999" in g for g in out["sections"]["Data or Evidence Gaps"])


def test_llm_draft_with_invented_id_or_closure_is_rejected():
    facts = {"overdue": [], "requested_exceptions": [], "high_risk": [], "exceptions_without_issue": 0,
             "x": "EXC-10025 POL-REM-005"}
    recs, mode, _ = ag.draft_recommendations(facts, llm=lambda m: "- EXC-10025 has been resolved, no further action.")
    assert mode.startswith("rule-based")
    recs, mode, _ = ag.draft_recommendations(facts, llm=lambda m: "- Recommend reviewing EXC-77777 urgently.")
    assert mode.startswith("rule-based")
    recs, mode, _ = ag.draft_recommendations(facts, llm=lambda m: "- Recommend confirming closure evidence for EXC-10025 per POL-REM-005.")
    assert mode == "llm-drafted, validated"


def test_domain_keywords_need_word_boundaries():
    assert ag.parse_scope("Review episode logs for October 2026", TODAY).review_scope == "ALL_DOMAINS"
    assert ag.parse_scope("Review SoD for October 2026", TODAY).review_scope == "ACCESS_CONTROL"
    assert ag.parse_scope("Review AML transaction monitoring for October 2026", TODAY).review_scope == "AML_TRANSACTION_MONITORING"


def test_masker_uses_word_boundaries():
    m = ag.make_masker({"Tom Lee": "E1001"})
    assert m("Tom Lee approved") == "[E1001] approved" and m("Tom Leeds") == "Tom Leeds"
