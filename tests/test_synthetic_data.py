"""Unit tests: the generator is deterministic and every planted violation is present.

Run locally:  pip install pandas pytest && pytest tests/ -q
"""
import json
import os
import sys

import pandas as pd
import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(ROOT, "src"))
from examiner import synthetic_data as sd  # noqa: E402


@pytest.fixture(scope="module")
def raw(tmp_path_factory):
    d = tmp_path_factory.mktemp("raw")
    sd.build_all(str(d), policies_src=os.path.join(ROOT, "resources", "policies"))
    return d


def csv(raw, name):
    return pd.read_csv(raw / name, dtype=str, keep_default_na=False).replace("", None)


def jsonl(path):
    return pd.DataFrame([json.loads(l) for l in open(path)])


def test_deterministic(tmp_path):
    a, b = tmp_path / "a", tmp_path / "b"
    sd.build_all(str(a)); sd.build_all(str(b))
    for f in ["customer_account.csv", "employee_access.csv", "transactions/transactions_2026_10.json"]:
        assert (a / f).read_bytes() == (b / f).read_bytes()


def test_bob_terminated_with_active_access(raw):
    ea = csv(raw, "employee_access.csv")
    bob = ea[ea.employee_id == "E1099"].set_index("access_id")
    assert (bob.termination_date == "2026-09-30").all()
    assert bob.loc["EA-0201", "access_status"] == "ACTIVE" and bob.loc["EA-0201", "privileged_access_flag"] == "Y"
    assert bob.loc["EA-0202", "access_status"] == "ACTIVE"
    assert bob.loc["EA-0203", "access_status"] == "REVOKED"


def test_only_planted_terminated_access(raw):
    ea = csv(raw, "employee_access.csv").drop_duplicates()
    bad = ea[ea.termination_date.notna() & (ea.access_status == "ACTIVE")]
    assert set(bad.access_id) == {"EA-0201", "EA-0202"}


def test_sod_conflict_only_alice(raw):
    ea = csv(raw, "employee_access.csv").drop_duplicates()
    act = ea[ea.access_status == "ACTIVE"]
    roles = act.groupby("employee_id").access_role.apply(set)
    pay = [e for e, r in roles.items() if {"PAYMENT_INITIATOR", "PAYMENT_APPROVER"} <= r]
    gl = [e for e, r in roles.items() if {"JOURNAL_PREPARER", "JOURNAL_APPROVER"} <= r]
    assert pay == ["E1098"] and gl == []


def test_exc_10025_issue_is_overdue(raw):
    iss = csv(raw, "compliance_issues.csv").set_index("issue_id")
    r = iss.loc["ISS-3025"]
    assert r.exception_id == "EXC-10025" and r.entity_id == "EA-0201" and r.control_id == "CTL-ACC-001"
    assert r.issue_status.upper() == "OPEN" and r.target_remediation_date < sd.AS_OF.isoformat()


def test_planted_transactions(raw):
    t = jsonl(raw / "transactions" / "transactions_2026_10.json").drop_duplicates().set_index("transaction_id", drop=False)
    assert pd.isna(t.loc["TXN-2026109001", "approved_by"])
    assert t.loc["TXN-2026109002", "approved_by"] == t.loc["TXN-2026109002", "initiated_by"] == "E1098"
    assert t.loc["TXN-2026109007", "country_code"] == "IR"
    assert t.loc["TXN-2026109008", "account_id"] == "ACC-900001"
    deposits = t[t.account_id == "ACC-900002"]
    assert len(deposits) == 4 and deposits.amount.astype(float).between(9000, 9999.99).all()


def test_no_unplanted_threshold_breaches(raw):
    rates = {"CAD": 1.0, "USD": 1.38}
    for m in ["08", "09", "10"]:
        t = jsonl(raw / "transactions" / f"transactions_2026_{m}.json")
        t = t[t.transaction_id.notna() & (t.amount != "N/A")]
        ccy = t.currency_code.str.strip().str.upper().replace({"C$": "CAD", "US$": "USD"})
        cad = t.amount.astype(float) * ccy.map(rates)
        bad = t[(cad >= 10000) & (t.approved_by.isna() | (t.approved_by == t.initiated_by))]
        expected = {"TXN-2026109001", "TXN-2026109002"} if m == "10" else set()
        assert set(bad.transaction_id) == expected, m


def test_duplicates_and_malformed_present(raw):
    for f in ["customer_account.csv", "employee_access.csv", "regulatory_reporting.csv", "compliance_issues.csv"]:
        assert csv(raw, f).duplicated().sum() >= 1, f
    t = jsonl(raw / "transactions" / "transactions_2026_10.json")
    assert t.duplicated().sum() == 2
    assert t.transaction_id.isna().sum() == 1 and (t.amount == "N/A").sum() == 1


def test_missing_and_late_reports(raw):
    r = csv(raw, "regulatory_reporting.csv")
    sep = r[r.reporting_period == "2026-09"].set_index("report_type")
    assert "LIQUIDITY_COVERAGE_RETURN" not in sep.index
    assert sep.loc["LARGE_CASH_TXN_REPORT", "submission_timestamp"] > "2026-10-15T23:59:59"
    assert sep.loc["CAPITAL_ADEQUACY_RETURN", "approval_status"] == "PENDING"


def test_late_batch(raw):
    p = sd.write_late_batch(str(raw))
    t = jsonl(p)
    assert (t.transaction_timestamp < "2026-11-01").sum() >= 5          # 3 late + stale + newer + dup
    assert (t.currency_code == "XYZ").sum() == 1


def test_policy_documents_have_metadata(raw):
    docs = sorted(os.listdir(raw / "policies"))
    assert len(docs) >= 5
    for d in docs:
        head = open(raw / "policies" / d).read().split("---")[1]
        for field in ["title:", "version:", "effective_date:", "owner:", "access_classification:"]:
            assert field in head, (d, field)
