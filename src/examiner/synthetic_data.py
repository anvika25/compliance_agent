"""
Synthetic data generator for the Examiner prototype.

Plain Python + pandas only (no Spark), so it runs in a Databricks notebook
*and* in a local pytest. Everything is seeded, so every run produces the same
files -- the planted violations always have the same IDs.

Planted violations (what the controls must find for the period 2026-10-01..2026-10-31):

  Employee access
    * Bob Martin (E1099) terminated 2026-09-30, still has ACTIVE access:
        EA-0201 CORE_BANKING / ADMIN (privileged)   -> re-detected as EXC-10025
        EA-0202 PAYMENTS_HUB / PAYMENT_INITIATOR    -> new exception
    * EA-0206 privileged DBA access last reviewed 2026-03-15 (> 90 days)
    * Alice Chen (E1098) holds PAYMENT_INITIATOR + PAYMENT_APPROVER  -> SoD conflict
  Compliance issues
    * ISS-3025 / EXC-10025 (Bob's admin access) OPEN, target 2026-10-31 -> overdue remediation
  Transactions
    * TXN-2026109001  48,500 CAD wire, no approver
    * TXN-2026109002  72,000 CAD wire, self-approved by Alice (E1098)
    * TXN-2026109003..6  4 cash deposits of 9,400-9,850 in 5 days (structuring)
    * TXN-2026109007  wire to high-risk jurisdiction (IR)
    * TXN-2026109008  25,000 CAD withdrawal on DORMANT account ACC-900001
  Regulatory reporting (period 2026-09, due 2026-10-15)
    * LARGE_CASH_TXN_REPORT submitted late, CAPITAL_ADEQUACY_RETURN unapproved,
      LIQUIDITY_COVERAGE_RETURN missing
  Data quality (every dataset): exact duplicate rows + malformed rows
  Incremental: write_late_batch() adds a November file with late-arriving,
  stale and updated October records.
"""
from __future__ import annotations

import json
import os
import random
import shutil
from datetime import date, datetime, timedelta

import pandas as pd

AS_OF = date(2026, 11, 26)

FIRST = ["Priya", "Marc", "Sofia", "Daniel", "Mei", "Omar", "Julie", "Kevin", "Fatima", "Luc",
         "Hannah", "Raj", "Chloe", "Samuel", "Aisha", "Tom", "Elena", "Noah", "Grace", "Yusuf"]
LAST = ["Singh", "Tremblay", "Rossi", "Nguyen", "Wong", "Haddad", "Roy", "Smith", "Khan", "Gagnon",
        "Lee", "Patel", "Brown", "Ali", "Clark", "Dubois", "Kim", "Cohen", "Moreau", "Silva"]


# --------------------------------------------------------------------------- helpers
def rand_date(rng: random.Random, start: date, end: date) -> date:
    return start + timedelta(days=rng.randint(0, max((end - start).days, 0)))


def rand_ts(rng: random.Random, start: date, end: date) -> datetime:
    d = rand_date(rng, start, end)
    return datetime(d.year, d.month, d.day, rng.randint(7, 20), rng.randint(0, 59), rng.randint(0, 59))


def iso(d):
    return d.isoformat() if d else None


def messy_date(d, rng, p=0.1):
    """Mostly ISO dates, ~10% US format -> Silver must standardize."""
    if d is None:
        return None
    return d.strftime("%m/%d/%Y") if rng.random() < p else d.isoformat()


def variant(value, variants: dict, rng, p=0.25):
    """Return a messy spelling of a status/currency some of the time."""
    return rng.choice(variants[value]) if value in variants and rng.random() < p else value


def month_bounds(year: int, month: int):
    start = date(year, month, 1)
    end = (date(year + (month == 12), month % 12 + 1, 1)) - timedelta(days=1)
    return start, end


def write_csv(rows, path):
    pd.DataFrame(rows).to_csv(path, index=False)


def write_jsonl(rows, path):
    with open(path, "w") as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")


# --------------------------------------------------------------------------- customer / account
ACCOUNT_STATUS_VARIANTS = {"ACTIVE": ["Active", "active ", " ACTIVE"], "DORMANT": ["Dormant", "INACTIVE"],
                           "CLOSED": ["Closed", "closed"]}
PRODUCTS = ["CHEQUING", "SAVINGS", "BUSINESS_CHEQUING", "LINE_OF_CREDIT", "INVESTMENT"]


def build_customer_accounts(rng):
    rows = []
    countries = ["CA"] * 14 + ["US"] * 4 + ["GB", "FR", "MX"]
    n = 0
    for i in range(1, 151):
        cid = f"CUST-{i:05d}"
        name = f"{rng.choice(FIRST)} {rng.choice(LAST)}"
        risk = rng.choices(["LOW", "MEDIUM", "HIGH"], [70, 24, 6])[0]
        country = rng.choice(countries)
        for _ in range(rng.choice([1, 1, 1, 2])):
            n += 1
            opened = rand_date(rng, date(2012, 1, 1), date(2025, 12, 31))
            status = rng.choices(["ACTIVE", "DORMANT", "CLOSED"], [88, 8, 4])[0]
            closed = rand_date(rng, opened + timedelta(days=30), date(2026, 6, 30)) if status == "CLOSED" else None
            rows.append({
                "customer_id": cid, "account_id": f"ACC-{100000 + n}", "customer_name": name,
                "risk_rating": risk, "country_code": country,
                "account_status": variant(status, ACCOUNT_STATUS_VARIANTS, rng),
                "account_open_date": messy_date(opened, rng), "account_close_date": messy_date(closed, rng),
                "product_type": rng.choice(PRODUCTS),
            })
    # planted accounts
    rows += [
        {"customer_id": "CUST-09001", "account_id": "ACC-900001", "customer_name": "Maplecrest Trading Ltd",
         "risk_rating": "MEDIUM", "country_code": "CA", "account_status": "DORMANT",
         "account_open_date": "2015-04-01", "account_close_date": None, "product_type": "BUSINESS_CHEQUING"},
        {"customer_id": "CUST-09002", "account_id": "ACC-900002", "customer_name": "Jordan Pelletier",
         "risk_rating": "MEDIUM", "country_code": "CA", "account_status": "ACTIVE",
         "account_open_date": "2020-02-11", "account_close_date": None, "product_type": "CHEQUING"},
        {"customer_id": "CUST-09003", "account_id": "ACC-900003", "customer_name": "Harbourline Imports Inc",
         "risk_rating": "HIGH", "country_code": "CA", "account_status": "ACTIVE",
         "account_open_date": "2018-09-30", "account_close_date": None, "product_type": "BUSINESS_CHEQUING"},
    ]
    rows += [dict(rows[5]), dict(rows[40])]                       # exact duplicates
    rows += [                                                     # malformed
        {**rows[10], "account_id": None},                         # missing key
        {**rows[11], "account_id": "ACC-199998", "account_open_date": "2026-13-45"},  # impossible date
        {**rows[12], "account_id": "ACC-199999", "account_status": "FROZEN??"},       # invalid status
    ]
    return rows


# --------------------------------------------------------------------------- employee access
BUSINESS_UNITS = ["RETAIL_BANKING", "TREASURY", "TECHNOLOGY", "COMPLIANCE", "FINANCE"]
ROLE_POOL = [("CORE_BANKING", "TELLER"), ("CORE_BANKING", "READ_ONLY"), ("PAYMENTS_HUB", "PAYMENT_VIEWER"),
             ("GENERAL_LEDGER", "JOURNAL_PREPARER"), ("GENERAL_LEDGER", "GL_VIEWER"), ("CRM", "CRM_USER"),
             ("DATA_WAREHOUSE", "ANALYST")]
FLAG_VARIANTS = {"Y": ["Y", "Yes", "TRUE", "1"], "N": ["N", "No", "FALSE", "0"]}

PAYMENT_APPROVERS = ["E1001", "E1002"]
JOURNAL_APPROVER = "E1003"
PAYMENT_INITIATORS = [f"E{n}" for n in range(1004, 1013)]          # E1010 is terminated
PRIVILEGED_ADMINS = ["E1040", "E1041", "E1042", "E1043"]
TERMINATED_CLEAN = {"E1010": date(2026, 3, 31), "E1020": date(2025, 12, 15)}


def build_employee_access(rng):
    rows, seq = [], 10000

    def grant(emp, app, role, granted, review, privileged, status="ACTIVE", access_id=None):
        nonlocal seq
        seq += 1
        return {
            "access_id": access_id or f"EA-{seq}", "employee_id": emp["employee_id"],
            "employee_name": emp["employee_name"], "employee_role": emp["employee_role"],
            "business_unit": emp["business_unit"], "access_role": role, "application_name": app,
            "access_status": status, "granted_date": messy_date(granted, rng, 0.05),
            "access_review_date": messy_date(review, rng, 0.05), "termination_date": iso(emp.get("termination_date")),
            "privileged_access_flag": rng.choice(FLAG_VARIANTS["Y" if privileged else "N"]),
            "manager_id": emp["manager_id"],
        }

    for n in range(1001, 1061):
        eid = f"E{n}"
        if eid in PAYMENT_APPROVERS + PAYMENT_INITIATORS:
            bu = "PAYMENTS_OPERATIONS"
        elif eid == JOURNAL_APPROVER:
            bu = "FINANCE"
        elif eid in PRIVILEGED_ADMINS:
            bu = "TECHNOLOGY"
        else:
            bu = rng.choice(BUSINESS_UNITS)
        is_mgr = eid in PAYMENT_APPROVERS + [JOURNAL_APPROVER]
        emp = {"employee_id": eid, "employee_name": f"{rng.choice(FIRST)} {rng.choice(LAST)}",
               "employee_role": "Manager" if is_mgr else rng.choice(["Analyst", "Senior Analyst", "Specialist"]),
               "business_unit": bu, "manager_id": None if is_mgr else rng.choice(PAYMENT_APPROVERS),
               "termination_date": TERMINATED_CLEAN.get(eid)}
        hire = rand_date(rng, date(2015, 1, 1), date(2024, 6, 30))
        status = "REVOKED" if eid in TERMINATED_CLEAN else "ACTIVE"

        if eid in PAYMENT_APPROVERS:
            roles = [("CORE_BANKING", "READ_ONLY", False), ("PAYMENTS_HUB", "PAYMENT_APPROVER", False)]
        elif eid == JOURNAL_APPROVER:
            roles = [("GENERAL_LEDGER", "JOURNAL_APPROVER", False), ("GENERAL_LEDGER", "GL_VIEWER", False)]
        elif eid in PRIVILEGED_ADMINS:
            roles = [(rng.choice([("CORE_BANKING", "ADMIN"), ("DATA_WAREHOUSE", "DBA")]) + (True,)),
                     ("CRM", "CRM_USER", False)]
        else:
            roles = [r + (False,) for r in rng.sample(ROLE_POOL, rng.randint(1, 3))]
            if eid in PAYMENT_INITIATORS:
                roles.append(("PAYMENTS_HUB", "PAYMENT_INITIATOR", False))

        for app, role, priv in roles:
            granted = rand_date(rng, hire, hire + timedelta(days=90))
            review = (rand_date(rng, date(2026, 8, 20), date(2026, 10, 28)) if priv
                      else rand_date(rng, date(2026, 1, 15), date(2026, 10, 28)))
            rows.append(grant(emp, app, role, granted, review, priv, status))

    bob = {"employee_id": "E1099", "employee_name": "Bob Martin", "employee_role": "Senior Payments Analyst",
           "business_unit": "PAYMENTS_OPERATIONS", "manager_id": "E1001", "termination_date": date(2026, 9, 30)}
    alice = {"employee_id": "E1098", "employee_name": "Alice Chen", "employee_role": "Senior Payments Analyst",
             "business_unit": "PAYMENTS_OPERATIONS", "manager_id": "E1001", "termination_date": None}
    dba = {"employee_id": "E1061", "employee_name": "Kevin Moreau", "employee_role": "Database Administrator",
           "business_unit": "TECHNOLOGY", "manager_id": "E1002", "termination_date": None}
    rows += [
        # Bob: terminated 2026-09-30, two grants still ACTIVE, one correctly revoked
        grant(bob, "CORE_BANKING", "ADMIN", date(2019, 3, 4), date(2026, 8, 28), True, access_id="EA-0201"),
        grant(bob, "PAYMENTS_HUB", "PAYMENT_INITIATOR", date(2019, 4, 1), date(2026, 5, 12), False, access_id="EA-0202"),
        grant(bob, "CRM", "CRM_USER", date(2019, 3, 4), date(2026, 5, 12), False, "REVOKED", access_id="EA-0203"),
        # Alice: SoD conflict -- initiator + approver in PAYMENTS_HUB
        grant(alice, "PAYMENTS_HUB", "PAYMENT_INITIATOR", date(2021, 5, 17), date(2026, 9, 2), False, access_id="EA-0204"),
        grant(alice, "PAYMENTS_HUB", "PAYMENT_APPROVER", date(2026, 6, 15), date(2026, 9, 2), False, access_id="EA-0205"),
        # Privileged access not reviewed for > 90 days
        grant(dba, "DATA_WAREHOUSE", "DBA", date(2022, 1, 10), date(2026, 3, 15), True, access_id="EA-0206"),
    ]
    rows[-6]["privileged_access_flag"] = "Y"      # keep planted flags unambiguous
    rows[-1]["privileged_access_flag"] = "Y"
    rows += [dict(rows[3]), dict(rows[20])]                                   # duplicates
    rows += [
        {**rows[30], "access_id": "EA-19997", "employee_id": None},          # missing employee
        {**rows[31], "access_id": "EA-19998", "granted_date": "31/02/2025"},  # bad date
        {**rows[32], "access_id": "EA-19999", "privileged_access_flag": "MAYBE"},
    ]
    return rows


# --------------------------------------------------------------------------- transactions
TXN_TYPES = [("DEPOSIT", ["BRANCH", "ATM", "MOBILE"]), ("WITHDRAWAL", ["BRANCH", "ATM"]),
             ("EFT", ["ONLINE", "MOBILE"]), ("BILL_PAYMENT", ["ONLINE", "MOBILE"]),
             ("WIRE_OUT", ["SWIFT"]), ("WIRE_IN", ["SWIFT"])]
CCY_VARIANTS = {"CAD": ["cad", "C$", "CAD "], "USD": ["usd", "US$"]}
TXN_STATUS_VARIANTS = {"COMPLETED": ["Completed", "completed", "COMPLETE"]}


def _active_accounts(accounts):
    return [a for a in accounts[:-8]                     # exclude planted + dq rows
            if a["account_status"].strip().upper() == "ACTIVE"]


def _txn(rng, txn_id, ts, acct, window, initiators, **over):
    ttype, channels = rng.choices(TXN_TYPES, [30, 15, 25, 20, 5, 5])[0]
    r = rng.random()
    amount = rng.uniform(10, 3000) if r < .85 else rng.uniform(3000, 6000) if r < .96 else rng.uniform(10000, 150000)
    ccy = "USD" if rng.random() < .08 else "CAD"
    needs_approval = amount >= 6000
    init = rng.choice(initiators) if needs_approval or ttype.startswith("WIRE") else "SYSTEM"
    status = rng.choices(["COMPLETED", "PENDING", "REVERSED"], [95, 3, 2])[0]
    t = {
        "transaction_id": txn_id, "customer_id": acct["customer_id"], "account_id": acct["account_id"],
        "transaction_timestamp": ts.isoformat(), "transaction_type": ttype, "amount": f"{amount:.2f}",
        "currency_code": variant(ccy, CCY_VARIANTS, rng),
        "country_code": rng.choice(["US", "GB", "FR", "MX"]) if ttype.startswith("WIRE") and rng.random() < .4 else acct["country_code"],
        "channel": rng.choice(channels), "status": variant(status, TXN_STATUS_VARIANTS, rng),
        "source_system": "PAYMENTS_HUB" if ttype in ("WIRE_OUT", "WIRE_IN", "EFT") else "CORE_BANKING",
        "initiated_by": init,
        "approved_by": rng.choice(PAYMENT_APPROVERS) if needs_approval else None,
        "approval_reference": f"APR-{rng.randint(100000, 999999)}" if needs_approval else None,
        "last_modified_ts": (ts + timedelta(minutes=rng.randint(1, 90))).isoformat(),
        "extract_window_start": iso(window[0]), "extract_window_end": iso(window[1]),
    }
    t.update(over)
    return t


def build_month_transactions(accounts, year, month, n=350):
    rng = random.Random(year * 100 + month)
    window = month_bounds(year, month)
    active = _active_accounts(accounts)
    initiators = [e for e in PAYMENT_INITIATORS if e not in TERMINATED_CLEAN]
    if (year, month) < (2026, 10):
        initiators = initiators + ["E1099"]            # Bob worked until 2026-09-30
    rows = [_txn(rng, f"TXN-{year}{month:02d}{i:04d}", rand_ts(rng, *window), rng.choice(active), window, initiators)
            for i in range(1, n + 1)]

    if (year, month) == (2026, 10):
        by_id = {a["account_id"]: a for a in accounts}
        base = active[7]

        def planted(txn_id, ts, acct, **kw):
            return _txn(random.Random(txn_id), txn_id, ts, acct, window, initiators,
                        status="COMPLETED", currency_code="CAD", **kw)
        rows += [
            planted("TXN-2026109001", datetime(2026, 10, 14, 15, 42, 10), base, transaction_type="WIRE_OUT",
                    channel="SWIFT", amount="48500.00", initiated_by="E1005", approved_by=None,
                    approval_reference=None, country_code="US"),
            planted("TXN-2026109002", datetime(2026, 10, 22, 11, 5, 0), active[9], transaction_type="WIRE_OUT",
                    channel="SWIFT", amount="72000.00", initiated_by="E1098", approved_by="E1098",
                    approval_reference="APR-771204", country_code="CA"),
        ]
        for k, (day, amt) in enumerate([(6, "9400.00"), (7, "9650.00"), (9, "9850.00"), (10, "9500.00")]):
            rows.append(planted(f"TXN-202610900{3 + k}", datetime(2026, 10, day, 10 + k, 15, 0), by_id["ACC-900002"],
                                transaction_type="DEPOSIT", channel="BRANCH", amount=amt, initiated_by="SYSTEM",
                                approved_by=None, approval_reference=None, country_code="CA"))
        rows += [
            planted("TXN-2026109007", datetime(2026, 10, 17, 9, 30, 0), by_id["ACC-900003"], transaction_type="WIRE_OUT",
                    channel="SWIFT", amount="15000.00", initiated_by="E1006", approved_by="E1001",
                    approval_reference="APR-771388", country_code="IR"),
            planted("TXN-2026109008", datetime(2026, 10, 20, 13, 0, 0), by_id["ACC-900001"], transaction_type="WITHDRAWAL",
                    channel="BRANCH", amount="25000.00", initiated_by="E1007", approved_by="E1002",
                    approval_reference="APR-771420", country_code="CA"),
        ]
        rows += [dict(rows[15]), dict(rows[16])]                                   # duplicates
        rows += [
            {**rows[20], "transaction_id": "TXN-2026109901", "amount": "N/A"},
            {**rows[21], "transaction_id": None},
            {**rows[22], "transaction_id": "TXN-2026109903", "transaction_timestamp": "not-a-timestamp"},
        ]
    return rows


def write_late_batch(raw_dir, accounts=None):
    """November extract. Run AFTER the first Bronze load to demo incremental Auto Loader.

    Contains: new November transactions, three late-arriving October transactions,
    a STALE older version of an October txn (must not overwrite), a NEWER version
    (status REVERSED, must overwrite), a cross-file exact duplicate and a malformed row.
    """
    if accounts is None:
        accounts = build_customer_accounts(random.Random(42))
    rng = random.Random(202611)
    window = (date(2026, 11, 1), date(2026, 11, 25))
    active = _active_accounts(accounts)
    initiators = [e for e in PAYMENT_INITIATORS if e not in TERMINATED_CLEAN]
    rows = [_txn(rng, f"TXN-202611{i:04d}", rand_ts(rng, *window), rng.choice(active), window, initiators)
            for i in range(1, 251)]
    october = build_month_transactions(accounts, 2026, 10)
    # late-arriving: October activity first seen in the November extract
    rows += [_txn(rng, f"TXN-2026108{i:03d}", rand_ts(rng, date(2026, 10, 25), date(2026, 10, 31)),
                  rng.choice(active), window, initiators) for i in range(1, 4)]
    stale, newer, dup = dict(october[30]), dict(october[31]), dict(october[32])
    stale.update(status="PENDING", last_modified_ts=(datetime.fromisoformat(stale["last_modified_ts"]) - timedelta(days=2)).isoformat())
    newer.update(status="REVERSED", last_modified_ts=datetime(2026, 11, 3, 9, 0, 0).isoformat())
    rows += [stale, newer, dup, {**rows[5], "transaction_id": "TXN-2026119901", "currency_code": "XYZ"}]
    path = os.path.join(raw_dir, "transactions", "transactions_2026_11_01_to_25.json")
    write_jsonl(rows, path)
    return path


# --------------------------------------------------------------------------- regulatory reporting
REPORT_TYPES = {"LARGE_CASH_TXN_REPORT": "FINANCIAL_INTELLIGENCE_UNIT",
                "SUSPICIOUS_ACTIVITY_SUMMARY": "FINANCIAL_INTELLIGENCE_UNIT",
                "CAPITAL_ADEQUACY_RETURN": "PRUDENTIAL_REGULATOR",
                "LIQUIDITY_COVERAGE_RETURN": "PRUDENTIAL_REGULATOR"}
PERIODS = ["2026-06", "2026-07", "2026-08", "2026-09", "2026-10"]


def build_report_calendar():
    rows = []
    for p in PERIODS:
        y, m = map(int, p.split("-"))
        due = date(y + (m == 12), m % 12 + 1, 15)
        rows += [{"report_type": t, "reporting_period": p, "due_date": iso(due), "regulator": reg}
                 for t, reg in REPORT_TYPES.items()]
    return rows


def build_regulatory_reports(rng, calendar):
    rows, n = [], 0
    for c in calendar:
        if (c["report_type"], c["reporting_period"]) == ("LIQUIDITY_COVERAGE_RETURN", "2026-09"):
            continue                                                         # MISSING report
        n += 1
        due = date.fromisoformat(c["due_date"])
        amount = round(rng.uniform(2e5, 9e7), 2)
        sub = datetime.combine(due - timedelta(days=rng.randint(1, 8)), datetime.min.time()) + timedelta(hours=rng.randint(9, 17))
        r = {"report_id": f"REG-{5000 + n}", "report_type": c["report_type"], "reporting_period": c["reporting_period"],
             "reported_amount": f"{amount:.2f}", "source_system_amount": f"{amount * rng.uniform(0.9995, 1.0005):.2f}",
             "submission_status": "SUBMITTED", "submission_timestamp": sub.isoformat(),
             "approval_status": "APPROVED", "approver_id": "E1003", "prepared_by": rng.choice(["E1021", "E1025", "E1033"])}
        key = (c["report_type"], c["reporting_period"])
        if key == ("LARGE_CASH_TXN_REPORT", "2026-09"):
            r["submission_timestamp"] = "2026-10-19T16:20:00"                # LATE (due 10-15)
        if key == ("CAPITAL_ADEQUACY_RETURN", "2026-09"):
            r["approval_status"], r["approver_id"] = "PENDING", None          # UNAPPROVED
        if key == ("SUSPICIOUS_ACTIVITY_SUMMARY", "2026-08"):
            r["reported_amount"], r["source_system_amount"] = "1250000.00", "1312400.00"  # UNRECONCILED (4.8%)
        if key[1] == "2026-10":                                              # not yet due-period work in progress
            r["submission_status"] = "SUBMITTED"
        rows.append(r)
    rows.append(dict(rows[2]))                                               # duplicate
    rows += [{**rows[3], "report_id": "REG-5998", "reported_amount": "twelve"},
             {**rows[4], "report_id": None}]
    return rows


# --------------------------------------------------------------------------- control catalogue
CONTROLS = [
    ("CTL-TXN-001", "Transaction-threshold approval", "AML_TRANSACTION_MONITORING",
     "Transactions at or above the threshold must have an independent approver and an approval reference.",
     "Head of Payments Operations", "High", "threshold_exception", 10000, 0, "DAILY", "silver_transaction",
     "POL-AML-001 s3.2", "Obtain retrospective approval, document rationale, review initiator's other transactions."),
    ("CTL-TXN-002", "Unusual transaction pattern", "AML_TRANSACTION_MONITORING",
     "Detect structuring (>=3 cash deposits between 90-100% of the threshold within 7 days) and high-risk geography.",
     "Chief AML Officer", "High", "pattern_detection", 10000, 0, "DAILY", "silver_transaction",
     "POL-AML-001 s3.3-3.4", "Refer to AML investigations; assess for suspicious activity reporting."),
    ("CTL-TXN-003", "Dormant-account activity", "AML_TRANSACTION_MONITORING",
     "Significant transactions on dormant or closed accounts require reactivation review.",
     "Head of Retail Operations", "Medium", "dormant_activity", 5000, 0, "DAILY", "silver_transaction",
     "POL-AML-001 s3.5", "Confirm customer identity, document reactivation, review source of funds."),
    ("CTL-ACC-001", "Employee-access recertification", "ACCESS_CONTROL",
     "Active access must belong to current employees and be reviewed (privileged: every N days, standard: annually).",
     "Head of Identity and Access Management", "Critical", "access_recertification", 90, 0, "MONTHLY",
     "silver_employee_access", "POL-ACC-002 s4.1, s4.3", "Revoke access, confirm no post-termination activity, recertify."),
    ("CTL-ACC-002", "Segregation of duties", "ACCESS_CONTROL",
     "No employee may hold conflicting role combinations defined in the SoD rule set.",
     "Head of Identity and Access Management", "Critical", "segregation_of_duties", 0, 0, "MONTHLY",
     "silver_employee_access", "POL-ACC-002 s5.1-5.3", "Remove one conflicting role or document a time-boxed compensating control."),
    ("CTL-REG-001", "Regulatory-reporting completeness", "REGULATORY_REPORTING",
     "Every scheduled report is submitted on time, approved, and reconciles to source within the tolerance.",
     "Head of Regulatory Reporting", "High", "reporting_completeness", 0.01, 0, "MONTHLY",
     "silver_regulatory_reporting", "POL-REG-003 s3-5", "File or correct the report, notify regulator if late, document root cause."),
    ("CTL-PRV-001", "Data retention review", "DATA_PRIVACY",
     "Records past their retention period are disposed of (designed, not yet automated).",
     "Chief Privacy Officer", "Medium", "retention_review", 2555, 0, "QUARTERLY", "n/a",
     "POL-PRV-004 s4", "Dispose of or place on legal hold."),
]
CONTROL_COLS = ["control_id", "control_name", "control_domain", "control_description", "control_owner", "severity",
                "test_type", "threshold", "tolerance_pct", "frequency", "population", "policy_reference",
                "remediation_action"]


def build_control_catalogue():
    rows = [dict(zip(CONTROL_COLS, c), active_flag="N" if c[0] == "CTL-PRV-001" else "Y") for c in CONTROLS]
    rows.append(dict(rows[0]))                                               # duplicate
    return rows


# --------------------------------------------------------------------------- compliance issues
ISSUE_STATUS_VARIANTS = {"OPEN": ["Open", "open"], "IN_PROGRESS": ["In Progress", "IN-PROGRESS"], "CLOSED": ["Closed"]}


def build_compliance_issues(rng):
    rows = []
    controls = [c[0] for c in CONTROLS[:6]]
    for i in range(1, 25):
        ctl = rng.choice(controls)
        opened = rand_date(rng, date(2025, 3, 1), date(2026, 6, 30))
        target = opened + timedelta(days=rng.choice([30, 60, 90]))
        status = "IN_PROGRESS" if i == 19 else "CLOSED"
        if i == 19:
            target = date(2026, 12, 15)
        rows.append({
            "issue_id": f"ISS-{3000 + i}", "exception_id": f"EXC-{10000 + i}", "control_id": ctl,
            "entity_id": f"HIST-{ctl[-7:]}-{i:03d}", "issue_title": f"Historical finding {i} for {ctl}",
            "issue_status": variant(status, ISSUE_STATUS_VARIANTS, rng), "issue_owner": rng.choice(
                ["Head of Payments Operations", "Head of Identity and Access Management", "Head of Regulatory Reporting"]),
            "business_unit": rng.choice(["PAYMENTS_OPERATIONS", "TECHNOLOGY", "FINANCE", "RETAIL_BANKING"]),
            "severity": rng.choice(["High", "Medium"]), "opened_date": messy_date(opened, rng),
            "target_remediation_date": iso(target),
            "actual_remediation_date": iso(target - timedelta(days=rng.randint(1, 20))) if status == "CLOSED" else None,
            "validation_status": "VALIDATED" if status == "CLOSED" else "NOT_STARTED",
            "remediation_action": "Remediated and validated by second line.",
        })
    rows.append({  # THE overdue item: raised by the September run, still open past target
        "issue_id": "ISS-3025", "exception_id": "EXC-10025", "control_id": "CTL-ACC-001", "entity_id": "EA-0201",
        "issue_title": "Terminated employee retains privileged CORE_BANKING admin access",
        "issue_status": "Open", "issue_owner": "Head of Identity and Access Management",
        "business_unit": "PAYMENTS_OPERATIONS", "severity": "Critical", "opened_date": "2026-10-05",
        "target_remediation_date": "2026-10-31", "actual_remediation_date": None,
        "validation_status": "NOT_STARTED",
        "remediation_action": "Revoke EA-0201, review CORE_BANKING admin logs after 2026-09-30, recertify Payments access.",
    })
    rows.append(dict(rows[4]))                                               # duplicate
    rows.append({**rows[5], "issue_id": "ISS-3999", "opened_date": "yesterday-ish"})  # malformed
    return rows


# --------------------------------------------------------------------------- reference data
SOD_RULES = [
    {"rule_id": "SOD-001", "application_name": "PAYMENTS_HUB", "role_a": "PAYMENT_INITIATOR",
     "role_b": "PAYMENT_APPROVER", "description": "Payment initiation and approval"},
    {"rule_id": "SOD-002", "application_name": "GENERAL_LEDGER", "role_a": "JOURNAL_PREPARER",
     "role_b": "JOURNAL_APPROVER", "description": "Journal preparation and approval"},
]
HIGH_RISK_COUNTRIES = [{"country_code": c, "reason": "Synthetic high-risk list"} for c in ["IR", "KP", "MM", "SY"]]
FX_RATES = [{"currency_code": c, "rate_to_cad": r} for c, r in [("CAD", 1.0), ("USD", 1.38), ("EUR", 1.50), ("GBP", 1.75)]]


# --------------------------------------------------------------------------- entry point
def build_all(raw_dir: str, policies_src: str | None = None) -> dict:
    """Write every raw file. Returns {file: row_count}."""
    for sub in ["transactions", "reference", "policies"]:
        os.makedirs(os.path.join(raw_dir, sub), exist_ok=True)
    out = {}

    accounts = build_customer_accounts(random.Random(42))
    access = build_employee_access(random.Random(43))
    calendar = build_report_calendar()
    files = {
        "customer_account.csv": accounts,
        "employee_access.csv": access,
        "regulatory_reporting.csv": build_regulatory_reports(random.Random(44), calendar),
        "control_catalogue.csv": build_control_catalogue(),
        "compliance_issues.csv": build_compliance_issues(random.Random(45)),
        "reference/regulatory_report_calendar.csv": calendar,
        "reference/sod_rules.csv": SOD_RULES,
        "reference/high_risk_countries.csv": HIGH_RISK_COUNTRIES,
        "reference/fx_rates.csv": FX_RATES,
    }
    for name, rows in files.items():
        write_csv(rows, os.path.join(raw_dir, name))
        out[name] = len(rows)

    for y, m in [(2026, 8), (2026, 9), (2026, 10)]:
        rows = build_month_transactions(accounts, y, m)
        name = f"transactions/transactions_{y}_{m:02d}.json"
        write_jsonl(rows, os.path.join(raw_dir, name))
        out[name] = len(rows)

    if policies_src and os.path.isdir(policies_src):
        for f in sorted(os.listdir(policies_src)):
            if f.endswith(".md"):
                shutil.copy(os.path.join(policies_src, f), os.path.join(raw_dir, "policies", f))
                out[f"policies/{f}"] = 1
    return out
