"""
Examiner: a controlled, multi-step, read-only compliance agent.

Design: a deterministic Python orchestrator. The PLAN (which tools, in which order) is fixed code, so every run
is reproducible and auditable. The LLM is used only to draft the "Recommended Compliance Follow-Up" prose from
facts the tools returned. That draft is checked for prohibited language and falls back to rule-based text.

Safety controls implemented here:
  * request guard: refuses close/update/delete/approve actions, raw SQL, external actions, final determinations
  * tool allow-list + per-argument validation (IDs, dates, domains), so arbitrary SQL can't be passed through a tool
  * scope validation (known domain, period not in the future, period resolves to dates)
  * output labelling: [FACT] / [MODEL OUTPUT] / [RECOMMENDATION]; explicit evidence gaps
  * PII masking of names in every response and log line
  * full trace: every tool call and LLM prompt with timestamps (the notebook persists it)
  * Human Review Required is always "Yes"; the agent never changes any status

Tools are injected as callables, so this module is unit-testable with fakes (tests/test_agent.py).
"""
from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone

AGENT_VERSION = "examiner-agent-v1.0"

DOMAINS = {
    "ACCESS_CONTROL": ["access-control", "access control", "employee access", "segregation of duties", "sod"],
    "AML_TRANSACTION_MONITORING": ["aml", "transaction monitoring", "transactions"],
    "REGULATORY_REPORTING": ["regulatory reporting", "regulatory-reporting", "reporting"],
}
MONTHS = {m: i for i, m in enumerate(["january", "february", "march", "april", "may", "june", "july", "august",
                                      "september", "october", "november", "december"], 1)}

# policy questions asked per control test type (answers come from RAG, never from the LLM's memory)
POLICY_QUESTIONS = {
    "access_recertification": ["How quickly must access be removed after an employee is terminated?",
                               "What is the review frequency for privileged access?"],
    "segregation_of_duties": ["Which roles may an employee not hold together under segregation of duties?"],
    "threshold_exception": ["What controls apply to transaction approval and release?"],
    "pattern_detection": ["What are the requirements for structuring and high-risk jurisdictions?"],
    "dormant_activity": ["What is required for transactions on dormant accounts?"],
    "reporting_completeness": ["What are the timeliness, approval and reconciliation requirements for regulatory reports?"],
}
ALWAYS_ASK = ["What evidence is required before a compliance exception can be closed?"]
ASK_IF_OVERDUE_CRITICAL = ["What is the escalation process for a critical exception?"]

SECTIONS = ["Compliance Review Scope", "Controls Evaluated", "Key Compliance Findings", "Highest-Risk Exceptions",
            "Evidence Summary", "Applicable Policy Requirements", "Remediation Status", "Data or Evidence Gaps",
            "Recommended Compliance Follow-Up", "Human Review Required", "Sources and Citations"]


# --------------------------------------------------------------------------- guard
PROHIBITED = [
    (r"\b(close|resolve|approve|reject|delete|remove|update|modify|change|mark|set|reopen|waive|dismiss)\b"
     r"[^.?!]{0,40}\b(issue|exception|finding|status|EXC-\d+|ISS-\d+|record|table)", "WRITE_ACTION",
     "Examiner is read-only. It cannot close, change or approve issues or exceptions; that needs a human reviewer."),
    (r"\b(select|insert|update|delete|drop|merge|alter|grant|revoke|truncate|create)\b[\s\S]{0,200}"
     r"\b(from|into|table|set|schema|catalog|on)\b", "RAW_SQL",
     "Examiner does not run arbitrary SQL. Ask in plain language and it will use its approved read-only tools."),
    (r"```|;\s*--|\bexecute\b|\brun (this|the following) (sql|query|code)", "RAW_SQL",
     "Examiner does not run arbitrary SQL or code."),
    (r"\b(email|notify|contact|send|file|submit|report)\b[^.?!]{0,30}\b(regulator|customer|fintrac|osfi|authority|employee)\b",
     "EXTERNAL_ACTION", "Examiner cannot take external action (contacting regulators, customers or staff)."),
    (r"\b(are|is) (we|the bank|the institution|it)\b[^.?!]{0,20}\b(compliant|in breach|in violation|non-compliant)\b|"
     r"\bfinal (regulatory )?determination\b", "DETERMINATION",
     "Examiner does not make final regulatory determinations. It can summarise findings for a human reviewer."),
]


def guard_request(text: str) -> list[dict]:
    hits = []
    for pattern, code, message in PROHIBITED:
        if re.search(pattern, text, flags=re.IGNORECASE):
            if code == "WRITE_ACTION" and re.search(r"\b(before|can|could|when|how|what|which)\b[^.?!]{0,60}\b(closed|be closed)\b",
                                                    text, flags=re.IGNORECASE):
                continue                         # "what evidence is required before ... can be closed?" is a question
            hits.append({"code": code, "message": message})
    return hits


# --------------------------------------------------------------------------- scope
@dataclass
class Scope:
    review_scope: str
    period_start: date
    period_end: date
    exception_ids: list = field(default_factory=list)
    business_unit: str | None = None
    wants_overdue: bool = True
    notes: list = field(default_factory=list)
    errors: list = field(default_factory=list)

    @property
    def period_label(self):
        return f"{self.period_start.isoformat()}..{self.period_end.isoformat()}"


def _month_bounds(y, m):
    start = date(y, m, 1)
    return start, date(y + (m == 12), m % 12 + 1, 1) - timedelta(days=1)


def parse_scope(text: str, today: date) -> Scope:
    t = text.lower()
    domain = next((d for d, kws in DOMAINS.items() if any(re.search(rf"\b{re.escape(k)}\b", t) for k in kws)), None)
    notes, errors = [], []
    if domain is None:
        domain = "ALL_DOMAINS"
        notes.append("No control domain named; reviewing all domains.")

    m_iso = re.search(r"\b(20\d{2})-(\d{2})\b", t)
    m_name = re.search(r"\b(" + "|".join(MONTHS) + r")\s+(20\d{2})\b", t)
    if "previous month" in t or "last month" in t or "prior month" in t:
        ps, pe = _month_bounds(*(lambda d: (d.year, d.month))(today.replace(day=1) - timedelta(days=1)))
    elif m_name:
        ps, pe = _month_bounds(int(m_name.group(2)), MONTHS[m_name.group(1)])
    elif m_iso and 1 <= int(m_iso.group(2)) <= 12:
        ps, pe = _month_bounds(int(m_iso.group(1)), int(m_iso.group(2)))
    else:
        ps, pe = _month_bounds(*(lambda d: (d.year, d.month))(today.replace(day=1) - timedelta(days=1)))
        notes.append("No period named; defaulting to the previous calendar month.")
    if pe > today:
        errors.append(f"Requested period {ps}..{pe} has not finished yet; results would be incomplete.")

    # business unit only when named explicitly ("business unit Treasury" or an ID like PAYMENTS_OPERATIONS),
    # so ordinary words like "compliance" don't narrow the scope
    bu = re.search(r"\b(RETAIL_BANKING|PAYMENTS_OPERATIONS|TREASURY|TECHNOLOGY|COMPLIANCE|FINANCE)\b", text) or \
         re.search(r"business unit\s+(retail banking|payments operations|treasury|technology|compliance|finance)\b", t)
    return Scope(review_scope=domain, period_start=ps, period_end=pe,
                 exception_ids=sorted(set(re.findall(r"\bEXC-\d{5}\b", text.upper()))),
                 business_unit=bu.group(1).upper().replace(" ", "_") if bu else None,
                 wants_overdue=True, notes=notes, errors=errors)


# --------------------------------------------------------------------------- tool registry
ARG_RULES = {
    "control_id": r"^CTL-[A-Z]{3}-\d{3}$",
    "exception_id": r"^EXC-\d{5}$",
    "issue_id": r"^ISS-\d{4}$",
    "period_start": r"^\d{4}-\d{2}-\d{2}$",
    "period_end": r"^\d{4}-\d{2}-\d{2}$",
    "review_scope": r"^(ACCESS_CONTROL|AML_TRANSACTION_MONITORING|REGULATORY_REPORTING|ALL_DOMAINS)$",
    "period_label": r"^\d{4}-\d{2}-\d{2}\.\.\d{4}-\d{2}-\d{2}$",
    "business_unit": r"^[A-Z_]{2,40}$",
    "question": r"^[^;`]{5,400}$",
}
TOOL_SPECS = {
    "run_control_test": ["control_id", "period_start", "period_end"],
    "exception_lookup": ["exception_id"],
    "evidence_retrieval": ["exception_id"],
    "policy_search": ["question"],
    "remediation_status": ["control_id", "issue_id", "exception_id"],          # any one of these
    "compliance_scope_summary": ["review_scope", "period_label", "business_unit"],
}


class ToolError(Exception):
    pass


class ToolRegistry:
    def __init__(self, impls: dict, masker=None):
        unknown = set(impls) - set(TOOL_SPECS)
        if unknown:
            raise ValueError(f"Tools not on the allow-list: {unknown}")
        self.impls, self.masker = impls, masker or (lambda s: s)
        self.trace: list[dict] = []

    def call(self, name: str, **kwargs):
        if name not in TOOL_SPECS or name not in self.impls:
            raise ToolError(f"Tool '{name}' is not approved")
        for k, v in kwargs.items():
            if k not in TOOL_SPECS[name]:
                raise ToolError(f"{name}: unexpected argument '{k}'")
            if v is not None and not re.match(ARG_RULES[k], str(v)):
                raise ToolError(f"{name}: invalid value for {k}")
        started, t0 = datetime.now(timezone.utc), time.time()
        status, out, err = "OK", None, None
        try:
            out = self.impls[name](**kwargs)
        except Exception as e:                                              # tool failure becomes an evidence gap
            status, err = "ERROR", f"{type(e).__name__}: {e}"[:300]
        self.trace.append({"step": len(self.trace) + 1, "tool": name, "args": kwargs, "started_at": started.isoformat(),
                           "latency_ms": int((time.time() - t0) * 1000), "status": status, "error": err,
                           "output_summary": self.masker(json.dumps(out, default=str)[:1500]) if out is not None else None})
        if status == "ERROR":
            raise ToolError(err)
        return out


# --------------------------------------------------------------------------- masking
def make_masker(name_to_id: dict):
    """Replace known personal names with pseudonymous IDs. Longest names first, case-insensitive."""
    names = sorted((n for n in name_to_id if n and len(n) > 3), key=len, reverse=True)
    pattern = re.compile(r"\b(?:" + "|".join(re.escape(n) for n in names) + r")\b", re.IGNORECASE) if names else None
    lower = {n.lower(): i for n, i in name_to_id.items() if n}

    def mask(text):
        if text is None or pattern is None:
            return text
        return pattern.sub(lambda m: f"[{lower.get(m.group(0).lower(), 'REDACTED')}]", str(text))
    return mask


# --------------------------------------------------------------------------- recommendation drafting
RECO_SYSTEM = """You draft follow-up recommendations for a compliance reviewer.
Use ONLY the FACTS provided. Every bullet must reference an exception, issue or policy ID that appears in the FACTS.
Write 3-6 short bullets starting with "- ". Use cautious language ("recommend", "consider", "confirm").
Never state that anything is closed, resolved, compliant or non-compliant, and never say a regulator should be notified
unless a cited policy section says so. Never invent dates, people or requirements."""
BANNED_OUTPUT = re.compile(r"\b(is|are|has been|have been|was) (now )?(closed|resolved|compliant|remediated)\b|"
                           r"\bno further action\b|\bfinal determination\b", re.IGNORECASE)


def rule_based_recommendations(facts: dict) -> list[str]:
    recs = []
    for o in facts["overdue"]:
        recs.append(f"Escalate overdue item {o.get('issue_id') or o['exception_id']} ({o['days_overdue']} days past "
                    f"target) per POL-REM-005 §6; agree a revised target with {o['owner']}.")
    for e in facts["requested_exceptions"]:
        if "TERMINATED" in (e.get("exception_description") or "").upper() or "terminated" in (e.get("exception_description") or ""):
            recs.append(f"Confirm revocation of {e['entity_id']} and review its activity after the termination date "
                        f"(POL-ACC-002 §4.3) before {e['exception_id']} is considered for closure.")
    for e in facts["high_risk"]:
        if e["control_id"] == "CTL-ACC-002":
            recs.append(f"Remove one conflicting role for {e['entity_id']} or document a compensating control of at most 90 days "
                        f"(POL-ACC-002 §5.3) for {e['exception_id']}.")
    if facts["exceptions_without_issue"]:
        recs.append(f"Raise remediation issues for the {facts['exceptions_without_issue']} open exception(s) that have none.")
    recs.append("Gather closure evidence (root cause, remediation evidence, second-line validation, sign-off) per POL-REM-005 §4.")
    return list(dict.fromkeys(recs))[:6]


def draft_recommendations(facts: dict, llm=None) -> tuple[list[str], str, dict | None]:
    base = rule_based_recommendations(facts)
    if llm is None:
        return base, "rule-based", None
    messages = [{"role": "system", "content": RECO_SYSTEM},
                {"role": "user", "content": "FACTS:\n" + json.dumps(facts, default=str, indent=1)[:6000] +
                 "\n\nRULE-BASED DRAFT (improve wording only, keep every ID):\n" + "\n".join(f"- {r}" for r in base)}]
    try:
        text = llm(messages)
    except Exception as e:
        return base, f"rule-based (LLM error: {type(e).__name__})", {"messages": messages, "response": None}
    bullets = [b.strip()[2:].strip() for b in text.splitlines() if b.strip().startswith(("- ", "* "))]
    known_ids = set(re.findall(r"\b(?:EXC-\d{5}|ISS-\d{4}|POL-[A-Z]{3}-\d{3}|EA-\d{4,5}|E\d{4}|CTL-[A-Z]{3}-\d{3})\b",
                               json.dumps(facts, default=str)))
    ok = bullets and not BANNED_OUTPUT.search(text) and all(
        set(re.findall(r"\b(?:EXC|ISS|POL|EA|CTL)-[\w-]+|\bE\d{4}\b", b)) <= known_ids and re.search(r"\b(EXC|ISS|POL|EA|CTL)-|\bE\d{4}\b", b)
        for b in bullets)
    trace = {"messages": messages, "response": text, "accepted": bool(ok)}
    return (bullets[:6], "llm-drafted, validated", trace) if ok else (base, "rule-based (LLM draft failed checks)", trace)


# --------------------------------------------------------------------------- orchestration
def _fmt_list(items, empty="None."):
    return "\n".join(f"- {i}" for i in items) if items else f"- {empty}"


def run_examiner(request: str, tools: ToolRegistry, today: date, llm=None, masker=None) -> dict:
    masker = masker or (lambda s: s)
    t0 = time.time()
    blocked = guard_request(request)
    if blocked:
        msg = "Request declined.\n" + "\n".join(f"- {b['message']}" for b in blocked) + \
              "\nHuman Review Required: Yes. Route this request to a compliance reviewer."
        return {"status": "REFUSED", "guard": blocked, "response": msg, "trace": tools.trace, "llm_trace": None,
                "scope": None, "latency_ms": int((time.time() - t0) * 1000), "agent_version": AGENT_VERSION}

    scope = parse_scope(request, today)
    if scope.errors:
        return {"status": "INVALID_SCOPE", "guard": [], "response": "Scope could not be validated:\n" + _fmt_list(scope.errors),
                "trace": tools.trace, "llm_trace": None, "scope": scope.__dict__, "latency_ms": int((time.time() - t0) * 1000),
                "agent_version": AGENT_VERSION}

    gaps, citations, sources = list(scope.notes), [], set()

    def safe(name, **kw):
        try:
            return tools.call(name, **kw)
        except ToolError as e:
            gaps.append(f"Tool {name}({', '.join(f'{k}={v}' for k, v in kw.items() if v)}) failed or returned nothing: {e}")
            return None

    # 1. scope summary (facts from gold_examiner_summary)
    summary = safe("compliance_scope_summary", review_scope=scope.review_scope, period_label=scope.period_label,
                   business_unit=scope.business_unit) or {}
    if not summary:
        gaps.append(f"No control results exist for {scope.review_scope} in {scope.period_label}; run the controls first.")
    sources.add("gold.gold_examiner_summary")

    # 2. control results, reperformed read-only and compared with the stored result
    control_lines, controls = [], summary.get("controls", [])
    for c in controls:
        r = safe("run_control_test", control_id=c["control_id"], period_start=scope.period_start.isoformat(),
                 period_end=scope.period_end.isoformat())
        if r:
            match = r.get("reperformance_matches")
            control_lines.append(
                f"[FACT] {r['control_id']} {r['control_name']}: {r['test_status']}; population {r['population_count']}, "
                f"exceptions {r['exception_count']} (rate {r['exception_rate']:.1%}), threshold {r['threshold']}"
                + ("; reperformed now, matches the stored result" if match else
                   "; reperformed now, DIFFERS from the stored result" if match is False else ""))
            if match is False:
                gaps.append(f"{r['control_id']}: reperformance differs from the stored test result; data may have changed since the run.")
    sources.add("gold.gold_control_test_results")

    # 3. highest-risk exceptions (model output)
    high_risk = []
    for h in (summary.get("high_risk_exceptions") or [])[:5]:
        e = safe("exception_lookup", exception_id=h["exception_id"])
        if e:
            high_risk.append(e)
    hr_lines = [f"[MODEL OUTPUT] {e['exception_id']} ({e['control_id']}, entity {e['entity_id']}): risk score "
                f"{e['risk_score']} {e['risk_band']}; top factors: {', '.join(e.get('top_risk_factors') or []) or 'n/a'}. "
                f"[FACT] {e['exception_description']}; owner {e['owner']}; status {e['status']}" for e in high_risk]
    sources.add("gold.gold_compliance_exceptions / ml.exception_risk_scores")

    # 4. requested exceptions: details + evidence
    requested, ev_lines = [], []
    for xid in scope.exception_ids:
        e = safe("exception_lookup", exception_id=xid)
        ev = safe("evidence_retrieval", exception_id=xid)
        if not e:
            gaps.append(f"{xid} was not found in the exception register.")
            continue
        requested.append(e)
        if not ev:
            gaps.append(f"No evidence record found for {xid}.")
            continue
        ev_lines += [
            f"[FACT] {xid}: {ev['evidence_summary']}",
            f"[FACT] Control logic: {ev['control_logic']}",
            f"[FACT] Expected: {ev['expected_condition']} | Actual: {ev['actual_condition']} | Variance: {ev['variance']}",
            f"[FACT] Source record {ev['source_record_id']} in {ev['source_table']}; event time {ev['event_timestamp']}; "
            f"related: {ev['related_records']}",
            f"[FACT] Lineage: {ev['lineage_reference']}; Bronze record "
            + ("verified present" if ev.get("lineage_verified") else "NOT found (lineage gap)"),
            f"[FACT] Evidence completeness {ev['evidence_completeness']:.0%}; review status {ev['review_status']}; "
            f"detected in {len(e.get('detected_periods') or [])} period(s)"
            + (f"; repeat of issue {e['linked_issue_id']}" if e.get("linked_issue_id") else ""),
        ]
        if ev.get("evidence_gaps"):
            gaps.append(f"{xid} evidence is missing: {', '.join(ev['evidence_gaps'])}.")
        if not ev.get("lineage_verified"):
            gaps.append(f"{xid}: the Bronze source record could not be re-located from the evidence hash.")
        sources.add(f"gold.gold_compliance_evidence {ev['evidence_id']}")

    # 5. remediation
    overdue, rem_lines, without_issue = [], [], 0
    seen_ctrl = sorted({c["control_id"] for c in controls} | {e["control_id"] for e in requested})
    for cid in seen_ctrl:
        items = safe("remediation_status", control_id=cid) or []
        for it in items:
            if it["remediation_status"] == "CLOSED":
                continue
            if it["record_type"] == "EXCEPTION_WITHOUT_ISSUE":
                without_issue += 1
            if it["is_overdue"]:
                overdue.append(it)
            rem_lines.append(
                f"[FACT] {it.get('issue_id') or 'no issue'} / {it['exception_id']} ({cid}): {it['remediation_status']}, "
                f"owner {it['owner']}, target {it['target_remediation_date']}"
                + (f", OVERDUE by {it['days_overdue']} days" if it["is_overdue"] else "")
                + f", validation {it['validation_status']}"
                + (" (target implied by POL-REM-005 SLA)" if it["record_type"] == "EXCEPTION_WITHOUT_ISSUE" else ""))
    sources.add("gold.gold_remediation_status")

    # 6. policy requirements via RAG (citations come from retrieved sections only)
    test_types = sorted({c.get("test_type") for c in controls if c.get("test_type")} |
                        {e.get("test_type") for e in requested if e.get("test_type")})
    questions = [q for tt in test_types for q in POLICY_QUESTIONS.get(tt, [])] + ALWAYS_ASK
    if any(o.get("severity") == "CRITICAL" for o in overdue):
        questions += ASK_IF_OVERDUE_CRITICAL
    policy_lines = []
    for q in dict.fromkeys(questions):
        p = safe("policy_search", question=q)
        if not p:
            continue
        if p["status"] == "ANSWERED":
            policy_lines.append(f"[FACT, policy text] Q: {q}\n  {p['answer']}")
            citations += [f"{c['doc_id']} v{c['version']} §{c['section']} — {c['title']}" for c in p["citations"]]
        else:
            gaps.append(f"No approved policy text found for: {q}")

    # 7. findings & recommendations
    findings = [f"[FACT] {f}" for f in (summary.get("key_findings") or [])]
    if overdue:
        findings.append(f"[FACT] {len(overdue)} remediation item(s) overdue in scope.")
    gaps += summary.get("evidence_gaps") or []
    facts = {"scope": scope.period_label, "review_scope": scope.review_scope,
             "high_risk": [{k: e.get(k) for k in ("exception_id", "control_id", "entity_id", "risk_score", "risk_band",
                                                  "exception_description")} for e in high_risk],
             "requested_exceptions": [{k: e.get(k) for k in ("exception_id", "control_id", "entity_id",
                                                             "exception_description", "linked_issue_id")} for e in requested],
             "overdue": [{k: o.get(k) for k in ("issue_id", "exception_id", "owner", "days_overdue", "severity")} for o in overdue],
             "exceptions_without_issue": without_issue, "policy_citations": sorted(set(citations))}
    recs, reco_mode, llm_trace = draft_recommendations(facts, llm)

    body = {
        "Compliance Review Scope": [f"[FACT] Domain {scope.review_scope}; period {scope.period_label}"
                                    + (f"; business unit {scope.business_unit}" if scope.business_unit else "")
                                    + (f"; exceptions requested: {', '.join(scope.exception_ids)}" if scope.exception_ids else "")],
        "Controls Evaluated": control_lines,
        "Key Compliance Findings": findings,
        "Highest-Risk Exceptions": hr_lines,
        "Evidence Summary": ev_lines,
        "Applicable Policy Requirements": policy_lines,
        "Remediation Status": rem_lines,
        "Data or Evidence Gaps": list(dict.fromkeys(gaps)),
        "Recommended Compliance Follow-Up": [f"[RECOMMENDATION] {r}" for r in recs] + [f"(drafting: {reco_mode})"],
        "Human Review Required": ["Yes. These are decision-support findings. A qualified compliance reviewer must confirm "
                                  "or reject each item and record their decision before any action (POL-HRV-007 §2-§4). "
                                  "Examiner has not changed any status."],
        "Sources and Citations": [f"Policy: {c}" for c in sorted(set(citations))] + [f"Data: {s}" for s in sorted(sources)],
    }
    text = "\n\n".join(f"{name}:\n{_fmt_list(body[name])}" for name in SECTIONS)
    return {"status": "COMPLETED", "guard": [], "scope": {**scope.__dict__, "period_label": scope.period_label},
            "response": masker(text), "sections": {k: [masker(x) for x in v] for k, v in body.items()},
            "trace": tools.trace, "llm_trace": llm_trace, "recommendation_mode": reco_mode,
            "latency_ms": int((time.time() - t0) * 1000), "agent_version": AGENT_VERSION}
