import os
import sys

import pandas as pd

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(ROOT, "src"))
from examiner import risk_scoring as rs  # noqa: E402


def row(**kw):
    base = {f: 0.0 for f in rs.FEATURES}
    base.update(kw)
    return base


def test_weights_sum_to_one():
    assert abs(sum(w for w, _ in rs.FACTORS.values()) - 1) < 1e-9


def test_rule_score_bounds():
    df = pd.DataFrame([row(), row(**{f: 1.0 for f in rs.FEATURES})])
    s = rs.rule_score(df)
    assert s.iloc[0] == 0 and s.iloc[1] == 100


def test_out_of_range_inputs_are_clipped():
    assert rs.rule_score(pd.DataFrame([row(f_impact=7.0, f_severity=-1)])).iloc[0] == 15.0


def test_bands():
    assert rs.band(80)[0] == "CRITICAL" and rs.band(60)[0] == "HIGH" and rs.band(40)[0] == "MEDIUM" and rs.band(1)[0] == "LOW"


def test_top_factors_explain_score():
    f = rs.top_factors(pd.DataFrame([row(f_overdue=1, f_severity=1, f_repeat=1, f_age=0.2)]))[0]
    assert f[0].startswith("Control severity") or f[0].startswith("Overdue remediation")
    assert len(f) == 3 and "Repeat occurrence (+10.0)" in f


def test_critical_repeat_overdue_outranks_low_one_off():
    model, metrics, _, _ = rs.train_model(rs.simulate_reviewer_history(1500))
    feats = pd.DataFrame([
        {"exception_id": "EXC-A", **row(f_severity=1, f_criticality=1, f_repeat=1, f_overdue=1, f_impact=0.6,
                                         f_detection_confidence=0.99, f_age=0.6)},
        {"exception_id": "EXC-B", **row(f_severity=0.5, f_impact=0.2, f_detection_confidence=0.7)},
    ])
    out = rs.score(feats, model, "lr-test").set_index("exception_id")
    assert out.loc["EXC-A", "risk_score"] > out.loc["EXC-B", "risk_score"]
    assert out.loc["EXC-A", "risk_band"] in ("CRITICAL", "HIGH")
    assert out.human_review_required.all()


def test_model_beats_chance_and_hybrid_not_worse_than_rules():
    _, m, _, _ = rs.train_model(rs.simulate_reviewer_history())
    assert m["model_roc_auc"] > 0.70
    assert m["hybrid_roc_auc"] >= m["rules_roc_auc"] - 0.01


def test_score_without_model_falls_back_to_rules():
    feats = pd.DataFrame([{"exception_id": "EXC-X", **row(f_severity=1, f_overdue=1)}])
    out = rs.score(feats, None, "none")
    assert out.risk_score.iloc[0] == round(rs.RULE_BLEND * 40, 2)
