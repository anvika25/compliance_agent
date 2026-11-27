"""
Exception risk scoring: a rule-plus-model hybrid.

  rule_score  (0-100)  transparent weighted sum of 9 normalised factors   -> primary, explainable
  model_prob  (0-1)    logistic regression trained on SIMULATED reviewer  -> secondary, learns interactions
                       decisions (no real labelled history exists yet)
  risk_score = RULE_BLEND * rule_score + (1 - RULE_BLEND) * 100 * model_prob

The score only PRIORITISES. human_review_required is always True, and nothing here
changes an exception's status.

Pure Python / numpy / pandas / scikit-learn, so it is unit-testable outside Databricks.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

RULE_VERSION = "rules-v1.0"
RULE_BLEND = 0.6

# factor -> (weight, human label). Weights sum to 1.0. Reviewed by Compliance; changing them is a new RULE_VERSION.
FACTORS = {
    "f_severity":             (0.20, "Control severity"),
    "f_impact":               (0.15, "Financial or regulatory impact"),
    "f_criticality":          (0.10, "Control criticality"),
    "f_repeat":               (0.10, "Repeat occurrence"),
    "f_age":                  (0.05, "Exception age"),
    "f_bu_exposure":          (0.05, "Business-unit exposure"),
    "f_evidence_gap":         (0.05, "Evidence incomplete"),
    "f_overdue":              (0.20, "Overdue remediation"),
    "f_detection_confidence": (0.10, "Detection confidence"),
}
FEATURES = list(FACTORS)
assert abs(sum(w for w, _ in FACTORS.values()) - 1.0) < 1e-9

BANDS = [  # (min score, band, priority)
    (75, "CRITICAL", "P1 - review within 1 business day"),
    (55, "HIGH", "P2 - review within 5 business days"),
    (35, "MEDIUM", "P3 - review within 15 business days"),
    (0, "LOW", "P4 - review in next monthly cycle"),
]


# --------------------------------------------------------------------------- rules
def rule_contributions(df: pd.DataFrame) -> pd.DataFrame:
    """Points (0-100 scale) each factor contributes."""
    return pd.DataFrame({f: df[f].clip(0, 1).fillna(0) * w * 100 for f, (w, _) in FACTORS.items()}, index=df.index)


def rule_score(df: pd.DataFrame) -> pd.Series:
    return rule_contributions(df).sum(axis=1).round(2)


def top_factors(df: pd.DataFrame, n: int = 3) -> list[list[str]]:
    contrib = rule_contributions(df)
    out = []
    for _, row in contrib.iterrows():
        best = row.sort_values(ascending=False).head(n)
        out.append([f"{FACTORS[f][1]} (+{v:.1f})" for f, v in best.items() if v > 0])
    return out


def band(score: float) -> tuple[str, str]:
    for lo, b, p in BANDS:
        if score >= lo:
            return b, p
    return BANDS[-1][1], BANDS[-1][2]


# --------------------------------------------------------------------------- simulated training data
def simulate_reviewer_history(n: int = 3000, seed: int = 7) -> pd.DataFrame:
    """SIMULATED: historical exceptions with a reviewer decision `escalated` (1 = reviewer treated as high priority).

    The hidden labelling logic is deliberately NOT identical to the rule weights: reviewers escalate
    repeat+overdue combinations and Critical access findings more than the linear rule predicts, and the
    labels carry noise (reviewer disagreement). This gives the model something the rules don't capture.
    """
    rng = np.random.default_rng(seed)
    sev = rng.choice([0.25, 0.5, 0.75, 1.0], n, p=[0.1, 0.35, 0.35, 0.2])
    X = pd.DataFrame({
        "f_severity": sev,
        "f_impact": rng.beta(1.5, 4, n),
        "f_criticality": np.clip(sev + rng.normal(0, 0.1, n), 0, 1),
        "f_repeat": rng.binomial(1, 0.25, n).astype(float),
        "f_age": rng.beta(2, 5, n),
        "f_bu_exposure": rng.beta(2, 3, n),
        "f_evidence_gap": rng.choice([0.0, 0.14, 0.29], n, p=[0.85, 0.1, 0.05]),
        "f_overdue": rng.binomial(1, 0.15, n).astype(float),
        "f_detection_confidence": rng.uniform(0.6, 1.0, n),
    })
    logit = (-4.2 + 2.6 * X.f_severity + 2.0 * X.f_impact + 1.2 * X.f_repeat + 1.8 * X.f_overdue
             + 1.5 * X.f_repeat * X.f_overdue + 1.2 * X.f_detection_confidence + 0.6 * X.f_age
             + 0.8 * (X.f_severity == 1.0) * X.f_criticality + rng.normal(0, 0.6, n))
    X["escalated"] = (rng.uniform(size=n) < 1 / (1 + np.exp(-logit))).astype(int)
    return X


def add_interactions(X: pd.DataFrame) -> pd.DataFrame:
    X = X[FEATURES].copy()
    X["x_repeat_overdue"] = X.f_repeat * X.f_overdue
    return X


# --------------------------------------------------------------------------- training / evaluation
def train_model(history: pd.DataFrame, seed: int = 7, C: float = 1.0):
    from sklearn.linear_model import LogisticRegression
    from sklearn.metrics import average_precision_score, brier_score_loss, roc_auc_score
    from sklearn.model_selection import train_test_split

    X, y = add_interactions(history), history["escalated"]
    X_tr, X_te, y_tr, y_te = train_test_split(X, y, test_size=0.25, random_state=seed, stratify=y)
    model = LogisticRegression(C=C, max_iter=1000).fit(X_tr, y_tr)
    p = model.predict_proba(X_te)[:, 1]
    rules = rule_score(X_te) / 100

    k = max(1, int(0.1 * len(y_te)))
    def precision_at_k(scores):
        return float(y_te.iloc[np.argsort(-np.asarray(scores))[:k]].mean())

    hybrid = RULE_BLEND * rules + (1 - RULE_BLEND) * p
    metrics = {
        "model_roc_auc": roc_auc_score(y_te, p), "rules_roc_auc": roc_auc_score(y_te, rules),
        "hybrid_roc_auc": roc_auc_score(y_te, hybrid),
        "model_avg_precision": average_precision_score(y_te, p),
        "model_brier": brier_score_loss(y_te, p),
        "model_precision_at_10pct": precision_at_k(p), "rules_precision_at_10pct": precision_at_k(rules),
        "hybrid_precision_at_10pct": precision_at_k(hybrid),
        "base_rate": float(y.mean()), "n_train": len(y_tr), "n_test": len(y_te),
    }
    coefficients = pd.DataFrame({"feature": X.columns, "coefficient": model.coef_[0]}).sort_values(
        "coefficient", ascending=False)
    return model, {k: round(float(v), 4) for k, v in metrics.items()}, coefficients, (X_te, y_te)


# --------------------------------------------------------------------------- scoring
def score(features: pd.DataFrame, model, model_version: str) -> pd.DataFrame:
    """features: one row per exception with exception_id + FEATURES. Returns the scoring output table."""
    rs = rule_score(features)
    mp = model.predict_proba(add_interactions(features.fillna(0)))[:, 1] if model is not None else np.zeros(len(features))
    final = (RULE_BLEND * rs + (1 - RULE_BLEND) * 100 * mp).round(2)
    bands = [band(s) for s in final]
    return pd.DataFrame({
        "exception_id": features["exception_id"].values,
        "risk_score": final.values,
        "rule_score": rs.values,
        "model_probability": np.round(mp, 4),
        "risk_band": [b for b, _ in bands],
        "top_risk_factors": top_factors(features),
        "recommended_priority": [p for _, p in bands],
        "model_or_rule_version": f"{RULE_VERSION}+{model_version}",
        "human_review_required": True,   # always: the score prioritises, a person decides
    })
