# Model card: Examiner exception risk score

| | |
|---|---|
| Name | `examiner_compliance.ml.exception_risk_model` (UC), served by alias `@champion` |
| Version string | `rules-v1.0+lr-v<n>` on every score |
| Code | `src/examiner/risk_scoring.py`, `notebooks/04_risk_scoring.py`, tests in `tests/test_risk_scoring.py` |
| Owner | Compliance Analytics (first line), validated by Model Risk Management (POL-AIG-006 s3) |
| Status | **Prototype.** Rules implemented; model trained on **simulated** labels |

## Purpose
Rank open compliance exceptions so reviewers look at the riskiest first. It is a **prioritisation aid only**: it does
not decide whether an exception is valid, does not close or change issues, and `human_review_required` is always true
(POL-HRV-007 s2, s4).

## Features (each scaled 0–1)
| Factor | Definition | Rule weight |
|---|---|---|
| Severity | Critical 1.0 / High 0.75 / Medium 0.5 / Low 0.25 | 0.20 |
| Financial or regulatory impact | log10(CAD+1)/6 (CAD 1M → 1.0); floor 0.7 regulatory reporting, 0.5 access | 0.15 |
| Control criticality | by domain: access 1.0, regulatory 0.9, AML 0.85 | 0.10 |
| Repeat occurrence | linked to a prior issue, or detected in > 1 period | 0.10 |
| Exception age | days since the event / 90, capped at 1 | 0.05 |
| Business-unit exposure | open exceptions in the BU / max across BUs | 0.05 |
| Evidence gap | 1 − evidence_completeness | 0.05 |
| Overdue remediation | linked issue open past its target date | 0.20 |
| Detection confidence | from the control SQL (e.g. 0.99 access, 0.70 structuring) | 0.10 |

## Approach
1. **Rule score** = Σ weight × factor × 100. Fully transparent; the three biggest contributions become `top_risk_factors`.
2. **Model**: logistic regression on the same 9 factors plus a `repeat × overdue` interaction. It predicts the
   probability that a reviewer would escalate the exception.
3. **Blend**: `risk_score = 0.6 × rule + 0.4 × 100 × model_prob`. Bands: ≥75 Critical/P1, ≥55 High/P2, ≥35 Medium/P3, else Low/P4.

**Training / calibration.** No real reviewer history exists, so 3,000 reviewer decisions are **simulated**
(`simulate_reviewer_history`) with a hidden logic that differs from the rule weights (interaction terms, noise for
reviewer disagreement). In production: retrain on real reviewer outcomes (confirmed vs false positive, escalated or not)
captured by the human-review workflow, and recalibrate the weights with Compliance.

## Evaluation (held-out 25%, logged to MLflow)
ROC AUC for model, rules and hybrid; average precision; Brier score (calibration); precision in the top 10%.
Latest local run: model AUC 0.75, rules 0.74, hybrid 0.75; precision in the top 10% ≈ 0.95. **Promotion gate:** a new version becomes
`@champion` only if hybrid AUC ≥ rules AUC − 0.01. Rollback = move the alias back to the previous version.

## Explainability
Every score carries `rule_score`, `model_probability` and `top_risk_factors` (named factor + points). Model coefficients
are logged as an MLflow artifact.

## Limitations
* Labels are synthetic, so the model's metrics show the pipeline works, not real-world accuracy.
* Very small live population (~12 exceptions); band thresholds are judgement-based.
* Domain-based criticality and impact floors are assumptions to be agreed with control owners.
* Account status is current, not point-in-time (affects dormant-account detections feeding the score).

## False-positive considerations
Pattern rules (structuring, high-risk geography) have lower detection confidence, so they score lower and false
positives aren't pushed to the top. Reviewer "false positive" decisions should become training labels and a
per-control false-positive rate on the platform dashboard. A low score never suppresses an exception: everything stays
in the queue.

## Human review
All outputs are recommendations. A qualified reviewer confirms or rejects each exception and records name, decision and
timestamp before any action (POL-HRV-007 s3). The model cannot change `status`.
