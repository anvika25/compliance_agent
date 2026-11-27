---
doc_id: POL-PRV-004
title: Data Privacy and Retention Standard
version: "2.0"
effective_date: 2026-02-01
owner: Chief Privacy Officer
access_classification: INTERNAL
---
# Data Privacy and Retention Standard

## 1. Purpose
Protects personal information and sets retention periods for compliance records.

## 2. Personal information
Customer names, employee names, contact details and government identifiers are personal information (PII). PII must be tagged in the data catalogue and masked or pseudonymised in analytical outputs, dashboards and AI-generated summaries unless the user's role requires it.

## 3. Access
Access to PII follows least privilege. Column masks or row filters must be applied where broad groups can query tables containing PII.

## 4. Retention
Compliance evidence, control test results and exception records are kept for 7 years from creation. Records under legal hold may not be deleted. Disposal is logged.

## 5. AI use
PII may not be sent to an external model endpoint unless it is approved for that data classification.
