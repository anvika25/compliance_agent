---
doc_id: POL-ACC-002
title: Employee Access and Segregation of Duties Policy
version: "2.4"
effective_date: 2026-03-01
owner: Head of Identity and Access Management
access_classification: CONFIDENTIAL
---
# Employee Access and Segregation of Duties Policy

## 1. Purpose
Ensures employees hold only the access their current role requires and that no individual can complete a sensitive process end to end.

## 2. Scope
All production applications, including CORE_BANKING, PAYMENTS_HUB, GENERAL_LEDGER, CRM and DATA_WAREHOUSE.

## 3. Access provisioning
Access is granted only through an approved request with manager approval. Privileged access (administrator, DBA) additionally requires approval by the application owner.

## 4. Access review and removal
### 4.1 Review frequency
Privileged access must be recertified at least every 90 days. Standard access must be recertified at least annually (every 365 days). Access with no recorded review is treated as unreviewed.
### 4.2 Reviewer
Reviews are performed by the employee's manager and, for privileged access, the application owner. Reviewers may not certify their own access.
### 4.3 Termination
All access must be revoked within 1 business day of an employee's termination date. Access still active after that is a Critical exception. The control owner must also confirm whether the access was used after termination.

## 5. Segregation of duties
### 5.1 Conflicting roles
No employee may hold both PAYMENT_INITIATOR and PAYMENT_APPROVER in PAYMENTS_HUB, or both JOURNAL_PREPARER and JOURNAL_APPROVER in GENERAL_LEDGER.
### 5.2 Detection
SoD conflicts are tested monthly against the approved SoD rule set.
### 5.3 Compensating controls
A temporary conflict may be allowed only with a documented compensating control approved by Compliance, valid for no more than 90 days. Expired compensating controls are treated as active conflicts.
