# Test Automation Design

This document captures testing-related prompts addressed for the RTGS solution and summarises the corresponding outputs, artefacts, and follow-up actions.

## Session 001 – Test Scope Identification
**Prompt/Input**  
1) You are a software tester, who has to identify key features to be tested based on the document and list out all testing scenarios.  
2) Generate synthetic data for a couple of such scenarios.

**Response Summary**
- Highlighted critical RTGS features spanning lifecycle integrity, entitlement controls, integrations, charges, and exception handling.
- Produced an enumerated list of end-to-end scenarios covering generation, validation, processing, limits, charge computation, integration, observability, security, performance, and recovery.
- Delivered two JSON synthetic datasets representing a high-value MT-103 and an incoming MT-199 correction case.

**Key Artefacts**
- Testing Focus (bullet list of lifecycle areas).
- Scenario Catalogue (`MSG-GEN-01` through `REC-01`).
- Synthetic data examples for high-value MT-103 authorization and MT-199 correction handling.

```json
{
  "Scenario": "Branch MT-103 exceeding limit requires HO authorization",
  "MessageType": "MT103",
  "PreparationUser": "BR001_USER1",
  "Branch": "Lahore Mall Road",
  "BeneficiaryBankBIC": "SCBLPKKX",
  "AmountPKR": 25000000,
  "BranchLimitPKR": 20000000,
  "ChargeConfig": {
    "BaseFee": 300,
    "FEDPercent": 16
  },
  "ExpectedOutcome": [
    "Status=Pending HO Authorization",
    "Charge=348 (300 + 16%) with timestamp",
    "HOUser prompts for dual approval"
  ]
}
```

```json
{
  "Scenario": "Incoming MT-199 correction routed to originating branch",
  "MessageType": "MT199",
  "OriginalMessageRef": "FINCA/001/2023",
  "Issue": "Incorrect Beneficiary Account",
  "ReceivingBank": "HMB7330681",
  "OriginatingBranch": "Karachi Clifton",
  "CorrectionFields": {
    "Field59": "PK15MEZN0049010103450183",
    "BeneficiaryName": "NAEEM TARIQ"
  },
  "ExpectedOutcome": [
    "Branch queue receives MT-199",
    "Operator required to amend original MT-103",
    "Audit log links correction to original message"
  ]
}
```

## Session 002 – MSG-GEN-01 Test Case Matrix
**Prompt/Input**  
Can you create the test cases to test all possible MSG-GEN-01 scenarios including boundary conditions, edge cases, happy paths, negative scenarios, regression, volume/load scenarios, network failure, concurrency, infra, privacy, security and vulnerability?

**Response Summary**
- Produced a comprehensive test matrix focused on MSG-GEN-01 (branch-limited MT creation) covering positive, negative, boundary, regression, and high-volume situations.
- Documented reusable reference data for branch configurations.
- Highlighted evidence expectations (audit logs, UI confirmation, system responses).

**Test Case Matrix**

| TC ID | Type | Preconditions | Steps | Expected Results |
| - | - | - | - | - |
| MSG-GEN-01-HP01 | Happy path | Branch user `BR001_USER1` logged in; branch context Lahore Mall Road; MT-102 template active | 1. Navigate to RTGS module → New MT. 2. Select MT-102. 3. Verify Branch dropdown shows only Lahore Mall Road (pre-selected). 4. Enter valid mandatory fields and amount within limit. 5. Save. | Message saved with status `Prepared`; audit shows branch `Lahore Mall Road`; record visible in branch pending queue; no authorization flags. |
| MSG-GEN-01-HP02 | Happy path | Branch user `BR002_USER2` at Rawalpindi Satellite; MT-103 allowed | 1. Launch MT creation. 2. Select MT-103. 3. Validate branch auto-populates as Rawalpindi Satellite. 4. Complete fields, submit. | MT-103 stored, branch tag correct; only MT-103 template accessible; confirmation toast displayed. |
| MSG-GEN-01-BND01 | Boundary – branch list | Branch user `BR001_USER1` logged in | 1. Open Branch selector (if editable). | Dropdown displays single entry (Lahore Mall Road). Attempt to select other branch results in “No options” or disabled control. |
| MSG-GEN-01-BND02 | Boundary – message type list | Same as above | 1. Open Message Type list. | Only MT-102 and MT-103 presented; MT-202/199 hidden/disabled. Selecting other type triggers validation error. |
| MSG-GEN-01-BND03 | Boundary – cut-off start | Cut-off time 09:00 for branch; user logs in 08:59 | 1. Create MT-102 at 08:59 and save before clock hits 09:00. 2. System time crosses 09:00. 3. Attempt new MT. | First message accepted. Subsequent attempt after cut-off prompts “Branch window closed; contact HO” – no draft created. |
| MSG-GEN-01-NEG01 | Negative – cross-branch selection via URL tampering | User intercepts API call altering branch ID to `BR999` | 1. Capture request payload. 2. Modify `branchId` to BR999. 3. Resend. | Server responds HTTP 403/validation error “Branch not authorized”; audit logs suspicious attempt; no record created. |
| MSG-GEN-01-NEG02 | Negative – login from different branch workstation | User physically at Karachi branch but logs via central credentials; attempts to pick Karachi branch | 1. Log in; 2. Attempt to create MT; 3. Choose Karachi branch in UI (if accessible). | System enforces home branch; change blocked with message “Cross-branch creation restricted”; event logged. |
| MSG-GEN-01-NEG03 | Negative – unauthorized message type | Branch user tries to access MT-202 via deep link/bookmark | 1. Paste MT-202 creation URL. | System redirects to dashboard with alert “Unauthorized message type”; no form loaded. |
| MSG-GEN-01-NEG04 | Negative – post cut-off override | Branch user after cut-off tries to save draft | 1. Initiate new MT after 16:30 (branch cut-off). 2. Click Save Draft. | Save blocked with message referencing cut-off; timestamp recorded; no draft persists. |
| MSG-GEN-01-REG01 | Regression – HO user unaffected | HO user `HO_TREASURY1` logged in | 1. Create MT-103 for Karachi Clifton branch. | Message successfully created with branch selectable; ensures branch restriction isolated to branch roles. |
| MSG-GEN-01-REG02 | Regression – existing queued items intact after patch | Branch user with existing drafts before control deployed | 1. Upgrade system enabling new control. 2. Login branch user. 3. Open older draft. | Draft remains editable; upon save ensures branch unchanged; no data loss. |
| MSG-GEN-01-VOL01 | Volume – sustained creation | Load-test account `BRLOAD_USER` with script | 1. Simulate 200 MT-102 creations within 10 min during allowed window. | All messages accepted; no throttling errors; each retains correct branch; response time within SLA (<2s). |
| MSG-GEN-01-EDGE01 | Edge – branch reassignment mid-session | Admin reassigns user to new branch while session active | 1. User logs in (branch A). 2. Admin switches user’s branch to branch B. 3. User attempts new MT without re-login. | Creation blocked with prompt to re-login; prevents stale branch use. |
| MSG-GEN-01-EDGE02 | Edge – offline mode / network glitch | User fills MT, network drops before submission and tries resubmit with cached branch ID | 1. Disconnect network after filling form. 2. Reconnect and resubmit. | Server revalidates branch; stale data rejected; otherwise normal flow resumes. |
| MSG-GEN-01-EDGE03 | Edge – bulk upload attempt | Branch user tries to upload file containing multiple branches | 1. Use bulk-import tool (if available) with entries for BR001 & BR002. | System rejects entries not matching user branch; per-record validation errors surfaced. |
| MSG-GEN-01-ERR01 | Negative – network failure mid-submit | Branch user `BR001_USER1` preparing MT-102; network simulator available | 1. Fill mandatory fields. 2. Disable network while submitting. 3. Restore network. 4. Resubmit. | Client shows “Connection lost”/retry prompt; backend has no partial record; audit logs failure with `NetworkFailure`; retry succeeds post-validation. |

**Reference Data**

| Branch ID | Branch Name | Allowed MT Types | Cut-off | Daily Limit | Notes |
| - | - | - | - | - | - |
| BR001 | Lahore Mall Road | MT-102, MT-103 | 09:00–16:30 | PKR 25M | Used for primary validation |
| BR002 | Rawalpindi Satellite | MT-102, MT-103 | 09:00–16:30 | PKR 30M | Alternate happy path |
| HO001 | Head Office Treasury | MT-102/103/202 | N/A | Unlimited | Regression control |

## Session 003 – Network Failure Augmentation
**Prompt/Input**  
add test case for network failure

**Response Summary**
- Added explicit negative test `MSG-GEN-01-ERR01` targeting connectivity loss during submission.
- Clarified expectations around client messaging, server-side resilience, and audit logging.

**Test Case Added**
- `MSG-GEN-01-ERR01`: Validates graceful handling of network interruption mid-submit, ensuring no duplicate or orphan records and confirming successful retry after connectivity restoration.
