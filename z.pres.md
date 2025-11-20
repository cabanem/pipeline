# Contractor Performance Tracking – Project Brief (v0.1)

**Project name**
Contractor Performance History & Risk Tracking (Phase 1)

**Owner**


**Process owner / SME**


**Date**


---
## Executive Summary

This initiative standardizes how we track contractor performance. Phase 1 replaces the current per-tab, free-text spreadsheet with a single structured contractor register and a centralized events log (1:1s, coaching, escalations, kudos, PIP milestones), implemented in Google Sheets. We add simple, transparent indicators for who is on a performance plan and who appears at risk, based on the process owner’s 20+ years of experience. Later phases may build an application and automation on top of the same schema once we’ve validated that the structure and rules work in practice.

## 1. Purpose & Context

This project aims to move the business unit from ad-hoc, spreadsheet-based contractor performance tracking to a structured, consistent model that supports better decisions about coaching, performance-improvement plans (PIPs), and contract actions.

Today, contractor performance information is stored in a single “master” spreadsheet plus one tab per contractor. Data is largely unstructured, depends heavily on individual habits, and relies on a long-tenured process owner’s tacit knowledge. The first phase of this project will focus on defining and validating a schema for contractor performance and implementing it in a standardized, low-friction way (Google Sheets). Later phases may implement a more robust application and automation against the same schema.

---

## 2. Business Problem

* Contractor performance for contingent staff is currently tracked in unstructured spreadsheets using free-text notes, screenshots, and per-contractor tabs.
* There is no single source of truth, no standard way to document performance events or improvement plans, and limited ability to see patterns over time (recurring issues, impact of coaching, renewal decisions, etc.).
* Performance-improvement plans and process improvement efforts are largely handled locally within business units and are not consistently visible or tracked beyond the spreadsheets.
* The process owner has deep (20+ years) expertise, but much of that knowledge is implicit (e.g., how to interpret notes, what patterns are worrying, when to escalate), which makes it hard to onboard new leads and hard to evidence decisions.

This increases operational risk, creates inconsistency across teams, and makes it time-consuming to reconstruct performance history when making renewal or termination decisions or responding to questions.

---

## 3. Objectives & Success Criteria

### Objectives

1. Define a **canonical schema** for contractor performance: contractors, performance events, PIP status, and risk indicators.
2. Implement this schema in a **standardized spreadsheet** to:

   * clean existing data,
   * validate the structure with real usage,
   * and provide immediate value to the business.
3. Make **PIP and risk status visible** at a glance, instead of being buried in notes across multiple tabs.
4. Capture key elements of the process owner’s expertise as **repeatable structures and rules** rather than purely tacit knowledge.

### Success criteria (Phase 1)

* All in-scope active contractors are represented in a central **Contractors** register.
* New performance-relevant notes are captured via a single **Events** log instead of per-contractor tabs.
* The process owner and leads can identify, from a single management view:
  * who is on a performance-improvement plan, and
  * which contractors are currently “at risk”.

* Time to prepare a performance summary for a contractor (for renewal/decision) is reduced compared to the current state (qualitative feedback acceptable for Phase 1).

---

## 4. Schema-Driven Solution Overview

The project will use a **schema-driven approach**:

* Define a canonical `contractor_performance.v1` schema that describes:

  * **Contractor**: identity, role, lead, status, PIP and risk indicators.
  * **PerformanceEvent**: dated events such as 1:1s, coaching, escalations, kudos, PIP milestones.
  * **Taxonomies**: event types, themes, severities, statuses, PIP statuses, follow-up statuses.
  * **Derived fields**: last event date, risk flag, risk reason.
* Treat this schema as the **source of truth**. All implementations (spreadsheets, dashboards, future applications, APIs) are projections of this schema.
* Use Phase 1 to **instantiate and validate** the schema in Google Sheets, using real data and feedback to refine it before any higher-investment build.

High-level domain entities:

* **Contractor** – one row per contractor (master data + status).
* **PerformanceEvent** – one row per performance event (1:N with Contractor).
* **PIP status** – fields on Contractor + event types related to plans.
* **RiskIndicator** – derived from events and/or PIP status.
* **Enums** – `Status`, `PIPStatus`, `EventType`, `Theme`, `Severity`, `FollowUpStatus`.

A JSON-style schema definition (v1) and risk rule configuration will be maintained as separate artifacts (see Appendix A/B).

---

## 5. Proposed Phase 1 Solution (MVP)

**Goal:** Deliver a low-friction, spreadsheet-based implementation of the schema that improves structure and visibility without requiring a new platform.

### Key elements

1. **Contractors register (tab: Contractors)**

   * Single list of contractors with core fields such as:
     `contractor_id`, `name`, `vendor`, `group`, `role`, `lead`, `timezone`, `start_date`, `status`, `pip_status`, `pip_start_date`, `last_event_date`, `risk_flag`, `risk_reason`.
   * `contractor_id` is a unique key used to link events and avoid duplicates.

2. **Events log (tab: Events)**

   * Single log of all performance-relevant events for in-scope contractors.
   * Each row represents a `PerformanceEvent` with fields such as:
     `event_id`, `date`, `contractor_id`, `event_type`, `theme`, `severity`, `summary`, `details`, `created_by`, `follow_up_date`, `follow_up_status`, `attachments_present`.
   * Uses dropdowns for `event_type`, `theme`, `severity`, and `follow_up_status` aligned with the schema’s enums.

3. **Risk and PIP indicators**

   * `pip_status` and `pip_start_date` managed on the Contractors register.
   * `event_type` values such as `PIP_START`, `PIP_CHECKIN`, `PIP_END` provide event-level history.
   * `risk_flag` and `risk_reason` computed using simple, documented rules, for example:

     * At risk if `pip_status = FORMAL_PLAN`, or
     * At risk if there are ≥ X high-severity negative events in the last N days.

4. **Management view (tab: Dashboard/Overview)**

   * Views showing, at minimum:

     * Contractors with `risk_flag = TRUE`.
     * Contractors with active PIP (`pip_status` ≠ `NONE`).
     * Recent high-severity events over a defined window (e.g., last 30 days).

5. **Data validation & protection**

   * Controlled lists for enums (status, event type, theme, severity, etc.).
   * Protected ranges for header rows, formulas, and validation rules to reduce accidental damage.

---

## 6. Scope (Phase 1)

### In scope

* Define and document the `contractor_performance.v1` schema and enums.
* Implement the schema in a standardized Google Sheets workbook (Contractors, Events, Dashboard).
* Backfill **limited** historical data (e.g., recent months and/or high-priority contractors), not full historical migration.
* Define and implement basic risk and PIP rules using spreadsheet formulas and/or helper tabs.
* Co-design categories (event types, themes, severities) and views with the process owner and key leads.
* Pilot with an agreed subset of teams/contractors and refine based on feedback.

### Out of scope (Phase 1)

* New application UI or custom front-end.
* Integration with HRIS, vendor systems, or ticketing platforms.
* Automated notifications, workflows, or approvals.
* Full historical backfill for all contractors.
* Formal changes to HR/performance policy (this project supports the process; it does not define policy).

## 6.1 Non-Goals (Phase 1)

The following are explicitly not goals of Phase 1:
* Designing or delivering a full, long-term platform for contractor management.
* Changing HR or performance-management policy.
* Automating all aspects of contractor lifecycle (onboarding, offboarding, timesheets, etc.).
* Providing a complete analytics solution for all contractor-related questions.

Phase 1 is focused on establishing a clean, usable schema and basic visibility into history, PIPs, and risk.

---

## 7. Stakeholders

* **Process owner / SME** – defines categories, review logic, and risk patterns based on deep experience; co-signs schema and Phase 1 behavior.
* **Team leads / managers** – record events, review contractor histories, and act on risk/PIP signals.
* **Business unit leadership** – consume management view to understand contractor risk profile and support decisions.
* **IT / data** (as needed) – advise on future technical implementation, integrations, and security when moving beyond Phase 1.

---

## 8. Functional Requirements (Phase 1 – Summary)

Detailed list in Appendix C; summary below.

The Phase 1 system will:

1. Provide a **central Contractor register** with unique identifiers and key attributes.
2. Provide a **central Events log** where each performance-relevant event is captured with date, contractor, type, theme, severity, summary, and details.
3. Standardize classification of events via **controlled enums** (event type, theme, severity, status, PIP status, follow-up status).
4. Allow users to **view the chronological history** of events for a given contractor.
5. Track whether a contractor is on a **performance-improvement plan** and associate PIP-related events with that plan.
6. Compute and display a basic **risk flag** and risk reason for each contractor using transparent rules.
7. Provide a **management overview** listing at-risk contractors, contractors on PIP, and recent high-severity events.
8. Enforce basic **data validation** and protect schema/logic from accidental edits.

---

## 9. Phased Delivery Approach

The core design decision is to validate the data model and working practices in a low-risk, spreadsheet-based implementation (Phase 1) before investing in a more technical application and automation layer (Phase 2).

### Phase 1 – Schema & Spreadsheet Implementation

**Purpose:**
Establish a clean, standardized view of contractor performance using the current spreadsheet platform, and confirm that the data model and rules work for the business.

**Key activities:**

* Workshop the schema and enums with the process owner and leads.
* Implement Contractors, Events, and Dashboard tabs aligned to the schema.
* Backfill a manageable set of historical data.
* Configure initial risk and PIP rules and test them against real examples.
* Run a pilot period, collect feedback, and refine the schema and rules.

**Outcome:**
Validated `contractor_performance.v1` schema, improved data quality, and a working spreadsheet-based system that delivers immediate visibility into history, PIPs, and risk.

### Phase 2 – Application & Automation (Future)

**Purpose:**
Build a more robust application layer and automation on top of the validated schema.

**Potential activities (to be defined later):**

* Implement application UI for event capture and history views.
* Implement role-based access control.
* Add reminders, alerts, and periodic summaries based on the risk and PIP logic.
* Integrate with HRIS/vendor systems to reduce duplicate data entry.
* Move from spreadsheet formulas to application or service-level rule evaluation.

**Outcome:**
A more scalable, user-friendly solution that still speaks the same schema, reducing rework and ensuring continuity with Phase 1.

---

## 10. Risks, Assumptions, and Open Questions

### Risks

* Users may continue to use legacy per-contractor tabs unless migration and expectations are clear.
* Overcomplicating enums or rules in Phase 1 could make data entry burdensome.
* Risk and PIP rules may initially misalign with SME intuition and require iteration.

### Assumptions

* The business is willing to standardize on a single workbook for in-scope contractors in Phase 1.
* The process owner and at least a small group of leads can invest time in co-design and pilot feedback.
* Google Sheets is acceptable as a Phase 1 implementation platform.

### Open questions (examples)

* Exact pilot scope: which teams, which contractors, what time window for backfill?
* Finalized lists for event types, themes, and severities.
* Target thresholds for risk rules (number and recency of events).
* Ownership for ongoing maintenance of enums and rules.

---

## Appendix A – Contractor Performance Schema v1 (Sketch)

> Note: This is a simplified, human-readable representation. A formal JSON Schema artifact can be maintained separately.

**Entity: Contractor**

* `contractor_id` (string, required, unique)
* `name` (string, required)
* `vendor` (string, optional)
* `group` (string, optional)
* `role` (string, optional)
* `lead` (string, optional)
* `timezone` (string, optional)
* `start_date` (date, required)
* `status` (enum: ACTIVE, ONBOARDING, ENDED)
* `pip_status` (enum: NONE, WATCH, FORMAL_PLAN, FORMERLY_ON_PLAN)
* `pip_start_date` (date, optional)
* `last_event_date` (date, derived/optional)
* `risk_flag` (boolean, derived/optional)
* `risk_reason` (string, derived/optional)

**Entity: PerformanceEvent**

* `event_id` (string, required, unique within system)
* `date` (date, required)
* `contractor_id` (string, required; FK → Contractor)
* `event_type` (enum: ONE_TO_ONE, COACHING, ESCALATION, KUDOS, PIP_START, PIP_CHECKIN, PIP_END, OTHER)
* `theme` (enum: ATTENDANCE, QUALITY, COMMUNICATION, ATTITUDE, PROCESS, POLICY, OTHER)
* `severity` (enum: LOW, MEDIUM, HIGH)
* `summary` (string, required)
* `details` (string, optional)
* `created_by` (string, optional)
* `follow_up_date` (date, optional)
* `follow_up_status` (enum: NOT_NEEDED, PLANNED, COMPLETED)
* `attachments_present` (boolean, optional)
* `attachment_refs` (array of strings, optional)

**Enums**

* `Status`: ACTIVE, ONBOARDING, ENDED
* `PIPStatus`: NONE, WATCH, FORMAL_PLAN, FORMERLY_ON_PLAN
* `EventType`: ONE_TO_ONE, COACHING, ESCALATION, KUDOS, PIP_START, PIP_CHECKIN, PIP_END, OTHER
* `Theme`: ATTENDANCE, QUALITY, COMMUNICATION, ATTITUDE, PROCESS, POLICY, OTHER
* `Severity`: LOW, MEDIUM, HIGH
* `FollowUpStatus`: NOT_NEEDED, PLANNED, COMPLETED

---

## Appendix B – Example Risk Rule Specification (v1)

```yaml
risk_rule_v1:
  description: "Basic risk flag for contractors"

  conditions:
    - name: "On formal PIP"
      if:
        pip_status: "FORMAL_PLAN"
      then:
        risk_flag: true
        risk_reason: "On formal performance plan"

    - name: "Recent high severity issues"
      if:
        events_in_last_days:
          days: 60
          severity: "HIGH"
          count_gte: 2
      then:
        risk_flag: true
        risk_reason: ">=2 high severity events in last 60 days"
```

Implementation detail (Phase 1): realized via spreadsheet formulas and filters; in later phases, this can be implemented as code or configuration in the application layer.

---

## Appendix C – Functional Requirements (Phase 1 – Detailed)

Scope: Google Sheets (or equivalent) implementation of the schema. Low-tech, schema-first, cleanup/validation phase.

### 1. Contractor master data

**FR1.01 – Central contractor register**
The system shall provide a centralized **Contractors** register containing, at minimum:
`contractor_id`, `name`, `vendor` (optional), `group`, `role`, `lead`, `timezone`, `start_date`, `status`.

**FR1.02 – Unique contractor identifier**
The system shall enforce a unique `contractor_id` per contractor and use it as the primary key for linking events.

**FR1.03 – Maintain contractor status**
The system shall allow authorized users to update contractor `status` (e.g., ACTIVE, ONBOARDING, ENDED).

---

### 2. Performance events / history

**FR1.04 – Central events log**
The system shall provide a single **Events** log where each row represents one performance-relevant event for a contractor.

**FR1.05 – Minimum event fields**
Each event shall capture at least:
`date`, `contractor_id`, `event_type`, `summary`.

**FR1.06 – Structured classification**
The system shall provide controlled dropdowns (data validation) for `event_type`, `theme`, `severity`, and `follow_up_status` aligned with the schema enums.

**FR1.07 – Link events to contractors**
Events shall be linkable to contractors via `contractor_id`, and users shall be able to filter the Events log by contractor to see a history.

**FR1.08 – Follow-up tracking**
The system shall support optional follow-up tracking per event via `follow_up_date` and `follow_up_status`.

---

### 3. Viewing contractor history

**FR1.09 – Chronological timeline**
The system shall allow users to view a **chronological list** of events for a given contractor (e.g., via filter or dedicated view).

**FR1.10 – Recent activity indicators**
The system shall compute and display for each contractor:

* `last_event_date`
* optionally, count of events in the last N days (where N is documented).

---

### 4. PIP (Performance Improvement Plan) tracking

**FR1.11 – PIP status fields**
The system shall track PIP-related information at contractor level via `pip_status` and `pip_start_date`.

**FR1.12 – PIP-related events**
The system shall allow events to be flagged as PIP-related via specific values of `event_type` (e.g., PIP_START, PIP_CHECKIN, PIP_END).

**FR1.13 – Identify contractors on PIP**
The system shall provide a way (filter or view) to list all contractors with `pip_status` not equal to NONE.

---

### 5. Risk identification

**FR1.14 – Basic risk flag**
The system shall compute a boolean `risk_flag` for each contractor based on documented rules using data from the Events log and/or PIP status.

**FR1.15 – Risk reason**
The system shall provide a `risk_reason` text field that summarizes *why* a contractor is flagged (either via formula or manual entry).

---

### 6. Management view

**FR1.16 – At-risk contractors view**
The system shall provide a management view listing all contractors with `risk_flag = TRUE`, showing at least `name`, `group`, `lead`, `risk_reason`, `last_event_date`, `pip_status`.

**FR1.17 – PIP overview**
The system shall provide a management view listing contractors where `pip_status` indicates an active plan, including `name`, `group`, `lead`, `pip_start_date`, and most recent PIP-related event date.

**FR1.18 – Recent high-severity events**
The system shall provide a view (e.g., filtered table or pivot) showing high-severity events within a defined recent period (e.g., last 30 days), including `date`, `contractor`, `lead`, `event_type`, `theme`, `severity`, `summary`.

---

### 7. Data quality & validation

**FR1.19 – Enum enforcement**
The system shall enforce controlled lists for all enum-based fields (`status`, `pip_status`, `event_type`, `theme`, `severity`, `follow_up_status`) using spreadsheet data validation.

**FR1.20 – Minimal event validation**
The system shall prevent an event from being considered valid unless, at minimum, `date`, `contractor_id`, `event_type`, and `summary` are present.

**FR1.21 – Protected structure**
The system shall protect schema-related elements (headers, formulas, validation lists) against accidental edits via protected ranges or equivalent.

---

### 8. Configuration & maintainability

**FR1.22 – Admin-maintained lists**
The system shall allow a designated admin/process owner to maintain the values of enum lists (e.g., add a new `Theme`) without structural redesign.

**FR1.23 – Documented risk rule**
The system shall document the risk rule(s) used to compute `risk_flag` and `risk_reason` in a human-readable form (e.g., separate tab or YAML-like spec) so they can be revised in future phases.

---
## Appendix D – Functional Requirements (Phase 2: Application & Automation – Draft)

Phase 2 assumes Phase 1 schema is validated. Now we build a “real” application (UI + services) *against that schema*, plus automation and integrations.

### 1. Schema & validation

**FR2.01 – Schema-based data model**
The application shall implement the validated `contractor_performance` schema (Contractor, PerformanceEvent, enums, derived fields) as its underlying data model.

**FR2.02 – Schema validation on input**
All create/update operations for contractors and events shall validate input against the schema (types, required fields, enum values).

**FR2.03 – Schema versioning**
The application shall record the schema version used for persisted data (e.g., `v1`, `v1.1`) to support future evolution.

---

### 2. User authentication & authorization

**FR2.04 – Authenticated access**
The application shall require users to authenticate before accessing contractor performance data.

**FR2.05 – Role-based access control (RBAC)**
The application shall support role-based permissions, at minimum:

* Admin / Process Owner
* Manager / Lead
* Read-only viewer

**FR2.06 – Scoped visibility (optional / advanced)**
The application shall support limiting visibility such that managers see only contractors within their scope (e.g., group, team, or explicit assignment), while admins can see all contractors.

---

### 3. Contractor management (UI & CRUD)

**FR2.07 – Contractor CRUD**
The application shall allow authorized users to create, view, update, and (logically) deactivate Contractor records.

**FR2.08 – Contractor profile view**
The application shall provide a **Contractor profile** page showing:

* Core contractor attributes
* Current status & PIP status
* Risk flag & risk reason
* Timeline of events (see below)

**FR2.09 – Bulk import/update**
The application shall support bulk import or sync of contractors from an external source (e.g., CSV/HRIS export) into the Contractor entity.

---

### 4. Event capture & history (UI)

**FR2.10 – Event creation form**
The application shall provide a guided form for creating new PerformanceEvents with:

* Enum-based dropdowns for `event_type`, `theme`, `severity`
* Required field enforcement
* Date picker for `date`

**FR2.11 – Event timeline per contractor**
The application shall display a chronological timeline of events per contractor, with filtering by `event_type`, `theme`, and `severity`.

**FR2.12 – Event edit/audit constraints**
The application shall allow edits to events within defined rules (e.g., within X days) and maintain an audit trail of changes (who edited what and when).

**FR2.13 – Attachments / evidence**
The application shall allow attaching references (links or files, depending on platform constraints) to events to replace ad-hoc screenshots.

---

### 5. PIP management

**FR2.14 – PIP lifecycle support**
The application shall support initiating, tracking, and closing PIPs using PIP-related event types and Contractor-level PIP fields.

**FR2.15 – PIP summary view**
For contractors with `pip_status` not NONE, the profile shall display:

* PIP start date
* PIP goal/description (if captured)
* PIP check-in events
* PIP end/outcome

**FR2.16 – PIP templates (optional)**
The application shall optionally support simple PIP templates (e.g., common goals or steps) to standardize documentation.

---

### 6. Risk rules & engine

**FR2.17 – Configurable risk rules**
The application shall externalize risk logic into a configurable rule set (e.g., admin-editable thresholds and conditions) instead of hard-coding in UI or queries.

**FR2.18 – Real-time risk evaluation**
The application shall re-evaluate risk for a contractor when relevant data changes (e.g., new event, status change, PIP change) and update `risk_flag` and `risk_reason`.

**FR2.19 – Rule transparency**
For each risk-flagged contractor, the application shall expose *which rule(s)* caused the flag (e.g., “On formal PIP”, “3+ High severity events in 60 days”).

---

### 7. Dashboards & reporting

**FR2.20 – Team/BU overview dashboard**
The application shall provide a dashboard summarizing, at least:

* Count of contractors by status
* Count and list of at-risk contractors
* Count and list of contractors on PIP
* Recent high-severity events

**FR2.21 – Filters & drill-down**
The dashboard shall support filtering by group, lead/manager, vendor, and date range, and allow drill-down into individual contractors/events.

**FR2.22 – Export capability**
The application shall allow exporting data (e.g., CSV, Excel) for contractors and events for further analysis or backup.

---

### 8. Notifications & automation

**FR2.23 – PIP check-in reminders**
The application shall support reminders to responsible leads for planned PIP check-ins based on `follow_up_date` fields.

**FR2.24 – New risk alerts**
The application shall support alerts (email/Slack/whatever channel) when a contractor transitions from not-at-risk to at-risk under the configured rules.

**FR2.25 – Periodic summaries**
The application shall support periodic summary notifications (e.g., weekly digest) with key changes: new PIPs, closed PIPs, new risk flags, upcoming follow-ups.

---

### 9. Integrations (optional but typical Phase 2)

**FR2.26 – HRIS/vender ID linkage**
The application shall support storing external IDs (e.g., HRIS ID, vendor system ID) on Contractor records to enable integration.

**FR2.27 – Upstream data sync (read)**
The application shall support one-way sync or import of basic contractor attributes from an external system of record, where feasible.

**FR2.28 – Downstream reporting integration (read)**
The application shall expose data for downstream reporting (e.g., BI, data warehouse) via export or API.

---

### 10. Audit, security, and compliance

**FR2.29 – Audit logging**
The application shall maintain an audit log for sensitive operations (create/update/delete events, changes to risk/PIP status, changes to rules).

**FR2.30 – Data retention & access review**
The application shall support documented retention and access-review practices aligned with HR/legal guidance (even if manual in early versions).

---

