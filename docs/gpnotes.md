# **Authoring Notes: RPA Team Guiding Principles**

---

## **1. Intent & Positioning**

* Purpose is not to dictate — it’s to *standardize and de-risk*.
* Document should function as both a **playbook** for day-to-day work and a **governance artifact** for leadership.
* Keep the tone **instructional, not bureaucratic** — avoid policy-speak.
* Target audience spans skill levels: senior developers, citizen developers, and support engineers.
* Must be **platform-agnostic in philosophy**, but **platform-specific in implementation guidance** (AA, Workato, Apps Script only).

---

## **2. Structure Planning**

**Sections brainstormed early:**

* Purpose and Objectives
* Core Principles
* Design/Architecture Standards
* Logging & Error Handling
* Peer Review Process
* Operational Support
* Platform-Specific Addenda
* Governance
* Continuous Improvement

**Decision:** Keep this single document concise (<10 pages). Use appendices and linked templates for deeper dives (SDDs, SOPs, checklists).

**Cross-reference:** Existing CoE documentation, AA Control Room runbooks, and Workato recipe conventions.

---

## **3. Goals & Desired Outcomes**

* **Consistency:** Developers across platforms should design and log automations in the same recognizable pattern.
* **Traceability:** Every automation run can be traced back through logs with a unique correlation ID.
* **Support readiness:** When something fails at 2 AM, anyone in support should know exactly where to look and what to check.
* **Security alignment:** No credential sprawl, no personal accounts.
* **Ease of onboarding:** New hires should be productive within a week using this doc as a guide.

---

## **4. Research & Benchmarking Notes**

**Industry Models Reviewed:**

* Deloitte RPA CoE Maturity Model — emphasis on governance & reusability.
* Google Cloud documentation on logging & observability patterns (for Workato + Apps Script).
* Automation Anywhere best practices (error handling, credential vault, modularization).
* Internal Workato SDK references for recipe modularity and connector design.

**Lesson:** Common failure pattern is lack of standard logging and no clear ownership model after deployment.

---

## **5. Drafting Design Standards**

**Design Principles Considered:**

* *Modularity:* Must enforce clear separation between logic and configuration.
* *Scalability:* Encourage event-driven or batch-driven patterns over synchronous serial flows.
* *Idempotence:* Every automation must be safe to rerun.
* *Resilience:* Explicit retries, fallback paths, and circuit breakers.

**Decision:** Include explicit mention of idempotence and retries — often omitted in smaller teams.

**Rationale:** Helps unify system behavior across AA and Workato (both support retries but implement differently).

---

## **6. Logging Framework Decisions**

* Logging format must be **portable** — JSON or CSV, not plain text.
* Define a minimal standard (timestamp, run_id, bot/recipe, severity, message).
* Encourage correlation IDs to connect Workato runs → downstream APIs → Apps Script logs.
* Clarify log destinations:

  * AA → Control Room
  * Workato → Data Tables or GCS/BigQuery
  * Apps Script → Cloud Logging or Sheets

**Design Note:** Avoid vendor lock-in; prefer logs that can be aggregated centrally later.

---

## **7. Peer Review Framework**

* Purpose: Enforce cross-validation and improve design quality.
* Required before UAT deployment.
* Use Git commits or formal review checklists.
* Review criteria brainstormed:

  * Naming conventions
  * Security (no plain credentials)
  * Logging completeness
  * Error handling
  * Documentation
* Capture in Appendix or Confluence form.

**Decision:** Keep this lightweight — peer review should take <30 min per automation.

---

## **8. Operational Considerations**

**Questions captured during design:**

* Who owns a bot after it’s deployed?
* How are incidents tracked and resolved?
* How do we ensure visibility for failed runs?
* What is the rollback plan?

**Action:** Define clear ownership model per automation (owner + backup contact).
**Decision:** Require previous version retention for rollback.

**Future idea:** Integrate run monitoring into a single dashboard (Control Room + Workato + Google Sheets aggregate).

---

## **9. Platform-Specific Notes**

**Automation Anywhere**

* Emphasize credential vault, audit logs, and error recovery via try/catch.
* Control Room provides native audit — leverage that instead of reinventing.

**Workato**

* Encourage structured telemetry envelopes and Data Tables logging.
* Recipes must have versioned exports (ZIP/Git).
* Avoid use of “magic strings” in recipes — all config externalized.

**Google Apps Script**

* Treat like lightweight backend service.
* Force use of properties for config.
* Logging via Cloud Logging or email alerts.
* Encourage modular script libraries for reuse.

---

## **10. Governance & Compliance Thoughts**

* Must align with IT’s change management process.
* Distinguish between *minor* and *major* updates (e.g., typo fix vs. logic change).
* Encourage tagging automations with owner, purpose, and version metadata.
* Suggest quarterly reviews and sign-offs by CoE.

**Note to self:** Governance shouldn’t block agility — just provide visibility and safety rails.

---

## **11. Document Maintenance**

* Living document — version every quarter (e.g., v1.0, v1.1).
* Keep change log at the top (who edited what, when).
* Encourage open feedback loop with developers after each incident or new tech adoption.

**Future add-ons planned:**

* Example logging templates (JSON schema).
* Peer review checklist.
* Design Document (SDD) template.
* Support incident playbook.

---

## **12. Drafting Tone and Style**

* Write in **plain language** — no fluff or buzzwords.
* Favor imperative phrasing (“Use”, “Ensure”, “Avoid”).
* Keep each section action-oriented.
* Treat developers as partners, not rule-followers.

---

## **13. Open Questions to Resolve Before Publishing**

* Who is the formal owner of this document? CoE lead or entire RPA governance group?
* How will peer reviews be logged — via Git commits, shared form, or ticket comments?
* Where is the official repository — Drive, Confluence, or Git?
* Does support have full access to platform logs, or will exports be needed?
* Should there be a maturity scale (e.g., compliance levels) or just a pass/fail model?

---

## **14. Next Actions (Pre-Release Checklist)**

* [ ] Review with automation leads and support manager.
* [ ] Add versioning & ownership metadata to top of doc.
* [ ] Create skeleton templates for SDD, peer review, and runbook.
* [ ] Publish under RPA CoE shared folder with read-only link.
* [ ] Schedule first quarterly review (Jan 2026).
