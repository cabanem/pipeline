# **RPA Team Guiding Principles**

**Version:** 1.0
**Prepared by:** 
**Applies to:** Automation Anywhere, Workato, and Apps Script automations
**Last Updated:** October 2025

---

## **1. Purpose**

The purpose of this document is to define the guiding principles, standards, and expectations for automation development, deployment, and maintenance within the RPA Team.

It serves as a **reference framework** for developers, solution architects, and support personnel to ensure consistency, maintainability, and operational excellence across all automation platforms.

This document is a **living standard** — to be reviewed and refined as the team’s tools, technologies, and practices evolve.

---

## **2. Objectives**

The guiding principles outlined here aim to:

**1. Support maintainability.**
Ensure every automation can be readily understood, updated, and supported by someone other than its original developer.

**2. Guide day-to-day operations.**
Provide consistent practices for monitoring, scheduling, alerting, and incident response.

**3. Define standards for logging and error handling.**
Enable predictable diagnosis and recovery from failures through consistent log structures and error-handling conventions.

**4. Promote peer review and shared accountability.**
Establish peer review as a formal step in the development lifecycle to ensure quality, security, and compliance before deployment.

---

## **3. Scope**

This framework applies to:

* All **Automation Anywhere**, **Workato**, and **Google Apps Script** automations developed, deployed, or maintained by the RPA team.
* All environments — development, testing, UAT, and production.
* Both citizen and professional developer contributions.

---

## **4. Core Principles**

### **4.1 Design and Architecture**

* **Modularity:** Build automations as composable components (e.g., login module, data processing module, notification module).
* **Scalability:** Design for parallelization and workload balancing (e.g., queues, batch processing).
* **Resilience:** Implement retry logic, idempotence (safe re-execution), and controlled degradation under load.
* **Security by Design:** Credentials must be stored in secure vaults (AA Control Room, Workato Connections, Apps Script Properties) and never hardcoded.
* **Separation of Concerns:** Keep business logic separate from configuration and data.
* **Documentation:** Every automation must include a README or metadata summary outlining its objective, inputs/outputs, dependencies, and failure modes.

---

### **4.2 Development Practices**

* **Version Control:** All scripts, bots, and recipe exports must be stored in Git or a centralized repository with version tagging (`vX.Y.Z`).
* **Naming Conventions:** Use clear, standardized naming across all platforms:

  * Workato: `Domain_Function_Action` (e.g., `HR_Process_PayrollSync`)
  * AA: `BOT_<System>_<Process>_<Step>`
  * Apps Script: follow camelCase for functions, snake_case for constants.
* **Code Readability:** No nested logic beyond three levels deep. Favor comments over cleverness.
* **Reusability:** Centralize shared logic (Workato libraries, AA MetaBots, Apps Script services).

---

### **4.3 Logging and Error Handling**

**Uniform logging standards** are critical for observability and support.

* **Minimum log fields:** Timestamp, bot/recipe name, run ID, message, and severity.
* **Severity levels:** `INFO`, `WARN`, `ERROR`, `FATAL`.
* **Error recovery:** Automations must:

  * Retry transient issues (network, API timeouts).
  * Gracefully fail and notify on critical issues.
  * Emit alerts via email, chat, or ticketing systems.
* **Log storage:**

  * AA logs retained in Control Room.
  * Workato logs exported to Data Tables or Cloud Logging if long-term retention is required.
  * Apps Script logs sent to Cloud Logging or a shared Google Sheet when appropriate.
* **Correlation IDs:** Use consistent IDs across systems for traceability (e.g., pass `corr_id` from Workato to downstream services).

---

### **4.4 Peer Review and Quality Assurance**

* **Mandatory peer review** before any deployment to UAT or production.
* **Review checklist** must cover:

  * Readability and naming standards
  * Logging and exception coverage
  * Security of credentials and data
  * Dependency documentation
  * Reusability and modularity
* **Review artifacts:** Approval must be logged (e.g., Git commit comment, Change Request, or Workato Note).
* **Quality gate:** No automation proceeds to production without peer sign-off.

---

### **4.5 Operations and Support**

* **Monitoring:**

  * Establish standard dashboards (AA Control Room, Workato Insights, Apps Script logs).
  * Track success/failure rates, queue depths, and average run times.
* **Incident Response:**

  * Maintain structured incident playbooks defining triage steps and escalation paths.
  * Use correlation IDs to quickly identify related runs.
  * Document root cause and resolution in a shared logbook.
* **Rollback Procedures:**

  * Always maintain the previous working version of a bot or recipe for immediate rollback.
* **Ownership:**

  * Each automation must have an assigned owner and backup contact.

---

## **5. Platform-Specific Standards**

### **5.1 Automation Anywhere**

* **Credential Handling:** Always use Control Room credential vaults.
* **Logging:** Use “Log to File” or “Send Email” blocks to externalize key execution details.
* **Error Recovery:** Implement try/catch and structured error messages for Control Room monitoring.
* **Audit Trail:** Ensure Control Room task history retention meets policy standards.

### **5.2 Workato**

* **Connector Usage:** Use Workato SDK connectors only when APIs are not natively supported.
* **Telemetry:** Capture structured logs via Data Tables or custom logging connectors.
* **Error Strategy:** Implement `on_error` steps to handle failures predictably.
* **Deployment:** Recipes in production must be locked and versioned; staging must mirror production topology.

### **5.3 Google Apps Script**

* **Authentication:** Use OAuth2 or Service Accounts with least privilege.
* **Configuration:** Store configurable values (e.g., URLs, keys) in Script Properties.
* **Logging:** Use `console.log()` for transient debugging, Cloud Logging for production monitoring.
* **Error Handling:** Use `try/catch` with detailed messages and fallback notifications via Gmail or Chat.
* **Deployment:** Maintain versioned deployments; avoid editing production scripts directly.

---

## **6. Documentation and Knowledge Sharing**

* Maintain **central documentation** in Confluence, Google Drive, or an equivalent repository.
* Include:

  * Solution Design Document (SDD)
  * Deployment Guide
  * Support Playbook
  * Peer Review Log
* Each automation must have an entry in the **Automation Inventory**, capturing metadata such as purpose, owner, dependencies, and support notes.

---

## **7. Governance and Compliance**

* **Change Management:**

  * All production deployments must be approved by the RPA Lead or CoE reviewer.
  * Major updates require a Change Request ticket or equivalent approval.
* **Auditability:**

  * Maintain full traceability of versions, approvals, and incident responses.
* **Data Security:**

  * All automations must comply with internal security and data handling standards.
  * PII (Personally Identifiable Information) must be masked, encrypted, or tokenized when stored or transmitted.

---

## **8. Continuous Improvement**

This framework must evolve with the technology landscape and lessons learned from production experience.
The RPA CoE will:

* Review this document quarterly.
* Collect feedback from developers, support, and stakeholders.
* Publish updated versions under controlled document management.

---

## **9. Appendix**

**Document Hierarchy**

1. **Guiding Principles (this document)** — strategic standards for design, governance, and support.
2. **Technical Standards** — detailed platform-specific rules (AA, Workato, Apps Script).
3. **Operational Procedures** — day-to-day support and maintenance instructions.
4. **Templates and Checklists** — peer review forms, SDD templates, deployment checklists.

---
