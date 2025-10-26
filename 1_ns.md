# **RAG Email System — Pre-Testing Overview Script**

---

### **Scene 1 – Context: Why We’re Building It**  *(0:00 – 0:30)*

> “Across our teams, handling inbound email is routine but time-consuming.
> Many messages repeat familiar questions or require information already documented elsewhere.
> The RAG Email System is designed to ease that burden — retrieving relevant material and composing grounded draft replies that remain accurate, transparent, and easy to verify.”

---

### **Scene 2 – Purpose of This Phase**  *(0:30 – 1:00)*

> “This stage is the prototype phase — focused on readiness, not performance claims.
> Our goal is to confirm that each component works reliably and meets governance and security standards before any live testing begins.
> The result is a stable foundation for controlled evaluation.”

---

### **Scene 3 – System Overview**  *(1:00 – 1:30)*

> “When a message arrives, the workflow activates automatically.
> Gmail passes the email to Workato, which coordinates retrieval through Google Cloud and Vertex AI.
> The system searches approved knowledge sources, builds a cited draft, and stores it as a Gmail draft — never sending anything automatically.”

---

### **Scene 4 – What’s Working Now**  *(1:30 – 2:00)*

> “Core functions are in place.
> The connectors process data correctly; the retrieval and generation pipeline returns coherent, sourced drafts.
> Every action records telemetry — timing, cost, and trace identifiers — so that output can be traced back to its inputs.
> Security and error-handling have been validated in controlled runs.”

---

### **Scene 5 – Preparing for Testing**  *(2:00 – 2:45)*

> “Next, the system will enter shadow testing.
> It will observe real email traffic and generate draft responses in the background.
> These drafts will be stored for review but not sent.
> Reviewers will evaluate factual grounding, latency, and operational stability to verify that the system behaves as expected before any human-in-the-loop pilot.”

---

### **Scene 6 – Governance and Safety**  *(2:45 – 3:15)*

> “All activity runs under a dedicated service account with least-privilege access.
> Data is encrypted in transit and at rest.
> Each request is authenticated, logged, and auditable.
> No automated action occurs without human oversight.
> This design ensures that accuracy and accountability remain central to every step.”

---

### **Scene 7 – What Success Looks Like**  *(3:15 – 3:45)*

> “Success will be measured, not assumed.
> During testing, we’ll confirm that generated drafts are correctly sourced, produced within target time, and compliant with data-handling standards.
> These results will determine readiness for the next phase — a limited pilot with human reviewers.”

---

### **Scene 8 – Next Steps**  *(3:45 – 4:00)*

> “With the prototype complete, attention turns to controlled testing.
> The coming weeks will focus on validation, feedback, and refinement — confirming that the RAG Email System delivers reliable automation grounded in evidence.”

---

### **Scene 9 – Closing Slide**  *(4:00 – 4:05)*

> “RAG Email System.
> Grounded automation for real work.”

---

### **Voice & Delivery Notes**

* **Tone:** measured, factual, calm — no adjectives beyond necessary descriptors.
* **Pace:** ~115 words per minute.
* **Inflection:** slight downward cadence at ends of sentences; no marketing enthusiasm.
* **Music:** optional ambient pad or silence.
* **Captions:** verbatim for clarity and accessibility.

---
