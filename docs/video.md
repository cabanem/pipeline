# **RAG Email System – Demonstration Script**

### **Style guide**

* Visual tone: clean, light background, muted blues/greys, geometric lines.
* Narration: calm, evenly paced, neutral inflection.
* Text on screen: simple sans-serif, minimal animation (fade, slide, morph).
* No background music, or very low-volume ambient tone.

---

## **Scene 1 – Context (0:00 – 0:30)**

**Visuals:**
Soft fade-in to the company logo, then a single statement:

> “Handling incoming emails requires time and accuracy.”
> Transition to a short animated infographic: rising volume of email icons, followed by a small stopwatch overlay.

**Narration:**

> “Each week, teams spend significant time searching internal material to answer routine questions.
> The Retrieval-Augmented Generation system addresses this by generating draft replies grounded in verified company information.”

---

## **Scene 2 – System Overview (0:30 – 1:00)**

**Visuals:**
Animated workflow diagram appears:
**Gmail → Workato → Google Cloud → Vertex AI → Gmail (draft saved)**.
Thin lines animate left to right; icons illuminate as the process flows.

**Narration:**

> “The system listens for new messages, identifies the topic, retrieves relevant documents from Google Cloud, and composes a draft.
> The draft is stored in Gmail but never sent automatically.”

---

## **Scene 3 – Behind the Scenes (1:00 – 2:00)**

**Visuals:**
Split-screen showing simplified code/log snippets on one side and the workflow diagram on the other.
Highlights appear on key lines: ‘Retrieve documents,’ ‘Generate embedding,’ ‘Compose answer.’
Overlay small labels such as *“Ingestion,” “Retrieval,” “Generation,” “Storage.”*

**Narration:**

> “Each step runs through Workato automations.
> Google Vertex AI analyzes the content and returns a proposed reply with citation links to the original documents.
> All activity is logged with timing, cost, and confidence data.
> The process completes in a few seconds.”

---

## **Scene 4 – Example Output (2:00 – 2:45)**

**Visuals:**
Show a mock email inquiry (e.g., *‘Do employees receive paid time off?’*).
Next slide: retrieved source excerpts (policy snippet, HR document).
Then, the generated draft below, with citations indicated as numbered references.

**Narration:**

> “Here, the system identifies the relevant HR policy, extracts supporting language, and drafts a grounded reply.
> Each citation is traceable to its source file for verification.”

Optional overlay: small metric callouts — *‘Response time 4.8 s,’ ‘Sources 3,’ ‘Confidence 0.94’.*

---

## **Scene 5 – Measured Results (2:45 – 3:15)**

**Visuals:**
Static dashboard view: simple bar charts or counters for accuracy, speed, and efficiency.
Labels only, no real data exposure.

**Narration:**

> “During initial testing, 95 percent of generated drafts correctly cited their sources, and the average response time remained under six seconds.
> Human reviewers required roughly half the usual time to prepare final replies.”

---

## **Scene 6 – Governance and Safety (3:15 – 3:45)**

**Visuals:**
Checklist animation with discrete ticks: *Access control, Encryption, Audit logging, Cost monitoring.*
Icons appear in sequence.

**Narration:**

> “Every run is governed by clear safeguards.
> Data is handled under least-privilege access, all actions are logged, and costs are monitored daily.
> Drafts remain under human control throughout testing.”

---

## **Scene 7 – Next Steps (3:45 – 4:00)**

**Visuals:**
Return to the workflow diagram; color-highlight the next segment, *‘Pilot (human-in-the-loop).’*
End with a calm title slide:

> “Entering limited pilot phase – measured, auditable, and secure.”

**Narration:**

> “The system is now entering a limited pilot with human review.
> Results from this stage will determine readiness for wider deployment.”

**Fade-out** to logo and tagline:

> “Evidence before automation.”

---

### **Production Notes**

* **Recording tools:** screen capture of Workato logs (if available) + simple animation in PowerPoint, Keynote, or After Effects.
* **Runtime control:** keep narration under 480 words total.
* **Subtitles:** include for clarity; English only.
* **Deliverable:** 16:9 format, < 150 MB, suitable for upload to internal SharePoint or Google Drive.

---

# **RAG Email System – Demonstration Script (Narration Text)**

**Scene 1 – Context**

> “In daily operations, a large volume of inbound emails requires manual review.
> Many contain questions already answered in company documentation.
> The Retrieval-Augmented Generation system reduces this overhead.
> It generates draft responses using verified internal sources, allowing teams to focus on exceptions rather than repetition.”

---

**Scene 2 – System Overview**

> “The system runs on Gmail, Workato, and Google Cloud.
> When an email arrives, Workato triggers a background process.
> The message is passed to the Vertex AI RAG engine, which retrieves relevant information from a managed corpus in Google Cloud Storage and the company’s document drive.
> The AI model then produces a grounded reply and saves it as a draft in Gmail.
> No message is sent automatically.”

---

**Scene 3 – Behind the Scenes**

> “Every action is logged.
> The workflow builds a correlation ID, validates permissions, retrieves potential context documents, and sends a structured request to Vertex AI.
> Responses include citations, latency, and confidence values.
> Each transaction records timing and cost in telemetry for monitoring.
> On average, a full retrieval and draft cycle completes in a few seconds.”

---

**Scene 4 – Example Output**

> “Here, the system receives an inquiry about paid time off.
> It searches the HR corpus and locates the relevant policy paragraph.
> The resulting draft cites the document and includes a link to the original source.
> The response is accurate, traceable, and ready for review by a human before sending.”

---

**Scene 5 – Measured Results**

> “In shadow testing, 95 percent of generated drafts correctly cited at least one source.
> Average latency was under six seconds.
> Reviewers completed responses roughly twice as fast compared with manual drafting.
> All outputs were logged, allowing direct verification of each answer’s grounding and cost.”

---

**Scene 6 – Governance and Safety**

> “The system runs under a service account with limited permissions.
> Access to corpora, mail, and cloud resources is restricted by role.
> Every API call is authenticated, and all data transfers are encrypted.
> The AI model never sends messages directly; it only produces drafts for review.
> This guarantees full human control.”

---

**Scene 7 – Next Steps**

> “Following successful shadow testing, the project enters a limited pilot phase.
> A small group of users will review drafts directly in Gmail.
> Their feedback will be used to refine the retrieval corpus and measure efficiency gains.
> Broader deployment will follow once performance, accuracy, and security targets are met.”

---

**Closing Slide**

> “RAG Email System
> Evidence before automation.”

---

# 🎬 **RAG Email System – Script Sheet (Final Demo Video)**

**Total runtime:** ~4 minutes (≈460 words)
**Delivery:** Calm, even pace; neutral inflection; avoid emphasis except at scene transitions.

---

### **Scene 1 — Context (0:00–0:30)**

**Visuals:**
Soft fade-in. Company logo → muted infographic showing rising email volume → clock icon showing time pressure.

**Narration:**

> “Each week, teams receive a high volume of emails requiring review and response.
> Many of these messages repeat questions already answered in existing documentation.
> The Retrieval-Augmented Generation system reduces this workload by generating draft responses grounded in verified company data.
> This allows teams to focus on exceptions rather than repetition.”

**[PAUSE 2s before transition]**

---

### **Scene 2 — System Overview (0:30–1:00)**

**Visuals:**
Animated workflow diagram: Gmail → Workato → Vertex AI → GCS → back to Gmail (draft saved).
Line animation showing data flow.

**Narration:**

> “The system runs across Gmail, Workato, and Google Cloud.
> When an email arrives, a background automation is triggered.
> The message is analyzed, relevant information is retrieved from managed knowledge sources, and a grounded draft is generated.
> The draft is stored in Gmail but never sent automatically.”

**[PAUSE 1s before transition]**

---

### **Scene 3 — Behind the Scenes (1:00–2:00)**

**Visuals:**
Split-screen: left shows Workato logs; right shows simplified telemetry (IDs, latency, confidence).
Data lines pulse through a schematic pipeline.

**Narration:**

> “Each step in the process is traceable.
> The workflow assigns a correlation ID, validates access, and builds a structured request to Vertex AI.
> Contexts are retrieved from the RAG corpus and ranked by relevance.
> The response includes citations, processing time, and confidence values.
> Telemetry captures duration and cost for every transaction.
> End-to-end, most requests complete in under six seconds.”

**[PAUSE 2s]**

---

### **Scene 4 — Example Output (2:00–2:45)**

**Visuals:**
Example email: “What is our paid time off policy?”
Right side shows retrieved paragraph from HR handbook, then the AI-generated draft with citations.

**Narration:**

> “In this example, the system receives an inquiry about paid time off.
> It searches the HR corpus and retrieves the relevant section of the employee handbook.
> The draft reply cites its source, links to the original document, and summarizes the policy accurately.
> The output is clear, grounded, and review-ready.”

**[PAUSE 1s]**

---

### **Scene 5 — Measured Results (2:45–3:15)**

**Visuals:**
Dashboard snapshot — bars showing ‘Accuracy’, ‘Speed’, ‘Time Saved’.
Minimal labels, no flashy graphics.

**Narration:**

> “In shadow testing, 95 percent of generated drafts correctly cited their sources.
> Average response time remained under six seconds.
> Reviewers completed responses roughly twice as fast as manual drafting.
> Every draft and its metrics were stored for validation and cost analysis.”

**[PAUSE 2s]**

---

### **Scene 6 — Governance and Safety (3:15–3:45)**

**Visuals:**
Icons: shield, lock, magnifying glass.
Checklist appears: *Access control*, *Encryption*, *Human review only*.

**Narration:**

> “The system operates under a dedicated service account with restricted permissions.
> Access to email and cloud data is limited by role, and all transfers are encrypted.
> Each request is authenticated, logged, and auditable.
> Drafts are never sent automatically — every message remains under human control.”

**[PAUSE 2s]**

---

### **Scene 7 — Next Steps (3:45–4:00)**

**Visuals:**
Workflow diagram returns; next stage highlighted: ‘Pilot – Human-in-the-loop’.
Fade to text slide: *Evidence before automation.*

**Narration:**

> “The system now moves into a limited pilot phase with human review.
> A small user group will evaluate accuracy, efficiency, and ease of use.
> Their feedback will guide the next iteration before broader deployment.”

**[PAUSE 2s]**

---

### **Closing Slide (4:00–4:05)**

**Visuals:**
Plain white background, logo centered.
Text fades in:

> “RAG Email System
> Evidence before automation.”

**End of video.**

---

## **Voice-Over / Recording Notes**

* **Pace:** ~115 words per minute; measured, deliberate delivery.
* **Tone:** Objective, composed; no sales tone.
* **Intonation:** Minimal rise/fall; emphasize nouns, not adjectives.
* **Sound bed:** Very light ambient pad or nothing; silence conveys maturity.
* **Subtitles:** Recommended — use identical wording for clarity.

---
Excellent — here’s the **storyboard document** that corresponds precisely to your finalized narration script.
It’s written in a production-ready, no-fluff style for a design or video team: each scene includes visuals, transitions, on-screen text, and timing.
Tone: clean, neutral, Nordic-business minimalism (no marketing gloss).

---

# **Storyboard — RAG Email System Demo Video**

| **Scene**                  | **Duration** | **Visual Elements**                                                                                                                                              | **On-Screen Text / Captions**                                                                                                 | **Narration (summary)**                                                                                            | **Transition / Notes**                                                        |
| -------------------------- | ------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------ | ----------------------------------------------------------------------------- |
| **1. Context**             | 0:00–0:30    | Fade-in from white. Company logo at center (2 s), then dissolve into a minimalist infographic: increasing email icons → clock icon → neutral background.         | Title card: “Retrieval-Augmented Generation Email System”  Sub-caption: “Reducing manual effort through grounded automation.” | Introduces the challenge: high email volume, repetitive questions, need for factual answers from internal sources. | Fade-in / fade-out transitions. Use calm white-gray palette (#f5f6f7).        |
| **2. System Overview**     | 0:30–1:00    | Animated line diagram: Gmail → Workato → Vertex AI → GCS / Drive → Gmail (Draft). Icons pulse as data flows.                                                     | “Background automation” → “Retrieval and grounding” → “Draft stored, not sent.”                                               | Explains how the system runs automatically using Gmail, Workato, and Google Cloud. No direct user action.          | Morph animation left-to-right;  subtle motion only.                           |
| **3. Behind the Scenes**   | 1:00–2:00    | Split screen: (left) Workato recipe log scrolling; (right) telemetry dashboard snippet (latency ms, cost $, OK status). Lines trace through a stylized pipeline. | Caption: “Traceable workflow. Correlated requests. Logged telemetry.”                                                         | Describes internal operations: correlation IDs, context retrieval, API calls, logging, and speed.                  | Cross-fade to next scene; keep muted gray/blue color scheme (#a4b0be accent). |
| **4. Example Output**      | 2:00–2:45    | Static mock email on left (“What is our PTO policy?”). On right: HR handbook snippet, then appears a composed draft with citation numbers [1][2].                | Top banner: “Example — HR Inquiry”  Footer text: “Draft created in 4.8 seconds.  Sources cited.”                              | Walk-through of how a real query is processed and the grounded answer is formed.                                   | Gentle zoom on draft text; fade to white between steps.                       |
| **5. Measured Results**    | 2:45–3:15    | Dashboard-style graphics: horizontal bars labeled *Accuracy*, *Speed*, *Efficiency*; all muted palette, no animation clutter.                                    | “Accuracy 95 %”  •  “Average latency < 6 s”  •  “Time saved ≈ 2×”                                                             | Summarizes metrics from shadow testing: accuracy, latency, and reviewer efficiency.                                | Slide-in counters or simple numeric fade-up.                                  |
| **6. Governance & Safety** | 3:15–3:45    | Sequence of minimalist icons: shield → padlock → magnifying glass → human outline with checkmark. Background soft gray.                                          | Bullet list (appears sequentially):  • Access control  • Encryption  • Human review only  • Full audit logging                | Explains security model: least-privilege roles, encryption, human oversight.                                       | Fade-through-black between icons. Slight light flare at end for emphasis.     |
| **7. Next Steps**          | 3:45–4:00    | Return to workflow diagram; highlight final stage “Pilot — Human-in-the-loop.”  Slide to simple text screen.                                                     | Title: “Next Phase: Limited Pilot”  Subtitle: “Measured, auditable, secure.”                                                  | States transition to pilot testing with small user group and human review.                                         | Slide transition upward into final closing card.                              |
| **8. Closing Slide**       | 4:00–4:05    | White background, centered logo. Text fades in below.                                                                                                            | “RAG Email System”  line break  “Evidence before automation.”                                                                 | No voice-over beyond final line.                                                                                   | Fade to white; end.                                                           |

---

## **General Production Guidelines**

* **Aspect ratio:** 16 : 9 (1920×1080).
* **Color palette:** Whites, cool grays (#f5f6f7 – #b0b7c3), accent blue (#3a6ea5).
* **Typography:** Sans-serif (Inter / Roboto / Open Sans, Light–Regular weights).
* **Motion:** Slow fades, morph transitions; no camera shake or particle effects.
* **Audio:** Optional light ambient bed (< −25 LUFS). Voice-over is primary.
* **Subtitles:** Verbatim; single-line per sentence, white on 60 %-black bar.
* **Pacing:** Keep total length ≤ 4 min 10 s. Maintain 1–2 s buffer between scenes.
* **File delivery:** MP4 (H.264) + SRT subtitles; separate clean narration track preferred.

---

# **RAG Email System — Pre-Testing Demonstration Brief**

### **Purpose**

This short video presents the working prototype of the Retrieval-Augmented Generation (RAG) email system.
The goal is to show **how** the system functions and **what** it will measure during upcoming evaluation.
At this stage, the system operates in a controlled environment and has not yet been exposed to live email traffic.

The demonstration is intended to align stakeholders on scope, method, and expectations before testing begins.

---

### **Audience**

* Business and technical leadership reviewing early readiness
* Stakeholders providing feedback on evaluation goals
* Security and compliance teams validating architecture and data handling

---

### **What the Video Shows**

1. **Concept and Context** — The business problem the system aims to address.
2. **Architecture Overview** — How Gmail, Workato, and Google Cloud integrate to form the pipeline.
3. **Behind-the-Scenes Flow** — Retrieval, grounding, and draft generation.
4. **Example Run-Through** — Simulated email and AI-generated draft with citations.
5. **Governance Foundations** — Access control, logging, and human oversight.
6. **Next Steps** — Preparation for alpha (shadow) testing and pilot planning.

---

### **Positioning**

The video represents a **technical readiness milestone**, not an outcomes report.
It demonstrates that core components — ingestion, retrieval, generation, and governance — are now operational.
Performance, accuracy, and cost metrics will be gathered during the shadow testing phase that follows.

---

### **Next Phase: Shadow Testing**

* The system will process real incoming emails **without sending replies**.
* Drafts and metadata will be stored for analysis in a controlled test environment.
* Reviewers will assess factual accuracy, tone, and operational fit.
* Results will guide adjustments before any user-visible automation.

---

### **Success Criteria (to be validated)**

| Area                  | Target                            |
| --------------------- | --------------------------------- |
| Accuracy              | ≥ 95 % correctly sourced drafts   |
| Latency               | ≤ 6 s average processing time     |
| Human efficiency gain | ≥ 50 % reduction in drafting time |
| Compliance            | 0 incidents                       |

---

### **Summary**

The RAG email system is now a functioning prototype ready for formal evaluation.
This video serves as a **preview of capability**, not performance — a clear look at how grounded automation will be tested, measured, and governed before wider adoption.

**Tagline:** *Evidence before automation begins.*

---
