# Categorization for HR Email Assistant

- Split into **hard filters** (binary), **soft signals** (scored), and **policy thresholds**. 
- Examples target an HR assistant that should only answer employee requests (e.g., PTO, benefits, payroll, address change, verification letters, policy clarifications).

## Rule set

### 1. Hard filters (drop early, no scoring)

**Sender & routing**

* `From:` domain not in `{company.com, subsidiary.co}` → **exclude**
* `From:` contains `no-reply`, `noreply`, `donotreply`, or `bounce` → **exclude**
* Mailing lists: has `List-Unsubscribe` header or `Precedence: bulk/list` → **exclude**
* Auto replies: `Auto-Submitted: auto-replied` or subject matches `(?i)\b(out of office|ooo|vacation auto-reply)\b` → **exclude**

**System noise**

* Calendar invites (`Content-Type: text/calendar` or Subject starts `Invitation:` / `Declined:`) → **exclude**
* Build/monitoring alerts (Subject contains `[ALERT]`, `Incident`, `PagerDuty`, `Opsgenie`) → **exclude**
* Social/marketing: body contains `unsubscribe` AND ≥2 of `newsletter|promotion|webinar|sale` → **exclude**

**Attachments you don’t want to parse automatically**

* Executables or archives: extensions `.{exe,bat,cmd,ps1,jar,apk,zip,rar}` with no clear HR intent → **exclude**

**Security & privacy**

* Suspected phish: SPF/DKIM/DMARC fails AND “reset password” links to non-corporate domain → **exclude + flag**
* Sensitive PII patterns (full SSN `\b\d{3}-\d{2}-\d{4}\b`, bank account numbers) without an explicit HR change request → **exclude + escalate**

**Thread hygiene**

* Pure FYI forwards: Subject starts with `Fwd:` AND body has no first-person request (“I need…”, “Can you…”) → **exclude**

---

### 2. Soft signals (score; keep if score ≥ threshold)

Give each signal a weight; sum them. Keep emails at or above `KEEP_THRESHOLD` (example: **≥ 6**). If score falls into a gray band (e.g., **4–5**), route to “needs triage.”

**Intent / speech-act**

* Contains a direct request pattern (weight **+4**):
  `(?i)\b(i need|please|can you|could you|would you|i’m requesting|i would like to)\b`
* Contains a question mark with HR keywords (weight **+2**)

**HR domain terms (any 2+ adds weight cumulatively; cap once per group)**

* PTO/Leave (weight **+3**): `pto|vacation|leave|sick|bereavement|parental|fmla|time off`
* Benefits (weight **+3**): `benefit(s)?|medical|dental|vision|401k|hsa|fsa|open enrollment|dependent`
* Payroll/Comp (weight **+3**): `payroll|paycheck|salary|wage|stub|withholding|garnish|overtime|bonus`
* Employment data (weight **+3**): `address change|name change|legal name|marital status|tax form|w-4|i-9`
* Verification/Letters (weight **+3**): `employment verification|proof of employment|letter of employment|tenancy|mortgage`
* Policy/Compliance (weight **+2**): `policy|handbook|code of conduct|holiday schedule|expense policy`

**Employee identity clues (light PII, safe to use)**

* Internal directory handle or email in body (weight **+1**)
* Employee ID pattern (non-sensitive form, e.g., `EID-\d{6}`) (weight **+2**)
* Mentions manager/team names from directory (weight **+1**)

**Structure & format**

* Subject looks like a request (weight **+2**):
  `(?i)\brequest|change|update|question|verification|enrollment\b`
* Attachments relevant to HR (weight **+2**): PDF/PNG/JPG of pay stub, W-4, benefits docs; cap at **+2** total

**Negative soft signals (subtract)**

* Marketing language (weight **−3**): `exclusive offer|webinar|sponsor|promo|discount`
* External vendor HR sales pitch (weight **−2**): “schedule a demo”, “pilot program”
* Multiple links to tracking domains (weight **−2**) with no HR terms

---

### 3. Policy thresholds

```json
{
  "KEEP_THRESHOLD": 6,
  "TRIAGE_BAND_MIN": 4,
  "TRIAGE_BAND_MAX": 5
}
```

* `score >= KEEP_THRESHOLD` → **HR-REQUEST**
* `TRIAGE_BAND_MIN <= score <= TRIAGE_BAND_MAX` → **REVIEW**
* Else → **IRRELEVANT**

---

### 4. Category guardrails (so the bot can actually reply)

Once an email survives filters, validate minimal fields per category before auto-reply:

**PTO / Leave request**

* Must have a date or range: detect `YYYY-MM-DD`, “Nov 12–15”, “next Friday” (resolve with org timezone).
* Must have type if policy requires it: `{vacation|sick|bereavement|parental}`.
* If ambiguous duration (“a few hours”), require follow-up template instead of auto-reply.

**Benefits question**

* Must contain at least one benefit keyword and one intent cue (“how do I…”, “am I eligible”).
* If the question references plan year or enrollment window, extract year; otherwise add a clarifying line in the draft.

**Payroll / Address change**

* Must not include full SSN/bank numbers. If present → **escalate** and scrub in logs.
* Require one of: “address”, “name change”, “withholding”, “pay stub”.

**Employment verification**

* Detect requester party (landlord/bank) and target format (“PDF letter”, “to this email”).
* If third-party external email present and domain not on allowlist → **triage**.

---

### 5. Concrete patterns (regex-ish)

* Direct request (English):

  * `(?i)\b(i (need|would like|want) to|please|can you|could you|i'm requesting)\b`
* PTO date spans:

  * ISO: `\b20\d{2}-\d{2}-\d{2}\b`
  * Natural: `(?i)\b(jan(?:uary)?|feb(?:ruary)?|...|dec(?:ember)?) \d{1,2}(\s*[-–]\s*\d{1,2})?\b`
* Address change:

  * `(?i)\b(address|move|relocat(e|ing)|new address)\b`
* Verification:

  * `(?i)\b(employment verification|proof of employment|verification letter)\b`
* Marketing unsubscribe:

  * `(?i)\bunsubscribe\b`
* No-reply:

  * `(?i)\bno[-\s]?reply\b`

(Keep patterns short and maintainable; avoid overfitting.)

---

### 6. Example scoring JSON (team-tunable)

```json
{
  "hard_exclude": {
    "from_contains": ["no-reply", "noreply", "donotreply", "bounce"],
    "headers_present": ["List-Unsubscribe", "Auto-Submitted:auto-replied"],
    "content_types": ["text/calendar"],
    "subject_regex": ["(?i)out of office|ooo|vacation auto-reply"],
    "security": { "spf_dkim_dmarc_fail": true }
  },
  "soft_signals": [
    { "name": "direct_request", "regex": "(?i)\\b(i need|please|can you|could you|i'm requesting)\\b", "weight": 4 },
    { "name": "hr_pto", "regex": "(?i)\\bpto|vacation|leave|sick|fmla\\b", "weight": 3 },
    { "name": "hr_benefits", "regex": "(?i)\\bbenefits?|medical|dental|401k|hsa|fsa|enrollment\\b", "weight": 3 },
    { "name": "hr_payroll", "regex": "(?i)\\bpayroll|paycheck|withholding|overtime|bonus\\b", "weight": 3 },
    { "name": "hr_employment_data", "regex": "(?i)\\baddress change|name change|w-4|i-9\\b", "weight": 3 },
    { "name": "verification", "regex": "(?i)\\bemployment verification|verification letter|proof of employment\\b", "weight": 3 },
    { "name": "subject_request", "field": "subject", "regex": "(?i)\\brequest|change|update|question|verification|enrollment\\b", "weight": 2 },
    { "name": "attachment_hr", "attachment_ext_in": ["pdf","png","jpg","jpeg"], "weight": 2 },
    { "name": "employee_id", "regex": "\\bEID-\\d{6}\\b", "weight": 2 },
    { "name": "marketing_language", "regex": "(?i)\\bwebinar|promotion|exclusive offer|discount\\b", "weight": -3 },
    { "name": "vendor_pitch", "regex": "(?i)\\bschedule a demo|pilot program\\b", "weight": -2 }
  ],
  "thresholds": { "keep": 6, "triage_min": 4, "triage_max": 5 }
}
```

---

### 7. Sample outcomes

* **Keep (auto-reply candidate):**
  Subject: “Address change request”
  Body: “Hi HR, I moved to 42 Cedar Ave on Nov 12. Can you update my records?”
  Signals: direct_request(+4) + employment_data(+3) + subject_request(+2) → **9** → HR-REQUEST
  Category guardrail passes (address present) → draft update steps.

* **Triage (needs review):**
  Subject: “Question about leave”
  Body: “How many sick days do I have left?” (no dates, no policy mention)
  Signals: hr_pto(+3) + question(+2 assumed) → **5** → REVIEW → send clarifying template.

* **Exclude:**
  Subject: “Webinar: Future of Benefits Platforms — Exclusive Offer” with unsubscribe link
  Hard filter triggers (`List-Unsubscribe`) → **IRRELEVANT**

---

### 8. Wiring it into your action

* Add a lightweight **pre-filter step** in `gen_categorize_email.execute` before building embeddings:

  * Run **hard filters** → short-circuit with `{ chosen: 'Irrelevant' }` or skip classification.
  * Else compute **soft score**; if below triage → same.
* Keep your current category set and let the flow reach `embedding/hybrid/generative` only when **HR-REQUEST**.

---

### 9. Observability (so you can tune fast)

Log, per email:

* `pre_filter: { hard_exclusion_reason, soft_score, matched_signals[] }`
* Final decision: `{ decision: HR-REQUEST|REVIEW|IRRELEVANT }`
* Downstream classification `{ chosen, confidence }`

Track weekly:

* Acceptance rate (kept / total), triage rate, false positives/negatives from reviewer feedback.

---
