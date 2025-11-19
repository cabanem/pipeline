Nice, greenfield-ish systems project with just enough spreadsheet chaos to be fun. Let’s load you up so that first call actually sets you up, not just generates more tabs.

---

## 1. What you’re trying to get out of the intake call

You don’t have to say all this out loud, but these are your internal goals:

* Understand **why** they track contractor performance (business outcomes, not just “because we always have”).
* Map **how work actually flows** today (not just what’s on the spreadsheet).
* Nail down **what “good” looks like** for the future system (MVP + “in a perfect world”).
* Surface **landmines** early: sensitive notes, politics, legal/HR constraints, timezones, renewals, etc.

---

## 2. Simple agenda for a 60-min intake

You can roughly timebox like this:

1. **Context + goals (5–10 min)**

   * Why we’re here, what problem we’re solving, what success would look like.

2. **Current process + spreadsheet tour (15–20 min)**

   * Walk through the master tab and one real employee tab.
   * Have them narrate how they actually use it.

3. **What they wish they had (15–20 min)**

   * Reporting, alerts, workflows, automation.
   * Gaps and annoyances.

4. **Constraints, risks, and adoption (10–15 min)**

   * Who needs access, what’s sensitive, what’s non-negotiable.
   * Phasing: MVP vs later dreams.

---

## 3. Concrete questions you can ask

### A. Outcomes: why does this system exist at all?

You’re trying to zoom out from “we have tabs” to “we’re managing risk and performance.”

* “If this system were **working perfectly** a year from now, what would be different in your day-to-day?”
* “When you say ‘performance’ for contractors, what do you *actually* care about?

  * Quality of work?
  * Reliability/attendance?
  * Speed?
  * Communication?
  * Cultural fit?”
* “What are the **scary scenarios** this system should help you catch early? (e.g., underperformers, missed deadlines, missed renewals, legal/compliance risk)”
* “Who outside your team cares about this data? Leadership, HR, finance, vendor management?”

These answers define your *north star* and later what you prioritize.

---

### B. Current process: how do they really use the spreadsheet?

You already know the structure; dig into behavior:

* “Walk me through a typical **new contractor**:

  * Where do they first get added?
  * Who fills in the master tab fields?
  * When does their individual tab get created?”
* “How do 1:1 notes actually get used?

  * Do people refer back to them before reviews?
  * Or is it more ‘write once, never look again’?”
* “What triggers someone to **open** an employee’s tab?

  * Scheduled 1:1s?
  * Performance concerns?
  * Vendor review cycles?”
* “What parts of the spreadsheet do you **trust** vs **ignore**?

  * Any columns or tabs that are basically dead?”
* “Where does this process **break down** today?

  * People forget to update?
  * Conflicting info between tabs?
  * Hard to find someone?
  * No clear history?”

If you want to be slightly spicy:

> “If I deleted this entire spreadsheet tomorrow, what would you actually be upset about losing?”

That tells you the true critical fields.

---

### C. Data model: what are these fields *really* meant to represent?

You can use the existing sheet as your starter schema.

**Master tab fields:**

* “Master tab: which columns are **mandatory** vs ‘nice-to-have’?”
* “Is there anything important you track **outside** this sheet (HRIS, vendor portal, time tracking) that we should consider linking to?”
* “Is ‘group’ and ‘role’ a fixed list, or does it change constantly?”
* “Do you ever need **history** for master data? e.g., role changes, lead changes, group changes.”

**Per-employee tab fields:**

* “For the per-employee tabs, what decisions do you make using:

  * `notes`
  * `personal notes`
  * `reminders`
  * `metrics`?”
* “Are `personal notes` things we’d *never* want widely shared?

  * How sensitive is that: coaching notes, health issues, ‘vibes’, escalation history?”
* “What goes into `metrics` today?

  * Is it numeric KPIs, qualitative ratings, or a mix?
  * Are metrics consistent across employees, or does each lead freestyle?”

This gives you a clean path to a normalized data model later.

---

### D. Workflow: what’s the lifecycle of a contractor?

Think in phases: **onboard → active → flagged → offboarded/ended**.

* “At what points in a contractor’s lifecycle do you **need to log something**?

  * Day 1?
  * First 30/60/90 days?
  * After every 1:1?
  * When performance concerns arise?
  * When you decide to renew / not renew?”
* “When someone is **struggling**, how does that show up in the current system?

  * Is there a flag?
  * More notes?
  * Different meeting cadence?”
* “Do you have any **formal checkpoints** (e.g., at contract renewal time) where you wish you had better data?”
* “What are the **typical actions** you take based on this data?

  * Coaching plans?
  * Contract termination?
  * Changing teams/roles?
  * Vendor feedback?”

You’re fishing for triggers, statuses, and events that will later become workflow steps and states.

---

### E. Reporting: what questions do they want answered at a glance?

This is where you steer them from “rows” to “views.”

* “What are the **top 5 questions** you ask of this data that currently take too long to answer?”

  * “Who is at risk?”
  * “Who is up for renewal soon?”
  * “Who are our top performers in X region/role/vendor?”
  * “Which leads have too many direct reports?”
* “Do you have any **recurring meetings** where you manually prep data from this sheet?

  * 1:1s, leadership reviews, vendor check-ins?”
* “If you had a **single dashboard**, what would absolutely need to be on it?”

Good litmus test:

> “What’s the last screenshot of this sheet you dropped into a slide or email? Why that view?”

---

### F. Roles, access, and sensitive information

You’ve got `personal notes`, `reminders`, maybe sensitive stuff. Better to surface this early.

* “Who should be able to **see everything** vs only their own team?”
* “Should leads see each other’s `personal notes`? Or is that restricted?”
* “Do contractors *ever* see anything from this system? (Now or in the future?)”
* “Any legal or HR constraints we should be aware of around:

  * documenting performance,
  * documenting health/personal issues,
  * retaining notes for X years?”

This is the difference between “cute dashboard” and “please enjoy your regulatory problem.”

---

### G. Integrations & tech constraints (light-touch on intake call)

You don’t need to architect on call, just map reality:

* “What systems already know about these people?

  * HRIS?
  * Vendor / MSP system?
  * Time-tracking or ticketing (Jira, ServiceNow, etc.)?”
* “Do you have any **non-starters**?

  * ‘No new logins’
  * ‘Must stay in Google Workspace / Sheets’
  * ‘Cannot store X data outside our region’”
* “Are there IT/security folks we should pull in early for data and access questions?”

---

### H. MVP vs later phases

You want them to *prioritize*, not dump a wish list.

* “If we only shipped something small in **4–6 weeks**, what would be the most valuable thing it could do?”

  * Example prompts:

    * “Even just standardized 1:1 notes + reminders?”
    * “Or a clean dashboard of who’s at risk and who’s up for renewal?”
* “What belongs in **v2 or v3**?

  * Fancy analytics, automated nudges, integration with HRIS, vendor scorecards, etc.”

You’re drawing a line between “must have to replace your current sheet” and “nice future candy.”

---

## 4. A quick “opening spiel” you can adapt

Steal and tweak:

> “Today I want to understand how you’re actually managing contractor performance today – beyond what the spreadsheet *says* you do – and where it’s failing you.
> I’m especially interested in:
>
> * what decisions you need to make,
> * what information you trust or don’t trust in the current sheet,
> * and what a realistic ‘version 1’ of a better system would look like.
>
> By the end of the call, I’d like us to have:
>
> * a shared picture of your current process,
> * your top success criteria,
> * and a rough idea of what’s MVP vs ‘future nice-to-have’.”

Short, honest, and makes them feel like co-designers, not survey respondents.

---

## 5. One “fun” question to end on

Close with something slightly more human:

* “If this new system had a **superpower** your spreadsheet never had, what would it be?”
* “What’s one thing you’d *love* to stop doing manually?”

That often surfaces the thing they really care about but didn’t know how to frame.

---

From this intake, you’ll have enough to sketch: entities (contractor, lead, group), lifecycle states, events (1:1, concern raised, renewal), and the early shape of metrics and dashboards. That’s the backbone of something a lot more robust than “master tab + chaos.”
