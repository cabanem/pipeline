/* eslint-disable no-var */
"use strict";

/**
 * Schedule Monitor — refactored
 * - Observations (for trends) logged every run or on change
 * - Alerts (Chat) deduped by TTL and logged separately as Events
 * - Header validation + debounce + locking
 * - Daily trend roll-up to a compact sheet
 *
 * Author: Emily Cabaniss
 */

// ───────────────────────────────────────────────────────────────────────────
// Constants (adjust as needed)
// ───────────────────────────────────────────────────────────────────────────

/** 1-based column indices */
const COLUMN_INDEX = {
  SCHEDULE_NAME: 1,        // A
  MACHINE: 7,              // G
  IF_RAN_TODAY: 10,        // J
  LAST_EXPECTED_RUN: 11,   // L
  MAX_START: 13,           // N
  CURRENT_STATUS: 15,      // O
  CURRENT_DURATION: 17,    // Q
  MAX_RUN_HOURS: 19,       // S
  NOT_RAN_REASON: 21,      // U
  IS_CRITICAL: 25,         // Y
  TIME_DIFF: 26,           // Z
  NOW_DIFF: 27,            // AA
  CRITICAL_ITEM_ALERT: 28, // AB
  EXTENDED_EXECUTION_ALERT: 29 // AC
};

const CRITICAL_ALERT_VALUE = "NOT RAN ALERT";
const EXTENDED_EXECUTION_ALERT = "DURATION ALERT";

const SHEET_NAME = "Notifications Logic";
const CHAT_WEBHOOK_URL = PropertiesService.getScriptProperties().getProperty("CHAT_WEBHOOK_URL");
const FAILURE_EMAIL_RECIPIENT = PropertiesService.getScriptProperties().getProperty("FAILURE_EMAIL_RECIPIENT");

// debounce + dedupe
const DEBOUNCE_MINUTES = 1;
const LAST_PROCESSED_PROPERTY = "lastProcessedTime";
const ALERT_TTL_MINUTES = 60;

// logging sheets
const LOG_SHEET_EVENTS = "EventLogs";     // audited alerts/actions
const LOG_SHEET_OBS = "Observations";     // raw state snapshots
const LOG_SHEET_TRENDS = "TrendMetrics";  // daily roll-up

// observations mode: 'changes' (default) or 'all'
const OBS_LOG_MODE = (PropertiesService.getScriptProperties().getProperty("OBS_LOG_MODE") || "changes").toLowerCase();


// ───────────────────────────────────────────────────────────────────────────
// Lightweight “types” via JSDoc
// ───────────────────────────────────────────────────────────────────────────
/**
 * @typedef {{
 *  scheduleName:string,
 *  machineName:string,
 *  ifRanToday:string,
 *  lastExpectedRun:(string|Date|null),
 *  currentDuration:number,
 *  maxRunHoursExpected:number,
 *  currentStatus:string,
 *  overdueMinutes:(number|null),
 *  notRanReason:string
 * }} RowData
 */

// ───────────────────────────────────────────────────────────────────────────
// Service classes (encapsulate I/O)
// ───────────────────────────────────────────────────────────────────────────

class SheetsRepo {
  /**
   * @param {GoogleAppsScript.Spreadsheet.Spreadsheet} ss
   * @param {string} tz
   */
  constructor(ss, tz) { this.ss = ss; this.tz = tz; }

  ensureSheet(name, header) {
    let sh = this.ss.getSheetByName(name);
    if (!sh) { sh = this.ss.insertSheet(name); sh.appendRow(header); }
    return sh;
  }

  appendRow(name, row) {
    const sh = this.ss.getSheetByName(name);
    sh.appendRow(row);
  }

  readMatrix(name) {
    const sh = this.ss.getSheetByName(name);
    return sh.getDataRange().getValues();
  }

  timestampISO(d = new Date()) {
    return Utilities.formatDate(d, this.tz, "yyyy-MM-dd'T'HH:mm:ssXXX");
  }
}

class ChatNotifier {
  constructor(webhookUrl) { this.url = webhookUrl; }
  /** @param {{text?:string, cardsV2?:any[]}} payload */
  send(payload) {
    if (!this.url) { Logger.log("CHAT_WEBHOOK_URL not set"); return; }
    const res = UrlFetchApp.fetch(this.url, {
      method: "post",
      contentType: "application/json",
      payload: JSON.stringify(payload),
      muteHttpExceptions: true
    });
    const code = res.getResponseCode();
    if (code >= 400) Logger.log(`Chat error ${code}: ${res.getContentText()}`);
  }
}

class AlertDedupeCache {
  constructor(props, ttlMinutes) { this.props = props; this.ttl = ttlMinutes * 60 * 1000; }
  shouldFire(schedule, machine, type) {
    const key = `alert::${type}::${schedule}::${machine}`;
    const now = Date.now();
    const last = Number(this.props.getProperty(key) || 0);
    if (now - last < this.ttl) return false;
    this.props.setProperty(key, String(now));
    return true;
  }
}

class ObservationsLogger {
  constructor(repo, sheetName) {
    this.repo = repo; this.sheet = sheetName;
    this.header = [
      "TimestampISO","RowIndex","Schedule","Machine","IfRanToday",
      "LastExpectedRunISO","OverdueMinutes","CurrentStatus",
      "CurrentDurationMin","MaxRunHours","CriticalFlag","ExtendedFlag"
    ];
    repo.ensureSheet(sheetName, this.header);
  }
  /** @param {number} rowIndex @param {RowData} d @param {{critical:boolean, extended:boolean}} f */
  append(rowIndex, d, f) {
    const ts = this.repo.timestampISO();
    const iso = d.lastExpectedRun ? new Date(d.lastExpectedRun).toISOString() : "";
    this.repo.appendRow(this.sheet, [
      ts, rowIndex, d.scheduleName, d.machineName, d.ifRanToday,
      iso, d.overdueMinutes ?? "", d.currentStatus, d.currentDuration,
      d.maxRunHoursExpected, f.critical ? 1 : 0, f.extended ? 1 : 0
    ]);
  }
}

class EventsLogger {
  constructor(repo, sheetName) {
    this.repo = repo; this.sheet = sheetName;
    this.header = ["TimestampISO","EventType","Schedule","Machine","DetailsJSON"];
    repo.ensureSheet(sheetName, this.header);
  }
  log(eventType, schedule, machine, details) {
    this.repo.appendRow(this.sheet, [
      this.repo.timestampISO(),
      eventType, schedule, machine,
      (typeof details === "string") ? details : JSON.stringify(details)
    ]);
  }
}

class TrendAggregator {
  constructor(repo, obsSheet, outSheet) { this.repo = repo; this.obsSheet = obsSheet; this.outSheet = outSheet; }
  buildDaily() {
    const values = this.repo.readMatrix(this.obsSheet);
    if (!values || values.length < 2) return;
    const H = {}; values[0].forEach((h,i)=>H[h]=i);
    const rows = values.slice(1);

    const buckets = {};
    for (const r of rows) {
      const ts = r[H["TimestampISO"]] || "";
      const day = ts.slice(0,10);
      const schedule = r[H["Schedule"]];
      const machine = r[H["Machine"]];
      const overdue = Number(r[H["OverdueMinutes"]] || 0);
      const curDur = Number(r[H["CurrentDurationMin"]] || 0);
      const critical = Number(r[H["CriticalFlag"]] || 0) === 1;
      const extended = Number(r[H["ExtendedFlag"]] || 0) === 1;

      const key = `${day}|${schedule}|${machine}`;
      const b = buckets[key] || (buckets[key] = {
        day, schedule, machine, samples:0, missed:0, extended:0,
        overdueSum:0, arr:[], durationMax:0
      });

      b.samples++;
      if (critical) b.missed++;
      if (extended) b.extended++;
      if (Number.isFinite(overdue)) { b.overdueSum += overdue; b.arr.push(overdue); }
      if (Number.isFinite(curDur) && curDur > b.durationMax) b.durationMax = curDur;
    }

    const out = [["Day","Schedule","Machine","Samples","Missed","Extended","AvgOverdueMin","P95OverdueMin","MaxDurationMin"]];
    Object.values(buckets)
      .sort((a,b) => (a.day+a.schedule+a.machine).localeCompare(b.day+b.schedule+b.machine))
      .forEach(b => {
        const avg = b.samples ? Math.round((b.overdueSum / b.samples) * 100) / 100 : "";
        let p95 = "";
        if (b.arr.length) {
          b.arr.sort((x,y)=>x-y);
          p95 = b.arr[Math.floor(0.95 * (b.arr.length - 1))];
        }
        out.push([b.day, b.schedule, b.machine, b.samples, b.missed, b.extended, avg, p95, b.durationMax]);
      });

    const sh = this.repo.ss.getSheetByName(this.outSheet) || this.repo.ss.insertSheet(this.outSheet);
    sh.clearContents();
    sh.getRange(1,1,out.length,out[0].length).setValues(out);
  }
}


// ───────────────────────────────────────────────────────────────────────────
// Pure helpers (no I/O)
// ───────────────────────────────────────────────────────────────────────────

function computeOverdueMinutes(raw) {
  if (raw === null || raw === undefined || raw === "") return null;
  let n = Number(raw);
  if (Number.isNaN(n)) return null;
  if (n < 1) n *= 1440; // treat values <1 as days
  return n;
}

function formatDuration(mins) {
  if (!Number.isFinite(mins)) return "N/A";
  const h = Math.floor(mins / 60);
  const m = Math.floor(mins % 60);
  return h ? `${h} hours ${m} minutes` : `${m} minutes`;
}

function getDurationComparisonMessage(currentMin, maxHours) {
  if (!currentMin || !maxHours) return "";
  const over = currentMin - maxHours * 60;
  return (over > 0) ? `Execution has exceeded the maximum duration by ${formatDuration(over)}.` : "";
}

function spreadsheetLocalDateTime(dateInput) {
  if (!dateInput) return "N/A";
  try {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const tz = ss.getSpreadsheetTimeZone();
    const d = new Date(dateInput);
    if (isNaN(d.getTime())) return "Invalid date";
    return Utilities.formatDate(d, tz, "MM/dd/yyyy - hh:mm a (z)");
  } catch (e) {
    Logger.log(`Error formatting date: ${e.message}`);
    return "Invalid date";
  }
}

/** state-change signature for observation throttling */
function stateSignature_(d) {
  const sig = [
    d.ifRanToday, d.currentStatus, d.currentDuration,
    d.maxRunHoursExpected, d.overdueMinutes, d.notRanReason
  ].join("|");
  return Utilities.base64Encode(Utilities.computeDigest(Utilities.DigestAlgorithm.MD5, sig));
}

function shouldLogObservation_(rowData) {
  if (OBS_LOG_MODE === "all") return true;
  const key = `obs_sig::${rowData.scheduleName}::${rowData.machineName}`;
  const props = PropertiesService.getScriptProperties();
  const lastSig = props.getProperty(key);
  const currentSig = stateSignature_(rowData);
  if (lastSig !== currentSig) {
    props.setProperty(key, currentSig);
    return true;
  }
  return false;
}

// ───────────────────────────────────────────────────────────────────────────
// Data mapping + validation
// ───────────────────────────────────────────────────────────────────────────

/** @param {Array<any>} row * @returns {RowData} */
function getRowData(row) {
  const num = (v) => {
    const n = Number(v);
    return Number.isFinite(n) ? n : 0;
  };
  return {
    scheduleName: String(row[COLUMN_INDEX.SCHEDULE_NAME - 1] || "").trim() || "N/A",
    machineName:  String(row[COLUMN_INDEX.MACHINE - 1] || "").trim() || "N/A",
    ifRanToday:   String(row[COLUMN_INDEX.IF_RAN_TODAY - 1] || "").trim() || "N/A",
    lastExpectedRun: row[COLUMN_INDEX.LAST_EXPECTED_RUN - 1] || null,
    currentDuration: num(row[COLUMN_INDEX.CURRENT_DURATION - 1]),
    maxRunHoursExpected: num(row[COLUMN_INDEX.MAX_RUN_HOURS - 1]),
    currentStatus: String(row[COLUMN_INDEX.CURRENT_STATUS - 1] || "").trim() || "N/A",
    overdueMinutes: computeOverdueMinutes(row[COLUMN_INDEX.NOW_DIFF - 1]),
    notRanReason: String(row[COLUMN_INDEX.NOT_RAN_REASON - 1] || "").trim()
  };
}

function validateSheetStructure(sheet) {
  const actual = sheet.getRange(1, 1, 1, sheet.getLastColumn()).getValues()[0]
    .map(h => String(h).trim().toLowerCase());

  const required = {
    [COLUMN_INDEX.SCHEDULE_NAME]: "schedule name",
    [COLUMN_INDEX.MACHINE]: "machine",
    [COLUMN_INDEX.LAST_EXPECTED_RUN]: "last expected run day / time",
    [COLUMN_INDEX.MAX_START]: "max start",
    [COLUMN_INDEX.CRITICAL_ITEM_ALERT]: "critical item alert",
    [COLUMN_INDEX.EXTENDED_EXECUTION_ALERT]: "extended execution alert"
  };

  const mismatches = [];
  Object.entries(required).forEach(([idx, expected]) => {
    const actualHeader = actual[idx - 1] || "missing";
    if (actualHeader !== expected) {
      mismatches.push(`Column ${idx}: Expected "${expected}", Found "${actualHeader}"`);
    }
  });

  if (mismatches.length) {
    throw new Error(`Header validation failed:\n${mismatches.join("\n")}`);
  }
}

// ───────────────────────────────────────────────────────────────────────────
// Message payload builders (Chat)
// ───────────────────────────────────────────────────────────────────────────

function formatNotificationText(d) {
  const message = [
    "Missed Schedule Alert:",
    `Schedule Name: ${d.scheduleName}`,
    `Machine Name: ${d.machineName}`,
    `Missed Schedule Time: ${spreadsheetLocalDateTime(d.lastExpectedRun)}`,
    `Minutes Overdue: ${d.overdueMinutes ?? "N/A"}`
  ].join("\n");
  return { text: message };
}

function formatExtendedExecutionText(d) {
  const note = getDurationComparisonMessage(d.currentDuration, d.maxRunHoursExpected);
  const message = [
    "Extended Execution Alert:",
    `Schedule Name: ${d.scheduleName}`,
    `Machine Name: ${d.machineName}`,
    `Expected Start: ${spreadsheetLocalDateTime(d.lastExpectedRun)}`,
    `Current Status: ${d.currentStatus}`,
    `Current Duration: ${formatDuration(d.currentDuration)}`,
    `Expected Duration: ${d.maxRunHoursExpected} hours`,
    note ? `Note: ${note}` : null
  ].filter(Boolean).join("\n");
  return { text: message };
}

// ───────────────────────────────────────────────────────────────────────────
// Public API (kept names so existing triggers keep working)
// ───────────────────────────────────────────────────────────────────────────

/**
 * Trigger entry point.
 * - lock + debounce
 * - header validation
 * - per-row observations, alerts (deduped), event logs
 */
function runScheduleMonitor() {
  const lock = LockService.getScriptLock();
  try { lock.waitLock(30000); } catch (_) { Logger.log("Lock busy"); return; }

  const props = PropertiesService.getScriptProperties();
  const last = Number(props.getProperty(LAST_PROCESSED_PROPERTY) || 0);
  const now = Date.now();
  if (now - last < DEBOUNCE_MINUTES * 60 * 1000) { lock.releaseLock(); return; }

  try {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const tz = ss.getSpreadsheetTimeZone();
    const repo = new SheetsRepo(ss, tz);
    const events = new EventsLogger(repo, LOG_SHEET_EVENTS);
    const obs = new ObservationsLogger(repo, LOG_SHEET_OBS);
    const chat = new ChatNotifier(CHAT_WEBHOOK_URL);
    const dedupe = new AlertDedupeCache(props, ALERT_TTL_MINUTES);

    const sheet = ss.getSheetByName(SHEET_NAME);
    if (!sheet) {
      const msg = `Sheet "${SHEET_NAME}" not found.`;
      Logger.log(msg);
      events.log("Error", "N/A", "N/A", msg);
      return;
    }

    try { validateSheetStructure(sheet); }
    catch (e) {
      Logger.log(e.message);
      events.log("Error", "N/A", "N/A", e.message);
      sendFailureNotification(e);
      sendFailureEmail(e);
      return;
    }

    const data = sheet.getDataRange().getValues();
    for (let i = 1; i < data.length; i++) {
      const row = data[i];
      const rowData = getRowData(row);

      const criticalCell = row[COLUMN_INDEX.CRITICAL_ITEM_ALERT - 1];
      const extendedCell = row[COLUMN_INDEX.EXTENDED_EXECUTION_ALERT - 1];
      const flags = {
        critical: criticalCell === CRITICAL_ALERT_VALUE,
        extended: extendedCell === EXTENDED_EXECUTION_ALERT
      };

      // Observations (for trends) — on change or every run
      if (shouldLogObservation_(rowData)) {
        obs.append(i + 1, rowData, flags);
      }

      // Alerts (deduped)
      if (flags.critical && rowData.overdueMinutes !== null && rowData.overdueMinutes >= 15 &&
          dedupe.shouldFire(rowData.scheduleName, rowData.machineName, "missed")) {
        chat.send(formatNotificationText(rowData));
        events.log("MissedScheduleNotification", rowData.scheduleName, rowData.machineName,
          { rowIndex: i + 1, reason: "CRITICAL_ALERT_VALUE" });
      }

      if (flags.extended &&
          dedupe.shouldFire(rowData.scheduleName, rowData.machineName, "extended")) {
        chat.send(formatExtendedExecutionText(rowData));
        events.log("ExtendedExecutionNotification", rowData.scheduleName, rowData.machineName,
          {
            rowIndex: i + 1,
            durationMinutes: rowData.currentDuration,
            maxRunHours: rowData.maxRunHoursExpected,
            reason: "DURATION ALERT"
          });
      }
    }

    Logger.log("Business logic execution completed.");
    events.log("Info", "N/A", "N/A", "Business logic execution completed successfully.");

    // set debounce marker at the end of a successful run
    props.setProperty(LAST_PROCESSED_PROPERTY, String(Date.now()));
  } catch (error) {
    Logger.log(`Error during time-triggered execution: ${error.message}`);
    try { sendFailureNotification(error); } catch (_) {}
    try { sendFailureEmail(error); } catch (_) {}
  } finally {
    lock.releaseLock();
  }
}

/**
 * Daily trend roll-up. Create a time-based trigger for this (e.g., 02:05).
 */
function buildTrendMetrics() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const repo = new SheetsRepo(ss, ss.getSpreadsheetTimeZone());
  new TrendAggregator(repo, LOG_SHEET_OBS, LOG_SHEET_TRENDS).buildDaily();
}

// ───────────────────────────────────────────────────────────────────────────
// Compatibility wrappers (keep names stable for any external callers)
// ───────────────────────────────────────────────────────────────────────────

/** Event log wrapper retained for compatibility (uses EventsLogger) */
function logEvent(eventType, scheduleName, machineName, details) {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const repo = new SheetsRepo(ss, ss.getSpreadsheetTimeZone());
  const events = new EventsLogger(repo, LOG_SHEET_EVENTS);
  events.log(eventType, scheduleName, machineName, details);
}

/** Old signature wrapper — delegates to ChatNotifier */
function sendGoogleChatNotification(payload) {
  new ChatNotifier(CHAT_WEBHOOK_URL).send(payload);
}

// ───────────────────────────────────────────────────────────────────────────
// Failure notifications (unchanged behavior, minor label fixes)
// ───────────────────────────────────────────────────────────────────────────

function sendFailureNotification(error) {
  if (!CHAT_WEBHOOK_URL) { Logger.log("Cannot send failure notification: CHAT_WEBHOOK_URL is not set."); return; }
  const projectName = DriveApp.getFileById(ScriptApp.getScriptId()).getName();
  const scriptId = ScriptApp.getScriptId();
  const text =
`*Script Execution Failed*

An unhandled exception occurred in the ${projectName} script.

*Script ID:* \`${scriptId}\`
*Error Message:* \`${error.message}\`

*Stack Trace:*
\`\`\`
${error.stack || "No stack trace available."}
\`\`\``;
  sendGoogleChatNotification({ text });
}

function sendFailureEmail(error) {
  if (!FAILURE_EMAIL_RECIPIENT) { Logger.log("Cannot send failure email: FAILURE_EMAIL_RECIPIENT is not set."); return; }
  const projectName = DriveApp.getFileById(ScriptApp.getScriptId()).getName();
  const scriptId = ScriptApp.getScriptId();
  const subject = "Error in Google Apps Script: Schedule Monitor";
  const htmlBody = `
    <p>An unhandled error occurred in the <b>Schedule Monitor</b> script.</p>
    <p><b>Project Name:</b> <code>${projectName}</code></p>
    <p><b>Script ID:</b> <code>${scriptId}</code></p>
    <p><b>Error Message:</b> ${error.message}</p>
    <p><b>Stack Trace:</b></p>
    <pre>${error.stack || "No stack trace available."}</pre>
  `;
  GmailApp.sendEmail(FAILURE_EMAIL_RECIPIENT, subject, "", { name: `${projectName} Script`, htmlBody });
}
