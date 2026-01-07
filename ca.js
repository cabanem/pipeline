/* eslint-disable no-var */
"use strict";

/**
 * @file Schedule Monitor v3.1 (Robust Simplicity + Optimized)
 * @description
 * Monitors automation schedules in Google Sheets for missed or extended runs.
 * * ARCHITECTURE OVERVIEW:
 * 1. Batch Processing: Reads all data once, processes in memory, writes results in bulk.
 * 2. Fail-Safe: Uses a `try...finally` block to ensure logs are saved even if the script crashes.
 * 3. Validation: Checks headers before processing to prevent reading misaligned columns.
 * 4. Optimization: Uses in-memory caching to reduce PropertyStore API calls by ~99%.
 * * @author Emily Cabaniss
 * @since Jan 2026
 */

// ===================================================================================
// 1. CONFIGURATION (Single Source of Truth)
// ===================================================================================

/**
 * Main configuration object.
 * Adjusting values here propagates through the entire system.
 * @constant
 */
const CONFIG = {
  /** Core system settings including sheet names and timeouts */
  CORE: {
    SHEET_NAME: "Notifications Logic", // The source tab to monitor
    DEBOUNCE_MINUTES: 1,               // Minimum cooldown between execution runs
    LAST_PROCESSED_PROP: "lastProcessedTime", // Key for storing last run timestamp
    TIMEZONE: "America/New_York"       // Fallback timezone
  },

  /** * Column Mappings (1-based indices to match SpreadsheetApp convention).
   * Used by getRowData to map array indices to named properties.
   */
  COLUMNS: {
    SCHEDULE_NAME: 1,            // Column A
    MACHINE: 7,                  // Column G
    IF_RAN_TODAY: 10,            // Column J
    LAST_EXPECTED_RUN: 11,       // Column L
    CURRENT_STATUS: 15,          // Column O
    CURRENT_DURATION: 17,        // Column Q
    MAX_RUN_HOURS: 19,           // Column S
    NOT_RAN_REASON: 21,          // Column U
    NOW_DIFF: 27,                // Column AA (Minutes Overdue)
    CRITICAL_ALERT: 28,          // Column AB
    EXTENDED_ALERT: 29           // Column AC
  },

  /**
   * Validation Rules: Map of { ColumnIndex: ["Allowed Header Name"] }.
   * The script will abort if these specific columns do not match the sheet.
   */
  EXPECTED_HEADERS: {
    1: ["Schedule Name", "Schedule"],
    7: ["Machine", "Server"],
    28: ["Critical Item Alert"],
    29: ["Extended Execution Alert"]
  },

  /** Alert Logic Configuration */
  ALERTS: {
    TTL_MINUTES: 60,             // Alert deduplication window (don't spam every minute)
    MIN_OVERDUE_MINUTES: 15,     // Grace period before alerting on missed schedule
    CRITICAL_VALUE: "NOT RAN ALERT",
    EXTENDED_VALUE: "DURATION ALERT"
  },

  /** Logging and Retention Configuration */
  LOGGING: {
    MODE: "changes",             // 'changes' = log only when status changes; 'all' = log every run
    RETENTION_DAYS: 60,          // Auto-delete rows older than this
    SHEETS: {
      EVENTS: "EventLogs",       // System audits (alerts sent, errors)
      OBSERVATIONS: "Observations", // Snapshot of schedule states
      TRENDS: "TrendMetrics"     // Daily aggregated stats
    }
  },

  /** External Service Integration Keys (loaded from Script Properties) */
  SERVICES: {
    CHAT_WEBHOOK_URL: PropertiesService.getScriptProperties().getProperty("CHAT_WEBHOOK_URL"),
    FAILURE_EMAIL: PropertiesService.getScriptProperties().getProperty("FAILURE_EMAIL_RECIPIENT")
  }
};

// Freeze to prevent accidental modification during runtime
Object.freeze(CONFIG);


// ===================================================================================
// 2. CORE LOGIC (The Engine)
// ===================================================================================

/**
 * PRIMARY ENTRY POINT.
 * Orchestrates the entire monitoring workflow.
 * * Logic Flow:
 * 1. Lock Check (Concurrency protection)
 * 2. Property Load (Memory Optimization)
 * 3. Header Validation (Data Integrity)
 * 4. Data Processing Loop (Business Logic)
 * 5. Flush (Data Persistence via `finally` block)
 * * @returns {void}
 */
function runScheduleMonitor() {
  const lock = LockService.getScriptLock();
  // 1. Concurrency Check: Prevent two executions from running effectively at once
  try { lock.waitLock(10000); } catch (e) { console.warn("Script is locked/busy."); return; }

  const props = PropertiesService.getScriptProperties();
  
  // OPTIMIZATION 1: Load ALL properties into memory once.
  // Google's PropertiesService is slow. Reading once reduces API calls from ~1000 to 1.
  const allProps = props.getProperties();
  const lastRun = Number(allProps[CONFIG.CORE.LAST_PROCESSED_PROP] || 0);
  const now = Date.now();

  // 2. Debounce Check: Ensure we respect the minimum interval
  if (now - lastRun < CONFIG.CORE.DEBOUNCE_MINUTES * 60 * 1000) { 
    lock.releaseLock(); 
    return; 
  }

  // Initialize Services
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const repo = new SheetsRepo(ss);
  const events = new EventsLogger(repo, CONFIG.LOGGING.SHEETS.EVENTS);
  const obs = new ObservationsLogger(repo, CONFIG.LOGGING.SHEETS.OBSERVATIONS);
  const notifier = new NotificationManager(CONFIG.SERVICES.CHAT_WEBHOOK_URL);
  
  // Initialize caches with the pre-loaded 'allProps' memory object
  const dedupe = new AlertDedupeCache(props, CONFIG.ALERTS.TTL_MINUTES, allProps);
  const stateCache = new ObservationStateCache(props, allProps);

  try {
    // 3. Validation Phase (Fail Fast)
    // We check headers BEFORE reading data. If columns shifted, we crash now rather than sending bad alerts.
    const validation = validateHeaders(repo, CONFIG.CORE.SHEET_NAME, CONFIG.EXPECTED_HEADERS);
    if (!validation.valid) throw new Error(`Header Validation Failed: ${validation.errors.join(", ")}`);

    // OPTIMIZATION 2: Narrow Read
    // Only reads columns A through AC (or whatever is max in CONFIG). Skips columns AD -> ZZ.
    const data = repo.readData(CONFIG.CORE.SHEET_NAME);
    if (data.length === 0) return;

    // 4. Processing Loop
    // Start at i=1 to skip the header row
    for (let i = 1; i < data.length; i++) {
      try {
        const row = data[i];
        
        // Skip completely empty rows or rows missing a Schedule Name (Performance)
        if (!row[0] && !row[6]) continue; 

        const rowData = getRowData(row);
        const flags = getAlertFlags(row);

        // A. Log Observations
        // Checks in-memory cache to see if state changed since last run
        if (stateCache.checkChange(rowData)) {
          obs.append(i + 1, rowData, flags);
        }

        // B. Critical Alerts (Missed Schedule)
        // Logic: Must be flagged critical AND be overdue by threshold (15 mins)
        if (flags.critical && (rowData.overdueMinutes || 0) >= CONFIG.ALERTS.MIN_OVERDUE_MINUTES) {
          if (dedupe.shouldFire(rowData.scheduleName, rowData.machineName, "missed")) {
            notifier.sendMissedSchedule(rowData);
            events.log("MissedScheduleNotification", rowData.scheduleName, rowData.machineName, "Sent Alert");
          }
        }

        // C. Extended Execution Alerts
        if (flags.extended) {
          if (dedupe.shouldFire(rowData.scheduleName, rowData.machineName, "extended")) {
            notifier.sendExtendedExecution(rowData);
            events.log("ExtendedExecutionNotification", rowData.scheduleName, rowData.machineName, "Sent Alert");
          }
        }

      } catch (rowError) {
        // Row-level error handling: Log it, but don't kill the whole batch.
        console.error(`Row ${i+1} Error: ${rowError.message}`);
        events.log("RowProcessingError", "Unknown", "Unknown", `Row ${i+1}: ${rowError.message}`);
      }
    }

    // Mark run as successful in memory (will be flushed in 'finally')
    props.setProperty(CONFIG.CORE.LAST_PROCESSED_PROP, String(Date.now()));

  } catch (fatalError) {
    // Script-level error handling: Notify dev team
    console.error("Fatal Script Error", fatalError);
    notifier.sendSystemFailure(fatalError);
    events.log("FatalError", "System", "System", fatalError.message);

  } finally {
    // 5. The Safety Net (Flush)
    // This executes whether the script succeeded OR crashed.
    // It guarantees that whatever logs/properties we queued get written to disk.
    obs.flush();       // Write observation rows
    events.flush();    // Write event rows
    dedupe.flush();    // Write alert dedupe timestamps
    stateCache.flush(); // Write observation state hashes
    lock.releaseLock();
  }
}

/**
 * Maintenance Trigger.
 * Should be scheduled to run once daily (e.g., 2:00 AM).
 * Handles log rotation/deletion and trend aggregation.
 */
function runDailyMaintenance() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const repo = new SheetsRepo(ss);
  
  // 1. Cleanup old logs
  const cleaner = new LogRetentionManager(repo, CONFIG.LOGGING.RETENTION_DAYS);
  cleaner.cleanSheet(CONFIG.LOGGING.SHEETS.OBSERVATIONS);
  cleaner.cleanSheet(CONFIG.LOGGING.SHEETS.EVENTS);
  
  // 2. Build Trend Report
  const trend = new TrendAggregator(repo, CONFIG.LOGGING.SHEETS.OBSERVATIONS, CONFIG.LOGGING.SHEETS.TRENDS);
  trend.buildDaily();
}


// ===================================================================================
// 3. SERVICE CLASSES
// ===================================================================================

/**
 * Abstraction layer for Google Sheets operations.
 * Separates data access from business logic.
 */
class SheetsRepo {
  /**
   * @param {GoogleAppsScript.Spreadsheet.Spreadsheet} ss 
   */
  constructor(ss) {
    this.ss = ss;
    this.tz = ss.getSpreadsheetTimeZone() || CONFIG.CORE.TIMEZONE;
  }

  /**
   * Gets a sheet or creates it if missing. Appends header if empty.
   * @param {string} name - The sheet name to find or create.
   * @param {Array<string>} header - The header row to append if created.
   * @returns {GoogleAppsScript.Spreadsheet.Sheet}
   */
  ensureSheet(name, header) {
    let sh = this.ss.getSheetByName(name);
    if (!sh) { sh = this.ss.insertSheet(name); sh.appendRow(header); }
    else if (sh.getLastRow() === 0) { sh.appendRow(header); }
    return sh;
  }

  /**
   * Optimized read that only fetches up to the maximum column defined in CONFIG.
   * Prevents reading thousands of empty columns in large sheets.
   * @param {string} name - The sheet name to read.
   * @returns {Array<Array<any>>} The data values (2D array).
   */
  readData(name) {
    const sh = this.ss.getSheetByName(name);
    if (!sh) return [];
    
    // OPTIMIZATION: Only read necessary columns + small buffer
    const maxCol = Math.max(...Object.values(CONFIG.COLUMNS));
    const lastCol = Math.min(maxCol + 2, sh.getLastColumn()); 
    
    return sh.getRange(1, 1, sh.getLastRow(), lastCol).getValues();
  }

  /**
   * Formats a date object to ISO string using the sheet's timezone.
   * @param {Date} [d=new Date()] 
   * @returns {string} ISO-8601 formatted string.
   */
  timestampISO(d = new Date()) {
    return Utilities.formatDate(d, this.tz, "yyyy-MM-dd'T'HH:mm:ssXXX");
  }
}

/**
 * Validates that specific columns contain specific header text.
 * Used to ensure columns haven't been shifted/deleted by users.
 * * @param {SheetsRepo} repo - Data access repository.
 * @param {string} sheetName - The sheet to check.
 * @param {Object} rules - Map of {colIndex: [allowedNames]}.
 * @returns {{valid: boolean, errors: Array<string>}}
 */
function validateHeaders(repo, sheetName, rules) {
  const sheet = repo.ss.getSheetByName(sheetName);
  if (!sheet) return { valid: false, errors: [`Sheet "${sheetName}" missing`] };

  const actualHeaders = sheet.getRange(1, 1, 1, sheet.getLastColumn()).getValues()[0];
  const errors = [];

  Object.entries(rules).forEach(([colIndex, allowedNames]) => {
    const idx = Number(colIndex) - 1; // Convert to 0-based
    const actual = String(actualHeaders[idx] || "").trim().toLowerCase();
    
    // Check if actual header matches any of the allowed variants
    const match = allowedNames.some(name => actual === name.toLowerCase());
    
    if (!match) {
      errors.push(`Column ${colIndex}: Expected [${allowedNames.join(", ")}], found "${actualHeaders[idx]}"`);
    }
  });

  return { valid: errors.length === 0, errors };
}

/**
 * Buffers observations in memory and writes them in a single batch.
 * Reducing write operations significantly improves execution speed.
 */
class ObservationsLogger {
  /**
   * @param {SheetsRepo} repo 
   * @param {string} sheetName 
   */
  constructor(repo, sheetName) {
    this.repo = repo;
    this.sheetName = sheetName;
    this.queue = [];
    this.header = [
      "TimestampISO","RowIndex","Schedule","Machine","IfRanToday",
      "LastExpectedRunISO","OverdueMinutes","CurrentStatus",
      "CurrentDurationMin","MaxRunHours","CriticalFlag","ExtendedFlag"
    ];
  }

  /**
   * Adds an observation to the memory queue.
   * @param {number} rowIndex - Source row index.
   * @param {Object} d - Normalized row data.
   * @param {Object} f - Alert flags.
   */
  append(rowIndex, d, f) {
    const iso = (d.lastExpectedRun instanceof Date) ? d.lastExpectedRun.toISOString() : "";
    this.queue.push([
      this.repo.timestampISO(), rowIndex, d.scheduleName, d.machineName, d.ifRanToday,
      iso, d.overdueMinutes ?? "", d.currentStatus, d.currentDuration,
      d.maxRunHoursExpected, f.critical ? 1 : 0, f.extended ? 1 : 0
    ]);
  }

  /** Writes queued rows to the sheet in one operation. */
  flush() {
    if (this.queue.length === 0) return;
    try {
      const sheet = this.repo.ensureSheet(this.sheetName, this.header);
      sheet.getRange(sheet.getLastRow() + 1, 1, this.queue.length, this.queue[0].length).setValues(this.queue);
      this.queue = []; // Clear buffer
    } catch (e) {
      console.error("Failed to flush observations:", e);
    }
  }
}

/**
 * Buffers system events (Audit Trail).
 * Handles structured JSON details in the final column.
 */
class EventsLogger {
  /**
   * @param {SheetsRepo} repo 
   * @param {string} sheetName 
   */
  constructor(repo, sheetName) {
    this.repo = repo;
    this.sheetName = sheetName;
    this.queue = [];
    this.header = ["TimestampISO","EventType","Schedule","Machine","DetailsJSON"];
  }

  /**
   * Logs a system event to the queue.
   * @param {string} type - Event Type (e.g. "Error", "AlertSent").
   * @param {string} schedule 
   * @param {string} machine 
   * @param {string|Object} details - Will be JSON stringified if object.
   */
  log(type, schedule, machine, details) {
    this.queue.push([
      this.repo.timestampISO(), type, schedule, machine,
      (typeof details === "object") ? JSON.stringify(details) : String(details)
    ]);
  }

  /** Writes queued events to the sheet. */
  flush() {
    if (this.queue.length === 0) return;
    try {
      const sheet = this.repo.ensureSheet(this.sheetName, this.header);
      sheet.getRange(sheet.getLastRow() + 1, 1, this.queue.length, this.queue[0].length).setValues(this.queue);
      this.queue = [];
    } catch (e) {
      console.error("Failed to flush events:", e);
    }
  }
}

/** * OPTIMIZED: In-Memory Alert Deduplication.
 * Prevents spamming alerts by checking last sent timestamp.
 * Reads from memory, queues writes, flushes to PropertyStore at end.
 */
class AlertDedupeCache {
  /**
   * @param {GoogleAppsScript.Properties.Properties} service - PropertyStore service.
   * @param {number} ttlMinutes - How long to silence repeat alerts.
   * @param {Object} memoryCache - Pre-loaded properties object for instant reads.
   */
  constructor(service, ttlMinutes, memoryCache) {
    this.service = service;
    this.ttl = ttlMinutes * 60 * 1000;
    this.cache = memoryCache || {}; // Read from pre-loaded memory
    this.pending = {};              // Queue for batch writing
  }
  
  /**
   * Checks if an alert should fire based on TTL.
   * @returns {boolean} True if alert should send, False if suppressed.
   */
  shouldFire(schedule, machine, type) {
    const s = (schedule||"").replace(/[^a-zA-Z0-9]/g, "");
    const m = (machine||"").replace(/[^a-zA-Z0-9]/g, "");
    const key = `alert::${type}::${s}::${m}`;
    
    // Read from Memory (Instant)
    const last = Number(this.cache[key] || 0);
    const now = Date.now();
    
    if (now - last < this.ttl) return false;
    
    // Update Memory immediately & Queue for disk write
    this.cache[key] = String(now);
    this.pending[key] = String(now);
    return true;
  }

  /** Performs a batch write to Script Properties */
  flush() {
    if (Object.keys(this.pending).length > 0) {
      this.service.setProperties(this.pending);
    }
  }
}

/**
 * OPTIMIZED: In-Memory Observation State Tracking.
 * Detects if a row's data has changed since the last run to avoid logging duplicate states.
 */
class ObservationStateCache {
  /**
   * @param {GoogleAppsScript.Properties.Properties} service 
   * @param {Object} memoryCache 
   */
  constructor(service, memoryCache) {
    this.service = service;
    this.cache = memoryCache || {};
    this.pending = {};
  }

  /**
   * Compares current row data against stored hash.
   * @param {Object} d - Row data object.
   * @returns {boolean} True if state has changed (log it), False if identical.
   */
  checkChange(d) {
    if (CONFIG.LOGGING.MODE === "all") return true;
    
    const key = `obs_sig::${d.scheduleName}::${d.machineName}`;
    // Compute hash of current state
    const sig = Utilities.base64Encode(Utilities.computeDigest(Utilities.DigestAlgorithm.MD5, 
      [d.ifRanToday, d.currentStatus, d.overdueMinutes, d.notRanReason].join("|")
    ));

    // Compare with Memory (Instant)
    const last = this.cache[key];
    if (last !== sig) {
      // State changed: Update Memory & Queue
      this.cache[key] = sig;
      this.pending[key] = sig;
      return true;
    }
    return false;
  }

  /** Performs a batch write to Script Properties */
  flush() {
    if (Object.keys(this.pending).length > 0) {
      this.service.setProperties(this.pending);
    }
  }
}

/**
 * Handles communication with Google Chat via Webhook.
 */
class NotificationManager {
  constructor(webhookUrl) { this.url = webhookUrl; }

  sendMissedSchedule(d) {
    this.send({ text: `*Missed Schedule Alert*\nSchedule: ${d.scheduleName}\nMachine: ${d.machineName}\nOverdue: ${d.overdueMinutes} mins` });
  }

  sendExtendedExecution(d) {
    this.send({ text: `*Extended Execution Alert*\nSchedule: ${d.scheduleName}\nMachine: ${d.machineName}\nRunning: ${formatDuration(d.currentDuration)}\nLimit: ${d.maxRunHoursExpected}h` });
  }

  sendSystemFailure(error) {
    this.send({ text: `🚨 *Monitor Script Failed*\nError: ${error.message}` });
  }

  /**
   * Executes the HTTP request to the chat webhook.
   * @param {Object} payload - The chat message payload.
   */
  send(payload) {
    if (!this.url) return;
    try {
      UrlFetchApp.fetch(this.url, {
        method: "post",
        contentType: "application/json",
        payload: JSON.stringify(payload),
        muteHttpExceptions: true
      });
    } catch (e) {
      console.error("Chat send failed", e);
    }
  }
}

/**
 * Aggregates daily statistics from Observations.
 * Buckets data by Day+Schedule+Machine.
 */
class TrendAggregator {
  constructor(repo, obsSheet, outSheet) { this.repo = repo; this.obsSheet = obsSheet; this.outSheet = outSheet; }

  /** Reads raw observations and writes aggregated metrics to the Trend sheet. */
  buildDaily() {
    const sheet = this.repo.ss.getSheetByName(this.obsSheet);
    if (!sheet || sheet.getLastRow() < 2) return;
    
    // Read all observations
    const data = sheet.getDataRange().getValues();
    const headers = data[0];
    const H = {}; headers.forEach((h,i)=>H[h]=i);
    
    const buckets = {};
    for (let i=1; i<data.length; i++) {
      const r = data[i];
      const day = String(r[H["TimestampISO"]] || "").slice(0,10);
      if (day.length !== 10) continue;
      
      const key = `${day}|${r[H["Schedule"]]}|${r[H["Machine"]]}`;
      if (!buckets[key]) buckets[key] = { day, sched: r[H["Schedule"]], mach: r[H["Machine"]], samples:0, missed:0, extended:0, overdueSum:0 };
      
      const b = buckets[key];
      b.samples++;
      if (r[H["CriticalFlag"]] == 1) b.missed++;
      if (r[H["ExtendedFlag"]] == 1) b.extended++;
      b.overdueSum += Number(r[H["OverdueMinutes"]]||0);
    }
    
    const out = [["Day","Schedule","Machine","Samples","Missed","Extended","AvgOverdue"]];
    Object.values(buckets).forEach(b => {
      out.push([b.day, b.sched, b.mach, b.samples, b.missed, b.extended, Math.round(b.overdueSum/b.samples)]);
    });
    
    const outSh = this.repo.ensureSheet(this.outSheet, out[0]);
    outSh.clearContents();
    if(out.length > 1) outSh.getRange(1,1,out.length,out[0].length).setValues(out);
  }
}

/**
 * Deletes log rows older than retention period to prevent sheet overflow.
 * Reads timestamps (Column A) and deletes logic rows.
 */
class LogRetentionManager {
  constructor(repo, retentionDays) {
    this.repo = repo;
    this.cutoff = new Date();
    this.cutoff.setDate(this.cutoff.getDate() - retentionDays);
  }
  
  /**
   * Scans the sheet and deletes old rows.
   * Assumes logs are chronological (oldest at top).
   * @param {string} sheetName 
   */
  cleanSheet(sheetName) {
    const sheet = this.repo.ss.getSheetByName(sheetName);
    if (!sheet || sheet.getLastRow() < 2) return;
    
    // Optimistic Check: Only check first 100 rows (since logs are chronological)
    const rowsToCheck = Math.min(sheet.getLastRow()-1, 100);
    const dates = sheet.getRange(2, 1, rowsToCheck, 1).getValues();
    let deleteCount = 0;
    
    for (let i=0; i<dates.length; i++) {
      const d = new Date(dates[i][0]);
      if (!isNaN(d.getTime()) && d < this.cutoff) deleteCount++;
      else break; // Stop as soon as we hit a new date
    }
    
    if (deleteCount > 0) {
      sheet.deleteRows(2, deleteCount);
      console.log(`Cleaned ${deleteCount} rows from ${sheetName}`);
    }
  }
}

// ===================================================================================
// 4. HELPERS
// ===================================================================================

/** * Safe extractor for row data. Handles mapping indices to named properties.
 * @param {Array} row - Raw row data from sheet.
 * @returns {Object} Structured data.
 */
function getRowData(row) {
  const get = (idx) => (row[idx-1] === undefined) ? "" : row[idx-1];
  const num = (idx) => {
    const n = Number(get(idx));
    return isFinite(n) ? n : null;
  };

  return {
    scheduleName: String(get(CONFIG.COLUMNS.SCHEDULE_NAME)).trim(),
    machineName: String(get(CONFIG.COLUMNS.MACHINE)).trim(),
    ifRanToday: String(get(CONFIG.COLUMNS.IF_RAN_TODAY)).trim(),
    lastExpectedRun: get(CONFIG.COLUMNS.LAST_EXPECTED_RUN),
    currentStatus: String(get(CONFIG.COLUMNS.CURRENT_STATUS)).trim(),
    currentDuration: num(CONFIG.COLUMNS.CURRENT_DURATION) || 0,
    maxRunHoursExpected: num(CONFIG.COLUMNS.MAX_RUN_HOURS) || 0,
    overdueMinutes: num(CONFIG.COLUMNS.NOW_DIFF),
    notRanReason: String(get(CONFIG.COLUMNS.NOT_RAN_REASON)).trim()
  };
}

/** * Parses trigger flags from the sheet columns.
 * @returns {{critical: boolean, extended: boolean}} 
 */
function getAlertFlags(row) {
  const crit = String(row[CONFIG.COLUMNS.CRITICAL_ALERT - 1] || "").toUpperCase();
  const ext = String(row[CONFIG.COLUMNS.EXTENDED_ALERT - 1] || "").toUpperCase();
  return {
    critical: crit === CONFIG.ALERTS.CRITICAL_VALUE,
    extended: crit === CONFIG.ALERTS.EXTENDED_VALUE || ext === CONFIG.ALERTS.EXTENDED_VALUE
  };
}

/**
 * Formats minutes into human readable string.
 * @param {number} m - Minutes.
 * @returns {string} e.g. "1h 30m".
 */
function formatDuration(m) {
  const h = Math.floor(m/60);
  const min = Math.floor(m%60);
  return h ? `${h}h ${min}m` : `${min}m`;
}

// ===================================================================================
// 5. UI
// ===================================================================================

/** Creates custom menu on open */
function onOpen() {
  SpreadsheetApp.getUi().createMenu('Schedule Monitor')
    .addItem('▶ Run Monitor Now', 'runScheduleMonitor')
    .addItem('🧹 Run Maintenance', 'runDailyMaintenance')
    .addToUi();
}
