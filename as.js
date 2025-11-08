/* eslint-disable no-var */
"use strict";

/**
 * @fileoverview Schedule Monitor System - Automated monitoring and alerting for scheduled tasks
 * @description Monitors scheduled tasks in Google Sheets, sends alerts for missed schedules 
 * and extended executions, maintains observation logs with automatic rotation, and generates 
 * trend metrics.
 * @author Emily Cabaniss
 * @version 2.0.0
 * @requires Google Apps Script
 */
// ------- CONFIG / CONSTANTS ---------------------------------------------------
/**
 * Main configuration object for the Schedule Monitor system.
 * @const {Object} CONFIG
 * @property {Object} CORE - Core system settings
 * @property {string} CORE.SHEET_NAME - Name of the sheet containing schedule data
 * @property {number} CORE.DEBOUNCE_MINUTES - Minimum minutes between runs
 * @property {string} CORE.LAST_PROCESSED_PROPERTY - Script property key for last run timestamp
 * @property {string} CORE.HEADER_HASH_PROPERTY - Script property key for header hash
 * @property {string} CORE.LAST_HEADERS_JSON_PROPERTY - Script property key for header snapshot
 * @property {Object} COLUMNS - Column index mappings (1-based)
 * @property {Object} EXPECTED_HEADERS - Expected header variations for validation
 * @property {Object} ALERTS - Alert configuration
 * @property {number} ALERTS.TTL_MINUTES - Alert deduplication time-to-live in minutes
 * @property {string} ALERTS.CRITICAL_VALUE - Cell value triggering critical alerts
 * @property {string} ALERTS.EXTENDED_VALUE - Cell value triggering extended execution alerts
 * @property {number} ALERTS.MIN_OVERDUE_MINUTES - Minimum overdue time to trigger alerts
 * @property {Object} LOGGING - Logging configuration
 * @property {string} LOGGING.MODE - Observation logging mode ('changes'|'all')
 * @property {number} LOGGING.MAX_ROWS - Maximum rows before log rotation
 * @property {Object} LOGGING.SHEETS - Sheet names for different log types
 * @property {Object} SERVICES - External service configurations
 * @readonly
 */
const CONFIG = {
  // Core Settings
  CORE: {
    SHEET_NAME: "Notifications Logic",
    DEBOUNCE_MINUTES: 1,
    LAST_PROCESSED_PROPERTY: "lastProcessedTime",
    HEADER_HASH_PROPERTY: "HEADER_HASH_SHA256",
    LAST_HEADERS_JSON_PROPERTY: "LAST_HEADERS_JSON"
  },
  
  // Column Mapping (1-based indices)
  COLUMNS: {
    SCHEDULE_NAME: 1,           // A
    MACHINE: 7,                  // G
    IF_RAN_TODAY: 10,           // J
    LAST_EXPECTED_RUN: 11,      // L
    MAX_START: 13,              // N
    CURRENT_STATUS: 15,         // O
    CURRENT_DURATION: 17,       // Q
    MAX_RUN_HOURS: 19,          // S
    NOT_RAN_REASON: 21,         // U
    NOW_DIFF: 27,               // AA
    CRITICAL_ITEM_ALERT: 28,    // AB
    EXTENDED_EXECUTION_ALERT: 29 // AC
  },
  
  // Expected Header Values (for validation)
  EXPECTED_HEADERS: {
    1: ["Schedule Name", "Schedule", "SCHEDULE NAME"],
    7: ["Machine", "Server", "MACHINE"],
    10: ["If Ran Today", "Ran Today", "IF RAN TODAY"],
    11: ["Last Expected Run Day / Time", "Last Expected Run", "Expected Start"],
    13: ["Max Start", "Latest Allowed Start", "MAX START"],
    15: ["Current Status", "Status", "CURRENT STATUS"],
    17: ["Current Duration (min)", "Current Duration", "Runtime Minutes"],
    19: ["Max Run Hours", "Expected Duration", "Max Duration Hours"],
    21: ["Not Ran Reason", "Miss Reason", "Reason"],
    27: ["Now Diff (min)", "Minutes Overdue", "Overdue (min)"],
    28: ["Critical Item Alert", "Not Ran Alert", "CRITICAL ITEM ALERT"],
    29: ["Extended Execution Alert", "Duration Alert", "EXTENDED EXECUTION ALERT"]
  },
  
  // Alert Settings
  ALERTS: {
    TTL_MINUTES: 60,
    CRITICAL_VALUE: "NOT RAN ALERT",
    EXTENDED_VALUE: "DURATION ALERT",
    MIN_OVERDUE_MINUTES: 15
  },
  
  // Logging Settings
  LOGGING: {
    MODE: "changes", // 'changes' | 'all'
    MAX_ROWS: 10000,
    SHEETS: {
      EVENTS: "EventLogs",
      OBSERVATIONS: "Observations", 
      TRENDS: "TrendMetrics",
      RUNTIME_HISTORY: "RuntimeHistory",
      RUNTIME_STATUS: "RuntimeStatus"
    }
  },
  
  // External Services (loaded from Script Properties)
  SERVICES: {
    CHAT_WEBHOOK_URL: PropertiesService.getScriptProperties().getProperty('CHAT_WEBHOOK_URL') || "",
    FAILURE_EMAIL_RECIPIENT: PropertiesService.getScriptProperties().getProperty('FAILURE_EMAIL_RECIPIENT') || "",
    LOG_ARCHIVE_FOLDER_ID: PropertiesService.getScriptProperties().getProperty('LOG_ARCHIVE_FOLDER_ID') || ""
  }
};

// Freeze CONFIG to prevent accidental modification
Object.freeze(CONFIG);
Object.freeze(CONFIG.CORE);
Object.freeze(CONFIG.COLUMNS);
Object.freeze(CONFIG.EXPECTED_HEADERS);
Object.freeze(CONFIG.ALERTS);
Object.freeze(CONFIG.LOGGING);
Object.freeze(CONFIG.LOGGING.SHEETS);
Object.freeze(CONFIG.SERVICES);

// ------- "TYPES" (via JSDoc) --------------------------------------------------
/**
 * Data structure representing a single schedule row
 * @typedef {Object} RowData
 * @property {string} scheduleName - Name of the scheduled task
 * @property {string} machineName - Machine/server running the task
 * @property {string} ifRanToday - Whether the schedule ran today
 * @property {(string|Date|null)} lastExpectedRun - Last expected execution time
 * @property {number} currentDuration - Current execution duration in minutes
 * @property {number} maxRunHoursExpected - Maximum expected runtime in hours
 * @property {string} currentStatus - Current execution status
 * @property {(number|null)} overdueMinutes - Minutes overdue (null if not applicable)
 * @property {string} notRanReason - Reason for non-execution
 */
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

// ------- SERVICE CLASSES ------------------------------------------------------
/**
 * Central cache key builder for consistent and collision-resistant key generation.
 * Implements hashing for long values and validates key lengths against Google's limits.
 * @class
 */
class CacheKeyBuilder {
  static SEPARATOR = '::';
  static MAX_COMPONENT_LENGTH = 50;
  static HASH_PREFIX_LENGTH = 25;
  
  /**
   * Build a cache key for alert deduplication
   * @static
   * @param {string} type - Alert type ('missed'|'extended')
   * @param {string} schedule - Schedule name
   * @param {string} machine - Machine name
   * @returns {string} Validated cache key
   * @example
   * const key = CacheKeyBuilder.alert('missed', 'Daily Report', 'Server01');
   * // Returns: "alert::missed::Daily_Report::Server01"
   */
  static alert(type, schedule, machine) {
    return ['alert', type, schedule, machine]
      .map(this.sanitize_)
      .join(this.SEPARATOR);
  }
  /**
   * Build a cache key for observation state signatures
   * @static
   * @param {string} schedule - Schedule name
   * @param {string} machine - Machine name
   * @returns {string} Validated cache key for observation deduplication
   */
  static observationSignature(schedule, machine) {
    return ['obs_sig', schedule, machine]
      .map(this.sanitize_)
      .join(this.SEPARATOR);
  }
  /**
   * Build a cache key for in-flight execution guards
   * @static
   * @param {string} [identifier='default'] - Optional identifier for the guard
   * @returns {string} Cache key for preventing concurrent executions
   */
  static inFlightGuard(identifier = 'default') {
    return ['run_in_flight', identifier]
      .map(this.sanitize_)
      .join(this.SEPARATOR);
  }
  /**
   * Sanitize a key component to prevent injection and collisions.
   * Uses SHA-256 hashing for values exceeding MAX_COMPONENT_LENGTH.
   * @private
   * @static
   * @param {*} value - Value to sanitize
   * @returns {string} Sanitized string with max 50 chars
   */
  static sanitize_(value) {
    const str = String(value || 'null').replace(/[^a-zA-Z0-9_-]/g, '_');
    
    // If the string is short enough, return as-is
    if (str.length <= this.MAX_COMPONENT_LENGTH) {
      return str;
    }
    
    // For long strings, use prefix + hash to maintain uniqueness
    const hash = Utilities.computeDigest(
      Utilities.DigestAlgorithm.SHA_256,
      str
    );
    const hashStr = Utilities.base64Encode(hash)
      .replace(/[^a-zA-Z0-9]/g, '')  // Remove non-alphanumeric from base64
      .substring(0, 20);  // Take first 20 chars of hash
    
    // Combine recognizable prefix with hash
    const prefix = str.substring(0, this.HASH_PREFIX_LENGTH);
    return `${prefix}_H${hashStr}`;  // 'H' indicates this is hashed
  } 
  /**
   * Validate that a complete cache key doesn't exceed Google's limits
   * @private
   * @param {string} key
   * @returns {string}
   * @throws {Error} if key is too long
   */
  static validateKeyLength_(key) {
    // Google Apps Script cache keys have a limit of 250 characters
    const MAX_KEY_LENGTH = 250;
    
    if (key.length > MAX_KEY_LENGTH) {
      // If somehow the key is still too long, hash the entire key
      const hash = Utilities.computeDigest(
        Utilities.DigestAlgorithm.SHA_256,
        key
      );
      const hashStr = Utilities.base64Encode(hash)
        .replace(/[^a-zA-Z0-9]/g, '')
        .substring(0, 60);
      
      Logger.log(`Warning: Cache key too long (${key.length} chars), using hash: ${hashStr}`);
      return `FULL_HASH_${hashStr}`;
    }
    
    return key;
  } 
  /**
   * Build any cache key with validation
   * @private
   * @param {string[]} components
   * @returns {string}
   */
  static buildKey_(components) {
    const key = components
      .map(c => this.sanitize_(c))
      .join(this.SEPARATOR);
    
    return this.validateKeyLength_(key);
  }
}
/**
 * Main orchestrator for schedule monitoring operations.
 * Coordinates validation, observation logging, alert deduplication, and notifications.
 * @class
 */
class ScheduleMonitor {
  /**
   * Creates a new ScheduleMonitor instance
   * @constructor
   * @param {Object} config - Configuration object (typically CONFIG)
   * @example
   * const monitor = new ScheduleMonitor(CONFIG);
   * const success = monitor.execute();
   */
  constructor(config) {
    this.config = config;
    this.runId = Utilities.getUuid();
    this.ss = SpreadsheetApp.getActiveSpreadsheet();
    this.tz = this.ss.getSpreadsheetTimeZone();
    
    // Initialize services
    this.repo = new SheetsRepo(this.ss, this.tz);
    this.events = new EventsLogger(this.repo, this.config.LOGGING.SHEETS.EVENTS, this.config);
    this.observations = new ObservationsLogger(this.repo, this.config.LOGGING.SHEETS.OBSERVATIONS, this.config);
    this.history = new RuntimeHistoryLogger(this.repo, this.config);
    this.notifications = new NotificationManager(this.config);
    this.alertDedupe = new AlertDedupeCache(this.config.ALERTS.TTL_MINUTES);
    
    // Tracking metrics
    this.metrics = {
      observationsLogged: 0,
      alertsFired: 0,
      headerChanged: false,
      rowErrors: 0,
      failedRows: []
    };
  }
  /**
   * Main execution method that orchestrates the monitoring workflow
   * @public
   * @returns {boolean} Success status of the monitoring run
   * @description Workflow:
   * 1. Validates prerequisites (sheet existence)
   * 2. Validates headers against expected configuration
   * 3. Processes all data rows for observations and alerts
   * 4. Logs completion metrics
   * 5. Updates header hash for change detection
   * 6. Marks successful run completion
   */
  execute() {
    try {
      // Step 1: Validate prerequisites
      if (!this.validatePrerequisites_()) {
        return false;
      }
      
      // Step 2: Validate headers
      const headerValidation = this.validateHeaders_();
      if (!headerValidation.success) {
        return false;
      }
      
      // Step 3: Process data rows
      this.processDataRows_();
      
      // Step 4: Log completion and update status
      this.logCompletion_();
      
      // Step 5: Update header hash after successful run
      this.updateHeaderHash_(headerValidation.headers);
      
      // Step 6: Mark successful completion
      this.markSuccessfulRun_();
      
      return true;
      
    } catch (error) {
      this.handleExecutionError_(error);
      return false;
    }
  }
  /**
   * Validate prerequisites (sheet existence)
   * @private
   */
  validatePrerequisites_() {
    const sheet = this.ss.getSheetByName(this.config.CORE.SHEET_NAME);
    if (!sheet) {
      const msg = `Sheet "${this.config.CORE.SHEET_NAME}" not found.`;
      Logger.log(msg);
      this.events.log("Error", "N/A", "N/A", msg);
      this.history.append({
        ok: false,
        runId: this.runId,
        obsCount: 0,
        alertCount: 0,
        headerChanged: false,
        error: msg
      });
      this.notifications.sendSystemFailure(new Error(msg));
      return false;
    }
    this.sheet = sheet;
    return true;
  }
  /**
   * Validate headers
   * @private
   * @returns {{success: boolean, headers: Array, errors?: Array, warnings?: Array}}
   */
  validateHeaders_() {
    const validator = new HeaderValidator(this.sheet, this.config);
    const validation = validator.validate();
    
    // Check for header changes
    const props = PropertiesService.getScriptProperties();
    const currentHash = validator._computeHash(validation.headers);
    const prevHash = props.getProperty(this.config.CORE.HEADER_HASH_PROPERTY) || "";
    this.metrics.headerChanged = !!(prevHash && prevHash !== currentHash);
    
    // Handle validation failure
    if (!validation.valid) {
      const msg = `Header validation failed:\n${validation.errors.join("\n")}`;
      Logger.log(msg);
      Logger.log(validator.getReport());
      this.events.log("Error", "N/A", "N/A", msg);
      
      this.writeRuntimeStatus_({
        ok: false,
        error: msg,
        headers: validation.headers,
        headerErrors: validation.errors,
        headerWarnings: validation.warnings,
        headerHash: currentHash,
        prevHeaderHash: prevHash || "(none)",
        headerChanged: this.metrics.headerChanged
      });
      
      this.history.append({
        ok: false,
        runId: this.runId,
        obsCount: 0,
        alertCount: 0,
        headerChanged: this.metrics.headerChanged,
        error: msg
      });
      
      this.notifications.sendSystemFailure(new Error(msg));
      return { success: false };
    }
    
    // Log warnings if any
    if (validation.warnings.length > 0) {
      Logger.log(`Header warnings:\n${validation.warnings.join("\n")}`);
      this.events.log("Warning", "N/A", "N/A", {
        msg: "Header warnings detected",
        warnings: validation.warnings
      });
    }
    
    return {
      success: true,
      headers: validation.headers,
      warnings: validation.warnings,
      currentHash,
      prevHash
    };
  }
  /**
   * Process all data rows with error tolerance
   * @private
   * @throws {Error} If more than 10 rows fail processing
   * @description Processes each row individually with error handling.
   * Failed rows are logged but don't stop processing unless threshold exceeded.
   */
  processDataRows_() {
    const data = this.sheet.getDataRange().getValues();
    
    for (let i = 1; i < data.length; i++) {
      try {
        const row = data[i];
        const rowData = getRowData(row);
        const flags = getAlertFlags(row);
        
        this.processObservation_(i + 1, rowData, flags);
        this.processAlerts_(i + 1, rowData, flags);
      } catch (error) {
        this.metrics.rowErrors++;
        this.metrics.failedRows.push(i + 1);  // Store actual row number
        Logger.log(`Error processing row ${i + 1}: ${error.message}`);
        
        // Log to events for audit trail
        this.events.log("RowProcessingError", "N/A", "N/A", {
          rowIndex: i + 1,
          error: error.message
        });
        
        // Fail fast if too many errors
        if (this.metrics.rowErrors > 10) {
          throw new Error(`Too many row processing errors (${this.metrics.rowErrors}). Failed rows: ${this.metrics.failedRows.join(', ')}`);
        }
      }
    }
  }
  /**
   * Process observation for a single row
   * @private
   */
  processObservation_(rowIndex, rowData, flags) {
    if (this.shouldLogObservation_(rowData)) {
      this.observations.append(rowIndex, rowData, flags);
      this.metrics.observationsLogged++;
    }
  }  
  /**
   * Process alerts for a single row
   * @private
   */
  processAlerts_(rowIndex, rowData, flags) {
    // Critical Alert
    if (this.shouldFireCriticalAlert_(rowData, flags)) {
      const key = CacheKeyBuilder.alert('missed', rowData.scheduleName, rowData.machineName);
      if (this.alertDedupe.shouldFire(rowData.scheduleName, rowData.machineName, 'missed')) {
        this.notifications.sendMissedSchedule(rowData);
        this.events.log("MissedScheduleNotification", rowData.scheduleName, rowData.machineName, {
          rowIndex,
          overdueMinutes: rowData.overdueMinutes,
          reason: this.config.ALERTS.CRITICAL_VALUE
        });
        this.metrics.alertsFired++;
      }
    }
    
    // Extended Execution Alert
    if (this.shouldFireExtendedAlert_(rowData, flags)) {
      const key = CacheKeyBuilder.alert('extended', rowData.scheduleName, rowData.machineName);
      if (this.alertDedupe.shouldFire(rowData.scheduleName, rowData.machineName, 'extended')) {
        this.notifications.sendExtendedExecution(rowData);
        this.events.log("ExtendedExecutionNotification", rowData.scheduleName, rowData.machineName, {
          rowIndex,
          durationMinutes: rowData.currentDuration,
          maxRunHours: rowData.maxRunHoursExpected,
          reason: this.config.ALERTS.EXTENDED_VALUE
        });
        this.metrics.alertsFired++;
      }
    }
  } 
  /**
   * Determine if an observation should be logged
   * @private
   * @param {RowData} rowData - Row data to evaluate
   * @returns {boolean} True if observation should be logged
   * @description In 'changes' mode, only logs when state signature changes.
   * In 'all' mode, logs every observation.
   */
  shouldLogObservation_(rowData) {
    if (this.config.LOGGING.MODE === "all") return true;
    
    const key = CacheKeyBuilder.observationSignature(rowData.scheduleName, rowData.machineName);
    const cache = CacheService.getScriptCache();
    const lastSig = cache.get(key);
    const currentSig = this.computeStateSignature_(rowData);
    
    if (lastSig !== currentSig) {
      cache.put(key, currentSig, 21600); // 6 hours max
      return true;
    }
    return false;
  } 
  /**
   * Compute state signature for observation throttling
   * @private
   */
  computeStateSignature_(rowData) {
    const sig = [
      rowData.ifRanToday,
      rowData.currentStatus,
      rowData.currentDuration,
      rowData.maxRunHoursExpected,
      rowData.overdueMinutes,
      rowData.notRanReason
    ].join("|");
    return Utilities.base64Encode(
      Utilities.computeDigest(Utilities.DigestAlgorithm.MD5, sig)
    );
  }
  /**
   * Check if critical alert should fire
   * @private
   */
  shouldFireCriticalAlert_(rowData, flags) {
    return flags.critical &&
           rowData.overdueMinutes !== null &&
           rowData.overdueMinutes >= this.config.ALERTS.MIN_OVERDUE_MINUTES;
  } 
  /**
   * Check if extended alert should fire
   * @private
   */
  shouldFireExtendedAlert_(rowData, flags) {
    return flags.extended;
  }
  /**
   * Log completion
   * @private
   */
  logCompletion_() {
    Logger.log("Business logic execution completed.");
    
    const completionData = {
      msg: "Business logic execution completed",
      observationsLogged: this.metrics.observationsLogged,
      alertsFired: this.metrics.alertsFired,
      debounceMinutes: this.config.CORE.DEBOUNCE_MINUTES,
      alertTtlMinutes: this.config.ALERTS.TTL_MINUTES,
      obsMode: this.config.LOGGING.MODE,
      headerWarnings: 0
    };
    
    // Add row error info if any occurred
    if (this.metrics.rowErrors > 0) {
      completionData.rowErrors = this.metrics.rowErrors;
      completionData.failedRows = this.metrics.failedRows;
    }
    
    this.events.log("Info", "N/A", "N/A", JSON.stringify(completionData));
    
    this.writeRuntimeStatus_({
      ok: true,
      observationsLogged: this.metrics.observationsLogged,
      alertsFired: this.metrics.alertsFired,
      headerChanged: this.metrics.headerChanged,
      rowErrors: this.metrics.rowErrors,  // Add this
      failedRows: this.metrics.failedRows  // Add this
    });
    
    this.history.append({
      ok: true,
      runId: this.runId,
      obsCount: this.metrics.observationsLogged,
      alertCount: this.metrics.alertsFired,
      headerChanged: this.metrics.headerChanged,
      rowErrors: this.metrics.rowErrors,  // Add this
      error: ""
    });
  } 
  /**
   * Update header hash after successful run
   * @private
   */
  updateHeaderHash_(headers) {
    const validator = new HeaderValidator(this.sheet, this.config);
    validator.updateStoredHeaders(headers);
  }
  /**
   * Mark successful run completion
   * @private
   */
  markSuccessfulRun_() {
    const props = PropertiesService.getScriptProperties();
    props.setProperty(this.config.CORE.LAST_PROCESSED_PROPERTY, String(Date.now()));
  }
  /**
   * Handle execution error
   * @private
   */
  handleExecutionError_(error) {
    Logger.log(`Error during execution: ${error.message}`);
    this.notifications.sendSystemFailure(error);
    
    try {
      this.history.append({
        ok: false,
        runId: this.runId,
        obsCount: this.metrics.observationsLogged,
        alertCount: this.metrics.alertsFired,
        headerChanged: this.metrics.headerChanged,
        error: error.message
      });
    } catch (_) {
      Logger.log("Failed to log error to history");
    }
  }
  /**
   * Write runtime status
   * @private
   */
  writeRuntimeStatus_(info) {
    const name = this.config.LOGGING.SHEETS.RUNTIME_STATUS || "RuntimeStatus";
    const sh = this.repo.ss.getSheetByName(name) || this.repo.ss.insertSheet(name);
    sh.clearContents();
    
    const rows = [];
    const add = (k, v) => rows.push([k, typeof v === "string" ? v : JSON.stringify(v)]);
    
    add("TimestampISO", this.repo.timestampISO());
    add("OK", String(!!info.ok));
    add("RunId", this.runId);
    
    if (info.error) add("Error", info.error);
    if (typeof info.observationsLogged === "number") {
      add("Observations Logged", String(info.observationsLogged));
    }
    if (typeof info.alertsFired === "number") {
      add("Alerts Fired", String(info.alertsFired));
    }
    if (typeof info.headerChanged === "boolean") {
      add("Header Changed", String(info.headerChanged));
    }
    
    // Add row error reporting
    if (typeof info.rowErrors === "number" && info.rowErrors > 0) {
      add("Row Processing Errors", String(info.rowErrors));
      if (info.failedRows && info.failedRows.length > 0) {
        add("Failed Rows", info.failedRows.join(", "));
      }
    }
    
    sh.getRange(1, 1, rows.length, 2).setValues(rows);
    sh.autoResizeColumns(1, 2);
  }
}
/**
 * Validates spreadsheet headers against expected configuration.
 * Detects changes, missing columns, and mismatched headers.
 * @class
 */
class HeaderValidator {
  /**
   * Creates a HeaderValidator instance
   * @constructor
   * @param {GoogleAppsScript.Spreadsheet.Sheet} sheet - Sheet to validate
   * @param {Object} config - Configuration object with column mappings
   */
  constructor(sheet, config = CONFIG) {
    this.sheet = sheet;
    this.config = config;
    this.errors = [];
    this.warnings = [];
  }
  /**
   * Validates sheet headers against configuration
   * @public
   * @returns {Object} Validation result
   * @returns {boolean} returns.valid - Whether headers are valid
   * @returns {string[]} returns.errors - Critical validation errors
   * @returns {string[]} returns.warnings - Non-critical warnings
   * @returns {string[]} returns.headers - Actual headers found
   * @example
   * const validator = new HeaderValidator(sheet, CONFIG);
   * const result = validator.validate();
   * if (!result.valid) {
   *   console.log('Errors:', result.errors);
   * }
   */
  validate() {
    this.errors = [];
    this.warnings = [];
    
    if (!this.sheet) {
      this.errors.push("Sheet not found");
      return this._getResult();
    }
    
    const lastColumn = this.sheet.getLastColumn();
    const headers = this.sheet.getRange(1, 1, 1, lastColumn).getValues()[0];
    
    // Validate each critical column
    Object.entries(this.config.COLUMNS).forEach(([name, index]) => {
      this._validateColumn(name, index, headers);
    });
    
    // Check for header changes
    this._checkHeaderIntegrity(headers);
    
    return this._getResult(headers);
  }
  /**
   * Validates a single column
   * @private
   */
  _validateColumn(name, expectedIndex, headers) {
    const actualHeader = headers[expectedIndex - 1]; // Convert to 0-based
    
    if (!actualHeader) {
      this.errors.push(`Column ${name} (${expectedIndex}): Missing header`);
      return;
    }
    
    const expectedVariants = this.config.EXPECTED_HEADERS[expectedIndex];
    if (!expectedVariants) {
      this.warnings.push(`Column ${name} (${expectedIndex}): No validation rules defined`);
      return;
    }
    
    const normalized = this._normalizeHeader(actualHeader);
    const expectedNormalized = expectedVariants.map(v => this._normalizeHeader(v));
    
    if (!expectedNormalized.includes(normalized)) {
      this.errors.push(
        `Column ${name} (${expectedIndex}): ` +
        `Found "${actualHeader}" but expected one of: ${expectedVariants.join(", ")}`
      );
    }
  } 
  /**
   * Normalizes header for comparison
   * @private
   */
  _normalizeHeader(header) {
    return String(header || "")
      .trim()
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, " ")
      .replace(/\s+/g, " ")
      .trim();
  }
  /**
   * Extract only the critical headers we monitor
   * @private
   */
  _extractCriticalHeaders(headers) {
    const critical = {};
    Object.values(this.config.COLUMNS).forEach(index => {
      if (index <= headers.length) {
        critical[index] = headers[index - 1];
      }
    });
    return headers.map((h, i) => {
      // Keep only headers we monitor or those nearby
      const colIndex = i + 1;
      const isMonitored = Object.values(this.config.COLUMNS).includes(colIndex);
      const isNearby = Object.values(this.config.COLUMNS).some(
        idx => Math.abs(idx - colIndex) <= 2
      );
      return (isMonitored || isNearby) ? h : null;
    });
  }
  /**
   * Checks for unexpected header changes using hash
   * @private
   */
  _checkHeaderIntegrity(headers) {
    const props = PropertiesService.getScriptProperties();
    const currentHash = this._computeHash(headers);
    const storedHash = props.getProperty(this.config.CORE.HEADER_HASH_PROPERTY);
    
    if (storedHash && storedHash !== currentHash) {
      this.warnings.push("Headers have changed since last run");
      
      // Get detailed diff if possible
      try {
        const storedHeadersJson = props.getProperty(this.config.CORE.LAST_HEADERS_JSON_PROPERTY);
        if (storedHeadersJson) {
          const storedHeaders = JSON.parse(storedHeadersJson);
          
          // Check if we have full headers or minimal version
          const isMinimal = storedHeaders.length > 0 && 
                           typeof storedHeaders[0] === 'object' && 
                           storedHeaders[0].hasOwnProperty('i');
          
          if (isMinimal) {
            this.warnings.push("Detailed header comparison unavailable (headers were too large to store completely)");
          } else {
            const diff = this._computeDiff(storedHeaders, headers);
            
            if (diff.added.length > 0) {
              this.warnings.push(`New columns added: ${diff.added.join(", ")}`);
            }
            if (diff.removed.length > 0) {
              this.warnings.push(`Columns removed: ${diff.removed.join(", ")}`);
            }
            if (diff.modified.length > 0) {
              diff.modified.forEach(m => {
                this.warnings.push(`Column ${m.index} changed from "${m.from}" to "${m.to}"`);
              });
            }
          }
        }
      } catch (e) {
        Logger.log(`Could not compute header diff: ${e.message}`);
      }
    }
    
    return currentHash;
  }
  /**
   * Computes SHA-256 hash of headers
   * @private
   */
  _computeHash(headers) {
    const normalized = headers.map(h => this._normalizeHeader(h)).join("|");
    const bytes = Utilities.computeDigest(Utilities.DigestAlgorithm.SHA_256, normalized);
    return bytes.map(b => (b + 256).toString(16).slice(-2)).join("");
  }
  /**
   * Computes difference between two header arrays
   * @private
   */
  _computeDiff(oldHeaders, newHeaders) {
    const diff = {
      added: [],
      removed: [],
      modified: []
    };
    
    const maxLength = Math.max(oldHeaders.length, newHeaders.length);
    
    for (let i = 0; i < maxLength; i++) {
      const oldHeader = oldHeaders[i];
      const newHeader = newHeaders[i];
      
      if (!oldHeader && newHeader) {
        diff.added.push(newHeader);
      } else if (oldHeader && !newHeader) {
        diff.removed.push(oldHeader);
      } else if (oldHeader && newHeader) {
        const oldNorm = this._normalizeHeader(oldHeader);
        const newNorm = this._normalizeHeader(newHeader);
        if (oldNorm !== newNorm) {
          diff.modified.push({
            index: i + 1,
            from: oldHeader,
            to: newHeader
          });
        }
      }
    }
    
    return diff;
  }
  /**
   * Updates stored header hash and snapshot with size management
   * @public
   * @param {string[]} headers - Current headers to store
   * @description Stores headers in Script Properties with automatic size
   * management. Falls back to minimal storage if headers exceed 8KB limit.
   */
  updateStoredHeaders(headers) {
    const props = PropertiesService.getScriptProperties();
    const hash = this._computeHash(headers);
    props.setProperty(this.config.CORE.HEADER_HASH_PROPERTY, hash);
    
    // Store headers with size management
    try {
      // First, try to store only the headers we care about (those in CONFIG.COLUMNS)
      const criticalHeaders = this._extractCriticalHeaders(headers);
      const criticalJson = JSON.stringify(criticalHeaders);
      
      // Check size (Properties have ~9KB limit per property)
      if (criticalJson.length < 8000) {
        props.setProperty(this.config.CORE.LAST_HEADERS_JSON_PROPERTY, criticalJson);
        props.deleteProperty(this.config.CORE.LAST_HEADERS_JSON_PROPERTY + "_FULL");
      } else {
        // If even critical headers are too large, store just indices and first few chars
        const minimalHeaders = criticalHeaders.map((h, i) => ({
          i: i,
          h: h ? h.substring(0, 20) : null
        }));
        props.setProperty(
          this.config.CORE.LAST_HEADERS_JSON_PROPERTY, 
          JSON.stringify(minimalHeaders)
        );
        props.setProperty(
          this.config.CORE.LAST_HEADERS_JSON_PROPERTY + "_FULL",
          "false"
        );
        
        Logger.log(`Headers too large for storage (${criticalJson.length} bytes). Storing minimal version.`);
      }
    } catch (e) {
      // If storage fails, at least store the hash
      Logger.log(`Failed to store headers: ${e.message}`);
      props.deleteProperty(this.config.CORE.LAST_HEADERS_JSON_PROPERTY);
    }
  }
  /**
   * Returns validation result
   * @private
   */
  _getResult(headers = []) {
    return {
      valid: this.errors.length === 0,
      errors: [...this.errors],
      warnings: [...this.warnings],
      headers: headers
    };
  }
  /**
   * Gets a formatted validation report
   * @public
   * @returns {string} Human-readable validation report
   */
  getReport() {
    const result = this.validate();
    const lines = [];
    
    lines.push(`Header Validation Report`);
    lines.push(`${"=".repeat(50)}`);
    lines.push(`Status: ${result.valid ? "✅ VALID" : "❌ INVALID"}`);
    lines.push("");
    
    if (result.errors.length > 0) {
      lines.push("ERRORS:");
      result.errors.forEach(e => lines.push(`  ❌ ${e}`));
      lines.push("");
    }
    
    if (result.warnings.length > 0) {
      lines.push("WARNINGS:");
      result.warnings.forEach(w => lines.push(`  ⚠️ ${w}`));
      lines.push("");
    }
    
    if (result.valid && result.warnings.length === 0) {
      lines.push("All headers match expected configuration.");
    }
    
    return lines.join("\n");
  }
}
/**
 * Repository pattern implementation for Google Sheets operations.
 * Provides centralized sheet management and data access.
 * @class
 */
class SheetsRepo {
  /**
   * Creates a SheetsRepo instance
   * @constructor
   * @param {GoogleAppsScript.Spreadsheet.Spreadsheet} ss - Spreadsheet instance
   * @param {string} tz - Timezone for date formatting
   */
  constructor(ss, tz) { this.ss = ss; this.tz = tz; }
  /**
   * Ensures a sheet exists with the specified header
   * @param {string} name - Sheet name
   * @param {string[]} header - Header row to create if sheet doesn't exist
   * @returns {GoogleAppsScript.Spreadsheet.Sheet} The sheet instance
   */
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
  /**
   * Formats a date as ISO timestamp with timezone
   * @param {Date} [d=new Date()] - Date to format
   * @returns {string} ISO formatted timestamp
   */
  timestampISO(d = new Date()) {
    return Utilities.formatDate(d, this.tz, "yyyy-MM-dd'T'HH:mm:ssXXX");
  }
}
/**
 * Handles log rotation with concurrency protection and data integrity.
 * Archives sheets when they exceed MAX_ROWS, with automatic recovery on failure.
 * @class
 */
class LogRotator {
  /**
   * Creates a LogRotator instance
   * @constructor
   * @param {SheetsRepo} repo - Repository instance
   * @param {string} sheetName - Name of sheet to rotate
   * @param {string[]} header - Header row for new sheets
   * @param {Object} config - Configuration object
   */
  constructor(repo, sheetName, header, config) {
    this.repo = repo; 
    this.sheetName = sheetName; 
    this.header = header; 
    this.cfg = config;
    this.repo.ensureSheet(sheetName, header);
    
    // Lock key specific to this sheet
    this.lockKey = `ROTATION_LOCK_${sheetName}`;
    this.lockTimeout = 60000; // 60 seconds max for rotation
  }
  getSheet_() { 
    return this.repo.ss.getSheetByName(this.sheetName); 
  }
  /**
   * Acquire a rotation lock for this specific sheet
   * @private
   * @returns {boolean} Whether lock was acquired
   */
  acquireRotationLock_() {
    const cache = CacheService.getScriptCache();
    const existingLock = cache.get(this.lockKey);
    
    if (existingLock) {
      // Check if lock is stale (older than timeout)
      const lockTime = Number(existingLock);
      const now = Date.now();
      
      if (now - lockTime > this.lockTimeout) {
        Logger.log(`Stale rotation lock detected for ${this.sheetName}, overriding`);
      } else {
        Logger.log(`Rotation already in progress for ${this.sheetName}`);
        return false;
      }
    }
    
    // Set lock with timestamp
    cache.put(this.lockKey, String(Date.now()), Math.floor(this.lockTimeout / 1000));
    return true;
  } 
  /**
   * Release the rotation lock
   * @private
   */
  releaseRotationLock_() {
    try {
      const cache = CacheService.getScriptCache();
      cache.remove(this.lockKey);
    } catch (e) {
      // Ignore errors when releasing lock
    }
  }
  /**
   * Check if rotation is needed and safe to perform
   * @private
   * @returns {{needed: boolean, safe: boolean, rows: number}}
   */
  checkRotationStatus_() {
    const sh = this.getSheet_();
    if (!sh) {
      return { needed: false, safe: false, rows: 0 };
    }
    
    const rows = sh.getLastRow();
    const dataRows = Math.max(0, rows - 1);
    
    // Check if rotation is needed
    const needed = dataRows >= this.cfg.LOGGING.MAX_ROWS;
    
    // Check if it's safe (not already rotating)
    const cache = CacheService.getScriptCache();
    const rotationInProgress = cache.get(this.lockKey);
    const safe = !rotationInProgress || this.isLockStale_(rotationInProgress);
    
    return { needed, safe, rows: dataRows };
  }
  /**
   * Check if a lock timestamp is stale
   * @private
   */
  isLockStale_(lockTimestamp) {
    if (!lockTimestamp) return true;
    const lockTime = Number(lockTimestamp);
    const now = Date.now();
    return (now - lockTime) > this.lockTimeout;
  }
  /**
   * Attempts rotation if needed with concurrency protection
   * @private
   * @throws {Error} If rotation fails after archive creation
   * @description Uses sheet-specific locks to prevent concurrent rotations.
   * Implements double-check pattern and automatic recovery on failure.
   */
  maybeRotate_() {
    const status = this.checkRotationStatus_();
    
    if (!status.needed) {
      return; // No rotation needed
    }
    
    if (!status.safe) {
      Logger.log(`Rotation needed for ${this.sheetName} but another rotation is in progress`);
      return; // Wait for existing rotation to complete
    }
    
    // Try to acquire lock
    if (!this.acquireRotationLock_()) {
      Logger.log(`Could not acquire rotation lock for ${this.sheetName}`);
      return;
    }
    
    const sh = this.getSheet_();
    const rows = sh.getLastRow();
    const dataRows = Math.max(0, rows - 1);
    
    Logger.log(`Starting rotation for ${this.sheetName}: ${dataRows} rows`);
    
    let archiveSheet = null;
    let originalData = null;
    
    try {
      // Double-check rotation is still needed (in case another process just did it)
      const currentRows = sh.getLastRow();
      if (currentRows - 1 < this.cfg.LOGGING.MAX_ROWS) {
        Logger.log(`Rotation no longer needed for ${this.sheetName}`);
        return;
      }
      
      // Step 1: Capture original data FIRST (before any modifications)
      const dataRange = sh.getDataRange();
      originalData = dataRange.getValues();
      
      if (!originalData || originalData.length === 0) {
        throw new Error("Failed to read original data");
      }
      
      // Step 2: Create archive sheet with timestamped name
      const ts = this.repo.timestampISO().replace(/[:]/g, "-").replace(/[T]/g, "_");
      const archiveName = `${this.sheetName}_Archive_${ts}`;
      
      // Check if archive name already exists (prevent overwrites)
      if (this.repo.ss.getSheetByName(archiveName)) {
        // Add a unique suffix if needed
        const uniqueSuffix = Utilities.getUuid().substring(0, 8);
        const uniqueArchiveName = `${archiveName}_${uniqueSuffix}`;
        Logger.log(`Archive name collision, using ${uniqueArchiveName}`);
        archiveName = uniqueArchiveName;
      }
      
      archiveSheet = this.repo.ss.insertSheet(archiveName);
      
      // Step 3: Write data to archive sheet
      if (originalData.length > 0 && originalData[0].length > 0) {
        archiveSheet.getRange(1, 1, originalData.length, originalData[0].length)
          .setValues(originalData);
      }
      
      // Step 4: Verify archive integrity
      const archiveRows = archiveSheet.getLastRow();
      if (archiveRows !== rows) {
        throw new Error(
          `Archive verification failed: expected ${rows} rows, got ${archiveRows}`
        );
      }
      
      // Step 5: Add metadata to archive (non-critical, don't fail rotation if this fails)
      try {
        archiveSheet.insertRowAfter(1);
        archiveSheet.getRange(2, 1, 1, 2).setValues([["Archived from:", this.sheetName]]);
        archiveSheet.getRange(3, 1, 1, 2).setValues([["Archive timestamp:", this.repo.timestampISO()]]);
        archiveSheet.getRange(4, 1, 1, 2).setValues([["Row count:", rows]]);
      } catch (metaError) {
        Logger.log(`Non-critical: Failed to add metadata to archive: ${metaError.message}`);
      }
      
      // Step 6: Optional CSV export (non-critical, don't fail rotation)
      if (this.cfg.SERVICES.LOG_ARCHIVE_FOLDER_ID) {
        try {
          const csv = this.exportCsv_(sh);
          const folder = DriveApp.getFolderById(this.cfg.SERVICES.LOG_ARCHIVE_FOLDER_ID);
          const csvName = `${this.sheetName}_${ts}.csv`;
          folder.createFile(Utilities.newBlob(csv, "text/csv", csvName));
          Logger.log(`CSV backup created: ${csvName}`);
        } catch (csvError) {
          // CSV export is best-effort; log but don't fail rotation
          Logger.log(`Optional CSV export failed (${this.sheetName}): ${csvError.message}`);
        }
      }
      
      // Step 7: Move archive sheet to end (non-critical)
      try {
        const lastPosition = this.repo.ss.getNumSheets();
        this.repo.ss.setActiveSheet(archiveSheet);
        this.repo.ss.moveActiveSheet(lastPosition);
      } catch (moveError) {
        Logger.log(`Non-critical: Failed to move archive sheet: ${moveError.message}`);
      }
      
      // Step 8: Clear and reset original sheet ONLY after archive is verified
      // Use a batch update for atomicity
      sh.clear(); // Clear everything including formatting
      sh.getRange(1, 1, 1, this.header.length).setValues([this.header]);
      
      // Step 9: Final verification
      const finalRows = sh.getLastRow();
      if (finalRows !== 1) {
        throw new Error(`Reset verification failed: expected 1 row, got ${finalRows}`);
      }
      
      Logger.log(`Successfully rotated ${this.sheetName}: ${dataRows} rows archived to ${archiveName}`);
      
      // Log successful rotation event
      this.logRotationSuccess_(archiveName, dataRows);
      
    } catch (error) {
      // CRITICAL ERROR RECOVERY
      Logger.log(`CRITICAL: Rotation failed for ${this.sheetName}: ${error.message}`);
      
      // Attempt to recover original data if possible
      if (originalData && sh) {
        try {
          Logger.log(`Attempting to restore original data to ${this.sheetName}`);
          sh.clear();
          sh.getRange(1, 1, originalData.length, originalData[0].length)
            .setValues(originalData);
          Logger.log(`Successfully restored original data`);
        } catch (restoreError) {
          Logger.log(`FATAL: Could not restore original data: ${restoreError.message}`);
          // At this point, data might be in the archive sheet if it was created
          if (archiveSheet) {
            Logger.log(`Data may be recoverable from archive sheet: ${archiveSheet.getName()}`);
          }
        }
      }
      
      // Clean up failed archive sheet if it was created
      if (archiveSheet) {
        try {
          this.repo.ss.deleteSheet(archiveSheet);
          Logger.log(`Cleaned up failed archive sheet`);
        } catch (cleanupError) {
          Logger.log(`Could not clean up archive sheet: ${cleanupError.message}`);
        }
      }
      
      // Re-throw to notify caller of failure
      throw new Error(`Log rotation failed: ${error.message}`);
      
    } finally {
      // Always release lock
      this.releaseRotationLock_();
    }
  }
  /**
   * Log successful rotation (avoiding circular dependency with EventsLogger)
   * @private
   */
  logRotationSuccess_(archiveName, rowCount) {
    try {
      // Direct log to avoid circular dependency
      const eventSheet = this.repo.ss.getSheetByName(this.cfg.LOGGING.SHEETS.EVENTS);
      if (eventSheet) {
        eventSheet.appendRow([
          this.repo.timestampISO(),
          "LogRotationCompleted",
          "SYSTEM",
          this.sheetName,
          JSON.stringify({
            archiveName: archiveName,
            rowsArchived: rowCount
          })
        ]);
      }
    } catch (e) {
      Logger.log(`Could not log rotation success: ${e.message}`);
    }
  }
  exportCsv_(sheet) {
    const values = sheet.getDataRange().getValues();
    const esc = (v) => {
      const s = v === null || v === undefined ? "" : String(v);
      // Escape quotes and wrap if contains comma, quote, newline, or carriage return
      if (/[",\n\r]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
      return s;
    };
    return values.map(r => r.map(esc).join(",")).join("\n");
  }
  /**
   * Appends a row with automatic rotation
   * @public
   * @param {Array} row - Row data to append
   * @throws {Error} If sheet not found and row cannot be appended
   * @description Attempts rotation first if needed, but always appends
   * the row even if rotation fails (data preservation priority).
   */
  appendRow(row) {
    // Try rotation with concurrency protection
    try {
      this.maybeRotate_();
    } catch (rotationError) {
      // Log rotation failed, but we shouldn't lose the current row
      // Continue to append to the existing sheet even if it's over the limit
      Logger.log(`Rotation failed, appending anyway: ${rotationError.message}`);
      
      // Optional: Send alert about rotation failure
      try {
        // Direct log to avoid circular dependency
        const eventSheet = this.repo.ss.getSheetByName(this.cfg.LOGGING.SHEETS.EVENTS);
        if (eventSheet) {
          eventSheet.appendRow([
            this.repo.timestampISO(),
            "LogRotationFailed",
            "SYSTEM",
            this.sheetName,
            JSON.stringify({ 
              error: rotationError.message, 
              rowsPending: 1 
            })
          ]);
        }
      } catch (e) {
        Logger.log(`Could not log rotation failure event: ${e.message}`);
      }
    }
    
    // Always attempt to append the row
    const sh = this.getSheet_();
    if (sh) {
      sh.appendRow(row);
    } else {
      throw new Error(`Cannot append row: sheet ${this.sheetName} not found`);
    }
  }
}
/**
 * Manages Google Chat webhook notifications.
 * @class
 */
class ChatNotifier {
  /**
   * Creates a ChatNotifier instance
   * @constructor
   * @param {string} webhookUrl - Google Chat webhook URL
   */
  constructor(webhookUrl) { this.url = webhookUrl; }
  /**
   * Sends a notification to Google Chat
   * @param {Object} payload - Chat message payload
   * @param {string} [payload.text] - Plain text message
   * @param {Array} [payload.cardsV2] - Card-based rich content
   * @description Sends notification via webhook with error logging
   */
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
/**
 * Cache-based alert deduplication to prevent notification spam.
 * @class
 */
class AlertDedupeCache {
  /**
   * Creates an AlertDedupeCache instance
   * @constructor
   * @param {number} ttlMinutes - Time-to-live for deduplication in minutes
   * @description TTL is capped at 6 hours (Google Apps Script limit)
   */
  constructor(ttlMinutes) {
    this.cache = CacheService.getScriptCache();
    this.ttlSec = Math.max(1, Math.min(21600, Math.floor((ttlMinutes || 60) * 60)));
  }
  /**
   * Determines if an alert should fire
   * @param {string} schedule - Schedule name
   * @param {string} machine - Machine name
   * @param {string} type - Alert type ('missed'|'extended')
   * @returns {boolean} True if alert should fire, false if deduplicated
   */
  shouldFire(schedule, machine, type) {
    const key = CacheKeyBuilder.alert(type, schedule, machine);
    const hit = this.cache.get(key);
    if (hit) return false;
    this.cache.put(key, String(Date.now()), this.ttlSec);
    return true;
  }
}
/**
 * Logger for schedule observations with automatic rotation.
 * Records schedule state changes and metrics.
 * @class
 */
class ObservationsLogger {
  /**
   * Creates an ObservationsLogger instance
   * @constructor
   * @param {SheetsRepo} repo - Repository instance
   * @param {string} sheetName - Observation sheet name
   * @param {Object} cfg - Configuration object
   */
  constructor(repo, sheetName, cfg) {
    this.repo = repo; this.sheet = sheetName; this.cfg = cfg;
    this.header = [
      "TimestampISO","RowIndex","Schedule","Machine","IfRanToday",
      "LastExpectedRunISO","OverdueMinutes","CurrentStatus",
      "CurrentDurationMin","MaxRunHours","CriticalFlag","ExtendedFlag"
    ];
    this.rotator = new LogRotator(repo, sheetName, this.header, cfg);
    repo.ensureSheet(sheetName, this.header);
  }
  /**
   * Appends an observation record
   * @param {number} rowIndex - Source row index (1-based)
   * @param {RowData} d - Row data
   * @param {Object} f - Alert flags
   * @param {boolean} f.critical - Critical alert flag
   * @param {boolean} f.extended - Extended execution alert flag
   */
  append(rowIndex, d, f) {
    const ts = this.repo.timestampISO();
    const iso = d.lastExpectedRun ? new Date(d.lastExpectedRun).toISOString() : "";
    this.rotator.appendRow([
      ts, rowIndex, d.scheduleName, d.machineName, d.ifRanToday,
      iso, d.overdueMinutes ?? "", d.currentStatus, d.currentDuration,
      d.maxRunHoursExpected, f.critical ? 1 : 0, f.extended ? 1 : 0
    ]);
  }
}
/**
 * Logger for system events with automatic rotation.
 * Records alerts, errors, and system events.
 * @class
 */
class EventsLogger {
  constructor(repo, sheetName, cfg) {
    this.repo = repo; this.sheet = sheetName; this.cfg = cfg;
    this.header = ["TimestampISO","EventType","Schedule","Machine","DetailsJSON"];
    this.rotator = new LogRotator(repo, sheetName, this.header, cfg);
    repo.ensureSheet(sheetName, this.header);
  }
  /**
   * Logs a system event
   * @param {string} eventType - Type of event (e.g., 'Error', 'Warning', 'Info')
   * @param {string} schedule - Schedule name or 'N/A'
   * @param {string} machine - Machine name or 'N/A'
   * @param {(string|Object)} details - Event details (stringified if object)
   */
  log(eventType, schedule, machine, details) {
    this.rotator.appendRow([
      this.repo.timestampISO(),
      eventType, schedule, machine,
      (typeof details === "string") ? details : JSON.stringify(details)
    ]);
  }
}
/**
 * Aggregates observations into daily trend metrics.
 * Processes data in batches for memory efficiency.
 * @class
 */
class TrendAggregator {
  /**
   * Creates a TrendAggregator instance
   * @constructor
   * @param {SheetsRepo} repo - Repository instance
   * @param {string} obsSheet - Source observations sheet name
   * @param {string} outSheet - Output trends sheet name
   */
  constructor(repo, obsSheet, outSheet) { 
    this.repo = repo; 
    this.obsSheet = obsSheet; 
    this.outSheet = outSheet;
    this.BATCH_SIZE = 1000; // Process 1000 rows at a time
  }
  /**
   * Builds daily trend metrics from observations
   * @public
   * @description Processes observations in configurable batches to prevent
   * memory issues. Calculates daily averages, percentiles, and counts.
   */
  buildDaily() {
    const sheet = this.repo.ss.getSheetByName(this.obsSheet);
    if (!sheet || sheet.getLastRow() < 2) return;
    
    // Get headers first
    const headers = sheet.getRange(1, 1, 1, sheet.getLastColumn()).getValues()[0];
    const H = {};
    headers.forEach((h, i) => H[h] = i);
    
    // Initialize buckets
    const buckets = {};
    
    // Process data in batches
    const totalRows = sheet.getLastRow();
    const dataRows = totalRows - 1; // Exclude header
    
    Logger.log(`Processing ${dataRows} rows in batches of ${this.BATCH_SIZE}`);
    
    for (let startRow = 2; startRow <= totalRows; startRow += this.BATCH_SIZE) {
      const endRow = Math.min(startRow + this.BATCH_SIZE - 1, totalRows);
      const batchSize = endRow - startRow + 1;
      
      // Read batch
      const batchData = sheet.getRange(startRow, 1, batchSize, sheet.getLastColumn()).getValues();
      
      // Process batch
      this.processBatch_(batchData, H, buckets);
      
      // Log progress
      if (startRow % (this.BATCH_SIZE * 5) === 2) {
        Logger.log(`Processed ${endRow - 1} of ${dataRows} rows`);
      }
      
      // Flush to prevent timeout on very large datasets
      SpreadsheetApp.flush();
    }
    
    // Write output
    this.writeOutput_(buckets);
  }  
  /**
   * Process a batch of rows
   * @private
   */
  processBatch_(rows, H, buckets) {
    for (const r of rows) {
      try {
        const ts = r[H["TimestampISO"]] || "";
        const day = ts.slice(0, 10);
        
        // Skip invalid dates
        if (!day || day.length !== 10) continue;
        
        const schedule = r[H["Schedule"]];
        const machine = r[H["Machine"]];
        const overdue = Number(r[H["OverdueMinutes"]] || 0);
        const curDur = Number(r[H["CurrentDurationMin"]] || 0);
        const critical = Number(r[H["CriticalFlag"]] || 0) === 1;
        const extended = Number(r[H["ExtendedFlag"]] || 0) === 1;
        
        const key = `${day}|${schedule}|${machine}`;
        const b = buckets[key] || (buckets[key] = {
          day, schedule, machine, 
          samples: 0, 
          missed: 0, 
          extended: 0,
          overdueSum: 0, 
          overdueValues: [], // Store values for percentile calculation
          durationMax: 0
        });
        
        b.samples++;
        if (critical) b.missed++;
        if (extended) b.extended++;
        if (Number.isFinite(overdue)) { 
          b.overdueSum += overdue; 
          b.overdueValues.push(overdue);
        }
        if (Number.isFinite(curDur) && curDur > b.durationMax) {
          b.durationMax = curDur;
        }
      } catch (e) {
        // Log but don't fail on individual row errors
        Logger.log(`Error processing trend row: ${e.message}`);
      }
    }
  }
  /**
   * Write aggregated output
   * @private
   */
  writeOutput_(buckets) {
    const out = [[
      "Day", "Schedule", "Machine", "Samples", 
      "Missed", "Extended", "AvgOverdueMin", 
      "P95OverdueMin", "MaxDurationMin"
    ]];
    
    // Convert buckets to sorted array
    const sortedBuckets = Object.values(buckets)
      .sort((a, b) => (a.day + a.schedule + a.machine).localeCompare(b.day + b.schedule + b.machine));
    
    // Process each bucket
    for (const b of sortedBuckets) {
      const avg = b.samples ? Math.round((b.overdueSum / b.samples) * 100) / 100 : "";
      
      let p95 = "";
      if (b.overdueValues.length > 0) {
        b.overdueValues.sort((x, y) => x - y);
        p95 = b.overdueValues[Math.floor(0.95 * (b.overdueValues.length - 1))];
      }
      
      out.push([
        b.day, b.schedule, b.machine, 
        b.samples, b.missed, b.extended, 
        avg, p95, b.durationMax
      ]);
    }
    
    // Write to sheet
    const sh = this.repo.ss.getSheetByName(this.outSheet) || this.repo.ss.insertSheet(this.outSheet);
    sh.clearContents();
    
    if (out.length > 1) {
      sh.getRange(1, 1, out.length, out[0].length).setValues(out);
      Logger.log(`Wrote ${out.length - 1} trend summary rows`);
    }
  }
  /**
   * Low-memory version for very large datasets
   * @public
   * @description Uses smaller batches and aggressive memory cleanup
   * for processing datasets that cause memory issues with standard method.
   */
  buildDailyLowMemory() {
    const sheet = this.repo.ss.getSheetByName(this.obsSheet);
    if (!sheet || sheet.getLastRow() < 2) return;
    
    // Store aggregated data in Properties temporarily for very large datasets
    const props = PropertiesService.getScriptProperties();
    const tempKey = "TREND_AGG_TEMP";
    
    try {
      // Clear any previous temp data
      props.deleteProperty(tempKey);
      
      // Get headers
      const headers = sheet.getRange(1, 1, 1, sheet.getLastColumn()).getValues()[0];
      const H = {};
      headers.forEach((h, i) => H[h] = i);
      
      const buckets = {};
      const totalRows = sheet.getLastRow();
      
      // Use smaller batches for low memory mode
      const SMALL_BATCH = 100;
      
      for (let startRow = 2; startRow <= totalRows; startRow += SMALL_BATCH) {
        const endRow = Math.min(startRow + SMALL_BATCH - 1, totalRows);
        const batchSize = endRow - startRow + 1;
        
        const batchData = sheet.getRange(startRow, 1, batchSize, sheet.getLastColumn()).getValues();
        this.processBatch_(batchData, H, buckets);
        
        // Aggressive memory cleanup
        if (startRow % 500 === 2) {
          SpreadsheetApp.flush();
          Utilities.sleep(100); // Brief pause to allow garbage collection
        }
      }
      
      this.writeOutput_(buckets);
      
    } finally {
      // Clean up temp storage
      try {
        props.deleteProperty(tempKey);
      } catch (e) {
        // Ignore cleanup errors
      }
    }
  }
}
/**
 * Logger for runtime history with automatic rotation.
 * Tracks execution metrics and errors.
 * @class
 */
class RuntimeHistoryLogger {
  /**
   * @param {SheetsRepo} repo
   * @param {typeof CONFIG} cfg
   */
  constructor(repo, cfg) {
    this.repo = repo; this.cfg = cfg;
    this.sheet = "RuntimeHistory";
    this.header = [
      "TimestampISO","OK","RunId","ObservationsLogged",
      "AlertsFired","HeaderChanged","RowErrors","Error"  // Add RowErrors column
    ];
    this.rotator = new LogRotator(repo, this.sheet, this.header, cfg);
    repo.ensureSheet(this.sheet, this.header);
  }
  /**
   * Appends a runtime history record
   * @param {Object} rec - Runtime record
   * @param {boolean} rec.ok - Success status
   * @param {string} rec.runId - Unique run identifier
   * @param {number} rec.obsCount - Observations logged
   * @param {number} rec.alertCount - Alerts fired
   * @param {boolean} rec.headerChanged - Header change detected
   * @param {number} rec.rowErrors - Row processing errors
   * @param {string} rec.error - Error message if failed
   */
  append(rec) {
    this.rotator.appendRow([
      this.repo.timestampISO(),
      String(!!rec.ok),
      String(rec.runId || ""),
      typeof rec.obsCount === "number" ? rec.obsCount : "",
      typeof rec.alertCount === "number" ? rec.alertCount : "",
      rec.headerChanged ? 1 : 0,
      typeof rec.rowErrors === "number" ? rec.rowErrors : "",  // Add this
      rec.error ? String(rec.error) : ""
    ]);
  }
}
/**
 * Unified notification manager for all alert types.
 * Formats and sends notifications via chat and email.
 * @class
 */
class NotificationManager {
  /**
   * @param {typeof CONFIG} config
   */
  constructor(config) {
    this.config = config;
    this.chatNotifier = new ChatNotifier(config.SERVICES.CHAT_WEBHOOK_URL);
  }
  /**
   * Send a missed schedule notification
   * @param {RowData} rowData - Schedule data
   * @returns {boolean} Success status
   */
  sendMissedSchedule(rowData) {
    const message = this.formatMissedSchedule_(rowData);
    return this.sendChat_(message);
  }
  /**
   * Send an extended execution notification
   * @param {RowData} rowData - Schedule data
   * @returns {boolean} Success status
   */
  sendExtendedExecution(rowData) {
    const message = this.formatExtendedExecution_(rowData);
    return this.sendChat_(message);
  }
  /**
   * Send a system failure notification
   * @param {Error} error - Error object
   * @param {boolean} [sendEmail=true] - Also send email notification
   * @description Sends notifications via chat and optionally email
   */
  sendSystemFailure(error, sendEmail = true) {
    const projectName = this.getProjectName_();
    const scriptId = ScriptApp.getScriptId();
    
    // Send chat notification
    const chatMessage = {
      text: `*Script Execution Failed*\n\n` +
            `An unhandled exception occurred in the ${projectName} script.\n\n` +
            `*Script ID:* \`${scriptId}\`\n` +
            `*Error Message:* \`${error.message}\`\n\n` +
            `*Stack Trace:*\n` +
            `\`\`\`\n${error.stack || "No stack trace available."}\n\`\`\``
    };
    this.sendChat_(chatMessage);
    
    // Send email if configured
    if (sendEmail && this.config.SERVICES.FAILURE_EMAIL_RECIPIENT) {
      this.sendFailureEmail_(error, projectName, scriptId);
    }
  }
  
  /**
   * Format missed schedule notification
   * @private
   */
  formatMissedSchedule_(rowData) {
    const message = [
      "Missed Schedule Alert:",
      `Schedule Name: ${rowData.scheduleName}`,
      `Machine Name: ${rowData.machineName}`,
      `Missed Schedule Time: ${this.formatDateTime_(rowData.lastExpectedRun)}`,
      `Minutes Overdue: ${rowData.overdueMinutes ?? "N/A"}`
    ].join("\n");
    return { text: message };
  }
  
  /**
   * Format extended execution notification
   * @private
   */
  formatExtendedExecution_(rowData) {
    const note = this.getDurationComparisonMessage_(
      rowData.currentDuration, 
      rowData.maxRunHoursExpected
    );
    
    const message = [
      "Extended Execution Alert:",
      `Schedule Name: ${rowData.scheduleName}`,
      `Machine Name: ${rowData.machineName}`,
      `Expected Start: ${this.formatDateTime_(rowData.lastExpectedRun)}`,
      `Current Status: ${rowData.currentStatus}`,
      `Current Duration: ${this.formatDuration_(rowData.currentDuration)}`,
      `Expected Duration: ${rowData.maxRunHoursExpected} hours`,
      note ? `Note: ${note}` : null
    ].filter(Boolean).join("\n");
    return { text: message };
  }
  
  /**
   * Send chat notification
   * @private
   */
  sendChat_(payload) {
    try {
      this.chatNotifier.send(payload);
      return true;
    } catch (error) {
      Logger.log(`Failed to send chat notification: ${error.message}`);
      return false;
    }
  }
  
  /**
   * Send failure email
   * @private
   */
  sendFailureEmail_(error, projectName, scriptId) {
    const subject = "Error in Google Apps Script: Schedule Monitor";
    const htmlBody = `
      <p>An unhandled error occurred in the <b>Schedule Monitor</b> script.</p>
      <p><b>Project Name:</b> <code>${projectName}</code></p>
      <p><b>Script ID:</b> <code>${scriptId}</code></p>
      <p><b>Error Message:</b> ${error.message}</p>
      <p><b>Stack Trace:</b></p>
      <pre>${error.stack || "No stack trace available."}</pre>
    `;
    
    try {
      GmailApp.sendEmail(
        this.config.SERVICES.FAILURE_EMAIL_RECIPIENT,
        subject,
        "",
        { name: `${projectName} Script`, htmlBody }
      );
    } catch (emailError) {
      Logger.log(`Failed to send failure email: ${emailError.message}`);
    }
  }
  
  /**
   * Format datetime for display
   * @private
   */
  formatDateTime_(dateInput) {
    if (!dateInput) return "N/A";
    try {
      const ss = SpreadsheetApp.getActiveSpreadsheet();
      const tz = ss.getSpreadsheetTimeZone();
      const d = new Date(dateInput);
      if (isNaN(d.getTime())) return "Invalid date";
      return Utilities.formatDate(d, tz, "MM/dd/yyyy - hh:mm a (z)");
    } catch (e) {
      return "Invalid date";
    }
  }
  
  /**
   * Format duration for display
   * @private
   */
  formatDuration_(mins) {
    if (!Number.isFinite(mins)) return "N/A";
    const h = Math.floor(mins / 60);
    const m = Math.floor(mins % 60);
    return h ? `${h} hours ${m} minutes` : `${m} minutes`;
  }
  
  /**
   * Get duration comparison message
   * @private
   */
  getDurationComparisonMessage_(currentMin, maxHours) {
    if (!currentMin || !maxHours) return "";
    const over = currentMin - maxHours * 60;
    return (over > 0) ? 
      `Execution has exceeded the maximum duration by ${this.formatDuration_(over)}.` : 
      "";
  }
  
  /**
   * Get project name
   * @private
   */
  getProjectName_() {
    try {
      return DriveApp.getFileById(ScriptApp.getScriptId()).getName();
    } catch (e) {
      return "Schedule Monitor";
    }
  }
}
/**
 * UI Helper for spreadsheet menu and dialogs.
 * Provides consistent UI operations and formatting.
 * @class
 */
class UIHelper {
  /**
   * Creates a UIHelper instance
   * @constructor
   * @description Initializes UI components and icon mappings
   */
  constructor() {
    this.ui = SpreadsheetApp.getUi();
    this.ss = SpreadsheetApp.getActiveSpreadsheet();
    this.tz = this.ss.getSpreadsheetTimeZone();
    
    // Icon constants for consistent UI
    this.ICONS = {
      RUN: '▶️',
      CHART: '📈',
      ARCHIVE: '🗄️',
      LIST: '📋',
      CLEAN: '🧹',
      DELETE: '🗑️',
      WARNING: '⚠️',
      TOOLS: '🔧',
      STATUS: '📊',
      CONFIG: '⚙️',
      SEARCH: '🔍',
      HELP: '❓',
      SUCCESS: '✅',
      ERROR: '❌',
      INFO: 'ℹ️'
    };
  }
  /**
   * Shows a simple alert dialog
   * @param {string} title - Dialog title
   * @param {(string|Array)} message - Message content
   * @param {string} [icon] - Optional icon key
   */
  alert(title, message, icon) {
    const formattedTitle = icon ? `${this.ICONS[icon]} ${title}` : title;
    const formattedMessage = Array.isArray(message) ? message.join('\n') : message;
    this.ui.alert(formattedTitle, formattedMessage, this.ui.ButtonSet.OK);
  }
  /**
   * Shows a confirmation dialog
   * @param {string} title - Dialog title
   * @param {(string|Array)} message - Message content
   * @param {string} [icon] - Optional icon key
   * @returns {boolean} True if user confirmed
   */
  confirm(title, message, icon) {
    const formattedTitle = icon ? `${this.ICONS[icon]} ${title}` : title;
    const formattedMessage = Array.isArray(message) ? message.join('\n') : message;
    const response = this.ui.alert(formattedTitle, formattedMessage, this.ui.ButtonSet.YES_NO);
    return response === this.ui.Button.YES;
  }
  /**
   * Build a formatted section for display
   * @param {string} header
   * @param {Object|Array} content
   * @returns {string}
   */
  formatSection(header, content) {
    const lines = [`${header}:`];
    
    if (Array.isArray(content)) {
      content.forEach(item => lines.push(`• ${item}`));
    } else if (typeof content === 'object') {
      Object.entries(content).forEach(([key, value]) => {
        lines.push(`• ${key}: ${value}`);
      });
    } else {
      lines.push(content);
    }
    
    return lines.join('\n');
  }
  /**
   * Format a timestamp for display
   * @param {number|Date} timestamp
   * @returns {string}
   */
  formatTimestamp(timestamp) {
    if (!timestamp) return 'Never';
    const date = timestamp instanceof Date ? timestamp : new Date(Number(timestamp));
    if (isNaN(date.getTime())) return 'Invalid';
    return Utilities.formatDate(date, this.tz, "yyyy-MM-dd HH:mm:ss z");
  }
  /**
   * Get sheet row count (excluding header)
   * @param {string} sheetName
   * @returns {number}
   */
  getRowCount(sheetName) {
    const sheet = this.ss.getSheetByName(sheetName);
    return sheet ? Math.max(0, sheet.getLastRow() - 1) : 0;
  }
  /**
   * Count archive sheets
   * @returns {number}
   */
  getArchiveCount() {
    return this.ss.getSheets().filter(s => s.getName().includes("_Archive_")).length;
  }
  /**
   * Builds the application menu
   * @public
   * @description Creates hierarchical menu with all system functions
   */
  buildMenu() {
    this.ui.createMenu(`${this.ICONS.STATUS} Schedule Monitor`)
      .addItem(`${this.ICONS.RUN} Run Monitor Now`, 'runScheduleMonitor')
      .addItem(`${this.ICONS.CHART} Build Trend Metrics`, 'buildTrendMetrics')
      .addSeparator()
      .addSubMenu(this.ui.createMenu(`${this.ICONS.ARCHIVE} Archive Management`)
        .addItem(`${this.ICONS.LIST} List All Archives`, 'showArchivesList')
        .addItem(`${this.ICONS.CLEAN} Clean Old Archives (>90 days)`, 'cleanupOldArchivesMenu')
        .addItem(`${this.ICONS.DELETE} Clean Old Archives (>30 days)`, 'cleanupOldArchives30Days')
        .addItem(`${this.ICONS.WARNING} Clean Old Archives (>7 days)`, 'cleanupOldArchives7Days'))
      .addSeparator()
      .addSubMenu(this.ui.createMenu(`${this.ICONS.TOOLS} Diagnostics`)
        .addItem(`${this.ICONS.STATUS} View System Status`, 'showSystemStatus')
        .addItem(`${this.ICONS.CONFIG} View Current Configuration`, 'showConfiguration')
        .addItem(`${this.ICONS.SEARCH} Check Header Bindings`, 'checkHeaderBindings'))
      .addSeparator()
      .addItem(`${this.ICONS.HELP} About`, 'showAbout')
      .addToUi();
  }  
  /**
   * Show archives list with formatted details
   */
  showArchives(archives) {
    if (archives.length === 0) {
      this.alert('Archive Status', 'No archived log sheets found.', 'LIST');
      return;
    }
    
    const lines = [`Found ${archives.length} archived log sheet(s):`, ''];
    
    archives.forEach(archive => {
      const dateStr = archive.created.replace(/_/g, ' ').replace(/-/g, ':');
      lines.push(
        `• ${archive.name.split('_')[0]}`,
        `  Created: ${dateStr}`,
        `  Rows: ${archive.rows}`,
        ''
      );
    });
    
    this.alert('Archived Logs', lines, 'LIST');
  } 
  /**
   * Show cleanup confirmation and result
   * @param {number} days
   * @param {string} severity - 'normal', 'warning', 'danger'
   * @returns {boolean} - Whether cleanup was performed
   */
  confirmAndCleanup(days, severity = 'normal') {
    const titles = {
      normal: 'Clean Old Archives',
      warning: 'Clean Old Archives',
      danger: 'Clean Recent Archives'
    };
    
    const messages = {
      normal: `This will delete all archive sheets older than ${days} days.`,
      warning: `This will delete all archive sheets older than ${days} days.`,
      danger: `WARNING: This will delete archive sheets older than just ${days} days.`
    };
    
    const icons = {
      normal: 'CLEAN',
      warning: 'DELETE',
      danger: 'WARNING'
    };
    
    const confirmed = this.confirm(
      titles[severity],
      [messages[severity], '', 'Do you want to continue?'],
      icons[severity]
    );
    
    if (confirmed) {
      const deleted = cleanupOldArchives(days);
      this.alert('Cleanup Complete', `Deleted ${deleted} old archive sheet(s).`, 'SUCCESS');
      return true;
    }
    
    return false;
  } 
  /**
   * Show system status dashboard
   */
  showSystemStatus() {
    const props = PropertiesService.getScriptProperties();
    const lastRun = props.getProperty(CONFIG.CORE.LAST_PROCESSED_PROPERTY);
    
    const status = {
      'Last Run': this.formatTimestamp(lastRun),
      'Next Run': 'Check time-based triggers'
    };
    
    const logMetrics = {
      [`Event Logs`]: `${this.getRowCount(CONFIG.LOGGING.SHEETS.EVENTS)} / ${CONFIG.LOGGING.MAX_ROWS} rows`,
      [`Observations`]: `${this.getRowCount(CONFIG.LOGGING.SHEETS.OBSERVATIONS)} / ${CONFIG.LOGGING.MAX_ROWS} rows`,
      [`Runtime History`]: `${this.getRowCount('RuntimeHistory')} / ${CONFIG.LOGGING.MAX_ROWS} rows`,
      [`Archive Sheets`]: this.getArchiveCount()
    };
    
    const message = [
      `${this.ICONS.STATUS} SYSTEM STATUS`,
      '',
      this.formatSection('STATUS', status),
      '',
      this.formatSection('LOG METRICS', logMetrics),
      '',
      `Total Sheets: ${this.ss.getNumSheets()} / 200 (Google limit)`
    ];
    
    this.alert('System Status', message, 'STATUS');
  } 
  /**
   * Show configuration details
   */
  showConfiguration() {
    const monitoring = {
      'Sheet Name': CONFIG.CORE.SHEET_NAME,
      'Debounce': `${CONFIG.CORE.DEBOUNCE_MINUTES} minutes`,
      'Alert TTL': `${CONFIG.ALERTS.TTL_MINUTES} minutes`
    };
    
    const logging = {
      'Observation Mode': CONFIG.LOGGING.MODE,
      'Max Rows Before Rotation': CONFIG.LOGGING.MAX_ROWS,
      'Event Log Sheet': CONFIG.LOGGING.SHEETS.EVENTS,
      'Observations Sheet': CONFIG.LOGGING.SHEETS.OBSERVATIONS,
      'Trends Sheet': CONFIG.LOGGING.SHEETS.TRENDS
    };
    
    const notifications = {
      'Chat Webhook': CONFIG.SERVICES.CHAT_WEBHOOK_URL ? 'Configured ✅' : 'Not set ❌',
      'Failure Email': CONFIG.SERVICES.FAILURE_EMAIL_RECIPIENT || 'Not set',
      'Archive to Drive': CONFIG.SERVICES.LOG_ARCHIVE_FOLDER_ID ? 'Enabled ✅' : 'Disabled ⚪'
    };
    
    const message = [
      `${this.ICONS.CONFIG} CURRENT CONFIGURATION`,
      '',
      this.formatSection('MONITORING', monitoring),
      '',
      this.formatSection('LOGGING', logging),
      '',
      this.formatSection('NOTIFICATIONS', notifications)
    ];
    
    this.alert('Configuration', message, 'CONFIG');
  }
  /**
   * Show header validation results
   */
  showHeaderValidation(sheet) {
    if (!sheet) {
      this.alert('Error', `Sheet "${CONFIG.CORE.SHEET_NAME}" not found!`, 'ERROR');
      return;
    }
    
    const validator = new HeaderValidator(sheet, CONFIG);
    const validation = validator.validate();
    const lines = [`${this.ICONS.SEARCH} HEADER BINDING STATUS`, ''];
    
    if (validation.valid) {
      lines.push('✅ All required headers are valid!', '', 'MAPPED COLUMNS:');
      
      const headers = validation.headers;
      Object.entries(CONFIG.COLUMNS).forEach(([name, index]) => {
        const actualHeader = headers[index - 1] || "(missing)";
        const expectedOptions = CONFIG.EXPECTED_HEADERS[index] || ["(no validation rules)"];
        lines.push(
          `• ${name}:`,
          `  Column ${index}: "${actualHeader}"`,
          `  Expected one of: ${expectedOptions.join(", ")}`,
          ''
        );
      });
    } else {
      lines.push('❌ Header validation failed!', '', 'ERRORS:');
      validation.errors.forEach(err => lines.push(`• ${err}`));
      
      if (validation.warnings.length > 0) {
        lines.push('', 'WARNINGS:');
        validation.warnings.forEach(warn => lines.push(`• ${warn}`));
      }
    }
    
    this.alert('Header Bindings', lines, 'SEARCH');
  } 
  /**
   * Show about dialog
   */
  showAbout() {
    const message = [
      `${this.ICONS.STATUS} SCHEDULE MONITOR v2.0`,
      '',
      'This script monitors scheduled tasks and sends alerts for:',
      '• Missed schedules (Critical alerts)',
      '• Extended executions (Duration alerts)',
      '',
      'Features:',
      `• Automatic log rotation at ${CONFIG.LOGGING.MAX_ROWS} rows`,
      `• Alert deduplication (${CONFIG.ALERTS.TTL_MINUTES} min TTL)`,
      '• Trend analysis and metrics',
      '• Header change detection',
      '',
      'Author: Emily Cabaniss',
      'Refactored with archive-to-sheet rotation'
    ];
    
    this.alert('About Schedule Monitor', message, 'HELP');
  }
}

// ------- HELPERS --------------------------------------------------------------
/**
 * Computes overdue minutes from raw cell value.
 * @function
 * @param {*} raw - Raw cell value
 * @returns {(number|null)} Overdue minutes or null
 * @description Handles both minute values and fractional day values.
 * Values < 1 are treated as days and converted to minutes.
 */
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

// ------- DATA MAPPING AND VALIDATION ------------------------------------------
/**
 * Extracts and validates row data from spreadsheet row.
 * @function
 * @param {Array} row - Spreadsheet row values
 * @returns {RowData} Extracted and normalized row data
 * @description Maps column indices to named properties with
 * type conversion and default values.
 */

function getRowData(row) {
  const cols = CONFIG.COLUMNS;
  const num = (v) => {
    const n = Number(v);
    return Number.isFinite(n) ? n : 0;
  };
  
  return {
    scheduleName: String(row[cols.SCHEDULE_NAME - 1] || "").trim() || "N/A",
    machineName: String(row[cols.MACHINE - 1] || "").trim() || "N/A",
    ifRanToday: String(row[cols.IF_RAN_TODAY - 1] || "").trim() || "N/A",
    lastExpectedRun: row[cols.LAST_EXPECTED_RUN - 1] || null,
    currentDuration: num(row[cols.CURRENT_DURATION - 1]),
    maxRunHoursExpected: num(row[cols.MAX_RUN_HOURS - 1]),
    currentStatus: String(row[cols.CURRENT_STATUS - 1] || "").trim() || "N/A",
    overdueMinutes: computeOverdueMinutes(row[cols.NOW_DIFF - 1]),
    notRanReason: String(row[cols.NOT_RAN_REASON - 1] || "").trim()
  };
}
/**
 * Extracts alert flags from spreadsheet row.
 * @function
 * @param {Array} row - Spreadsheet row values
 * @returns {Object} Alert flags
 * @returns {boolean} returns.critical - Critical alert triggered
 * @returns {boolean} returns.extended - Extended execution alert triggered
 * @description Case-insensitive comparison against configured values.
 */
function getAlertFlags(row) {
  const cols = CONFIG.COLUMNS;
  const normalize = s => String(s || "").trim().toUpperCase();
  
  return {
    critical: normalize(row[cols.CRITICAL_ITEM_ALERT - 1]) === CONFIG.ALERTS.CRITICAL_VALUE.toUpperCase(),
    extended: normalize(row[cols.EXTENDED_EXECUTION_ALERT - 1]) === CONFIG.ALERTS.EXTENDED_VALUE.toUpperCase()
  };
}
function normalizeHeader_(s) {
  // trim, lowercase, collapse spaces, strip punctuation
  return String(s || "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, " ")
    .replace(/[^a-z0-9 ]+/g, "");
}

// ------- IN-FLIGHT GUARD ------------------------------------------------------
/**
 * Prevents concurrent runs using cache-based guard.
 * @function
 * @private
 * @param {Function} fn - Function to execute
 * @returns {*} Result of fn execution
 * @description Uses 3-minute TTL cache entry to prevent
 * thundering herd problem when multiple triggers fire.
 */
function withInFlightGuard_(fn) {
  const cache = CacheService.getScriptCache();
  const key = CacheKeyBuilder.inFlightGuard();
  const existing = cache.get(key);
  if (existing) { Logger.log("Another run is in flight. Exiting early."); return; }
  try {
    cache.put(key, String(Date.now()), 180);
    return fn();
  } finally {
    try { cache.remove(key); } catch (_) {}
  }
}
// ------- PUBLIC API -----------------------------------------------------------
/**
 * Main trigger entry point for schedule monitoring.
 * Should be called by time-based trigger (e.g., every 5 minutes).
 * @function
 * @public
 * @returns {boolean|undefined} Success status or undefined if skipped
 * @description Implements:
 * - Script lock to prevent concurrent runs
 * - Debounce check to respect minimum interval
 * - In-flight guard for additional concurrency protection
 * @example
 * // Set up time-based trigger in Apps Script:
 * // Edit > Current project's triggers > Add trigger
 * // Function: runScheduleMonitor
 * // Time-based: Every 5 minutes
 */
function runScheduleMonitor() {
  const lock = LockService.getScriptLock();
  try {
    lock.waitLock(30000);
  } catch (_) {
    Logger.log("Lock busy");
    return;
  }

  const props = PropertiesService.getScriptProperties();
  const last = Number(props.getProperty(CONFIG.CORE.LAST_PROCESSED_PROPERTY) || 0);
  const now = Date.now();
  
  // Debounce check
  if (now - last < CONFIG.CORE.DEBOUNCE_MINUTES * 60 * 1000) {
    lock.releaseLock();
    return;
  }

  try {
    return withInFlightGuard_(() => {
      const monitor = new ScheduleMonitor(CONFIG);
      return monitor.execute();
    });
  } finally {
    lock.releaseLock();
  }
}
/**
 * Builds daily trend metrics from observations.
 * Should be called by daily trigger (e.g., 2:00 AM).
 * @function
 * @public
 * @description Aggregates observations into daily summaries with:
 * - Sample counts
 * - Alert counts
 * - Average and 95th percentile overdue times
 * - Maximum execution durations
 * @example
 * // Set up daily trigger in Apps Script:
 * // Function: buildTrendMetrics
 * // Time-based: Day timer, 2am-3am
 */
function buildTrendMetrics() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const repo = new SheetsRepo(ss, ss.getSpreadsheetTimeZone());
  new TrendAggregator(repo, CONFIG.LOGGING.SHEETS.OBSERVATIONS, CONFIG.LOGGING.SHEETS.TRENDS).buildDaily();
}

// ------- COMPATIBILITY WRAPPERS -----------------------------------------------
/** Event log wrapper retained for compatibility (uses EventsLogger) */
function logEvent(eventType, scheduleName, machineName, details) {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const repo = new SheetsRepo(ss, ss.getSpreadsheetTimeZone());
  const events = new EventsLogger(repo, CONFIG.LOGGING.SHEETS.EVENTS, CONFIG);
  events.log(eventType, scheduleName, machineName, details);
}
/** Old signature wrapper — delegates to ChatNotifier */
function sendGoogleChatNotification(payload) {
  new ChatNotifier(CONFIG.SERVICES.CHAT_WEBHOOK_URL).send(payload);
}

// ------- USER INTERFACE -------------------------------------------------------
// Global UI helper instance (lazy-loaded)
let _uiHelper = null;

/**
 * Get or create UI helper instance
 * @returns {UIHelper}
 */
function getUIHelper() {
  if (!_uiHelper) {
    _uiHelper = new UIHelper();
  }
  return _uiHelper;
}
/**
 * Creates custom menu when spreadsheet opens.
 * @function
 * @public
 * @description Auto-triggered by Google Sheets on open.
 * Creates hierarchical menu with all user functions.
 */
function onOpen() {
  getUIHelper().buildMenu();
}
/**
 * Lists all archived log sheets.
 * @function
 * @public
 * @returns {Array<Object>} Array of archive information
 * @returns {string} returns[].name - Archive sheet name
 * @returns {string} returns[].created - Creation timestamp
 * @returns {number} returns[].rows - Number of rows
 */
function showArchivesList() {
  const archives = listArchivedLogs();
  getUIHelper().showArchives(archives);
}
/**
 * Deletes archive sheets older than specified days.
 * @function
 * @public
 * @param {number} daysOld - Age threshold in days
 * @returns {number} Number of sheets deleted
 * @description Parses timestamps from archive sheet names
 * and deletes those exceeding age threshold.
 */
function cleanupOldArchivesMenu() {
  getUIHelper().confirmAndCleanup(90, 'normal');
}
/**
 * Deletes archive sheets older than specified days.
 * @function
 * @public
 * @param {number} daysOld - Age threshold in days
 * @returns {number} Number of sheets deleted
 * @description Parses timestamps from archive sheet names
 * and deletes those exceeding age threshold.
 */
function cleanupOldArchives30Days() {
  getUIHelper().confirmAndCleanup(30, 'warning');
}
/**
 * Deletes archive sheets older than specified days.
 * @function
 * @public
 * @param {number} daysOld - Age threshold in days
 * @returns {number} Number of sheets deleted
 * @description Parses timestamps from archive sheet names
 * and deletes those exceeding age threshold.
 */
function cleanupOldArchives7Days() {
  getUIHelper().confirmAndCleanup(7, 'danger');
}
/**
 * Show system status
 */
function showSystemStatus() {
  getUIHelper().showSystemStatus();
}
/**
 * Show current configuration
 */
function showConfiguration() {
  getUIHelper().showConfiguration();
}
/**
 * Check header bindings
 */
function checkHeaderBindings() {
  const sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(CONFIG.CORE.SHEET_NAME);
  getUIHelper().showHeaderValidation(sheet);
}
/**
 * Show about dialog
 */
function showAbout() {
  getUIHelper().showAbout();
}
/**
 * List all archived log sheets
 * @returns {Array<{name: string, created: string, rows: number}>}
 */
function listArchivedLogs() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const archives = [];
  
  ss.getSheets().forEach(sheet => {
    const name = sheet.getName();
    if (name.includes("_Archive_")) {
      const parts = name.split("_Archive_");
      archives.push({
        name: name,
        created: parts[1] || "",
        rows: sheet.getLastRow()
      });
    }
  });
  
  // Sort by creation date (newest first)
  archives.sort((a, b) => b.created.localeCompare(a.created));
  
  return archives;
}
/**
 * Clean up old archive sheets
 * @param {number} daysOld - Delete archives older than this many days
 * @returns {number} - Number of sheets deleted
 */
function cleanupOldArchives(daysOld) {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const cutoffDate = new Date();
  cutoffDate.setDate(cutoffDate.getDate() - daysOld);
  
  let deletedCount = 0;
  const sheetsToDelete = [];
  
  ss.getSheets().forEach(sheet => {
    const name = sheet.getName();
    if (name.includes("_Archive_")) {
      try {
        // Extract timestamp from name (format: SheetName_Archive_2024-01-01_12-00-00-05-00)
        const timestampPart = name.split("_Archive_")[1];
        if (timestampPart) {
          // Convert timestamp format back to parseable date
          const dateStr = timestampPart.replace(/_/g, 'T').replace(/-(\d{2})-(\d{2})$/, ':$1:$2');
          const archiveDate = new Date(dateStr);
          
          if (!isNaN(archiveDate.getTime()) && archiveDate < cutoffDate) {
            sheetsToDelete.push(sheet);
          }
        }
      } catch (e) {
        Logger.log(`Error parsing archive date for ${name}: ${e.message}`);
      }
    }
  });
  
  // Delete the sheets
  sheetsToDelete.forEach(sheet => {
    try {
      ss.deleteSheet(sheet);
      deletedCount++;
      Logger.log(`Deleted archive: ${sheet.getName()}`);
    } catch (e) {
      Logger.log(`Failed to delete ${sheet.getName()}: ${e.message}`);
    }
  });
  
  return deletedCount;
}
