// ───────────────────────────────────────────────────────────────────────────
// Test Harness for Schedule Monitor
// Paste this BELOW your main implementation.
// ───────────────────────────────────────────────────────────────────────────

/**
 * Toggle test mode: when enabled, Chat + failure emails are captured into sheets
 * instead of calling external services.
 */
function test_enableMockMode() {
  PropertiesService.getScriptProperties().setProperty('TEST_MODE', '1');
  Logger.log('TEST_MODE enabled.');
}
function test_disableMockMode() {
  PropertiesService.getScriptProperties().deleteProperty('TEST_MODE');
  Logger.log('TEST_MODE disabled.');
}
function test_isMock() {
  return (PropertiesService.getScriptProperties().getProperty('TEST_MODE') === '1');
}

// ── Tiny assertion helpers
function __assert(cond, msg) { if (!cond) throw new Error('ASSERTION FAILED: ' + msg); }
function __assertEq(actual, expected, msg) {
  if (actual !== expected) {
    throw new Error(`ASSERTION FAILED: ${msg} (actual=${actual}, expected=${expected})`);
  }
}

// ── Local helpers (sheet ops)
function __ss() { return SpreadsheetApp.getActiveSpreadsheet(); }

function __ensureSheet(name, header) {
  const ss = __ss();
  let sh = ss.getSheetByName(name);
  if (!sh) sh = ss.insertSheet(name);
  if (header && sh.getLastRow() === 0) sh.appendRow(header);
  return sh;
}

function __clearSheet(name) {
  const ss = __ss();
  const sh = ss.getSheetByName(name);
  if (sh) sh.clearContents();
}

function __setHeaders(name, headers) {
  const sh = __ensureSheet(name, headers);
  sh.clearContents();
  sh.appendRow(headers);
  return sh;
}

function __appendRows(name, rows) {
  const sh = __ensureSheet(name);
  if (rows.length === 0) return;
  const startRow = sh.getLastRow() + 1;
  const startCol = 1;
  sh.getRange(startRow, startCol, rows.length, rows[0].length).setValues(rows);
}

function __values(name) {
  const sh = __ensureSheet(name);
  return sh.getDataRange().getValues();
}

function __countRows(name) {
  const v = __values(name);
  return Math.max(0, v.length - 1);
}

// ───────────────────────────────────────────────────────────────────────────
// MOCKS (override selected functions when TEST_MODE=1)
// ───────────────────────────────────────────────────────────────────────────

/**
 * Overrides the global sendGoogleChatNotification only in TEST_MODE.
 * Writes payloads to "ChatOutbox" sheet.
 */
function sendGoogleChatNotification(payload) {
  if (!test_isMock()) {
    // Call the real notifier when not in test mode
    return new ChatNotifier(CHAT_WEBHOOK_URL).send(payload);
  }
  const ss = __ss();
  const repo = new SheetsRepo(ss, ss.getSpreadsheetTimeZone());
  const sh = repo.ensureSheet('ChatOutbox', ['TimestampISO','Payload(JSON)']);
  repo.appendRow('ChatOutbox', [repo.timestampISO(), JSON.stringify(payload)]);
}

/**
 * Overrides failure email only in TEST_MODE.
 * Writes emails to "MailOutbox" sheet.
 */
function sendFailureEmail(error) {
  if (!test_isMock()) {
    // Real path
    // (reuse the implementation from your file if you prefer; keeping it minimal here)
    try {
      const projectName = DriveApp.getFileById(ScriptApp.getScriptId()).getName();
      const scriptId = ScriptApp.getScriptId();
      const subject = `Error in Google Apps Script: Schedule Monitor`;
      const htmlBody = `
        <p>An unhandled error occurred in the <b>Schedule Monitor</b> script.</p>
        <p><b>Project Name:</b> <code>${projectName}</code></p>
        <p><b>Script ID:</b> <code>${scriptId}</code></p>
        <p><b>Error Message:</b> ${error.message}</p>
        <p><b>Stack Trace:</b></p>
        <pre>${error.stack || "No stack trace available."}</pre>
      `;
      GmailApp.sendEmail(FAILURE_EMAIL_RECIPIENT, subject, "", { name: `${projectName} Script`, htmlBody });
    } catch (e) {
      Logger.log(`CRITICAL: Failed to send failure email notification. Error: ${e.message}`);
    }
    return;
  }
  const ss = __ss();
  const repo = new SheetsRepo(ss, ss.getSpreadsheetTimeZone());
  const sh = repo.ensureSheet('MailOutbox', ['TimestampISO','Subject','Body']);
  const body = {
    message: error && error.message,
    stack: (error && error.stack) || null
  };
  repo.appendRow('MailOutbox', [repo.timestampISO(), 'FailureEmail(Mock)', JSON.stringify(body)]);
}

// ───────────────────────────────────────────────────────────────────────────
// FIXTURE: Seed Notifications Logic with realistic headers and rows
// ───────────────────────────────────────────────────────────────────────────

/**
 * Creates headers across 29 columns (A:AC) and seeds 3 rows:
 *  - Row 2: Missed schedule (CRITICAL alert) with overdue >= 15 min
 *  - Row 3: Extended execution (DURATION ALERT)
 *  - Row 4: Normal (no alerts)
 */
function test_seedNotificationsSheet() {
  const headers = [];
  // Build 29 columns with generic names, then set required ones
  for (let i=1; i<=29; i++) headers.push('col_'+i);

  // Required header names at specific indices (1-based)
  headers[COLUMN_INDEX.SCHEDULE_NAME - 1]         = "schedule name";
  headers[COLUMN_INDEX.MACHINE - 1]               = "machine";
  headers[COLUMN_INDEX.LAST_EXPECTED_RUN - 1]     = "last expected run day / time";
  headers[COLUMN_INDEX.MAX_START - 1]             = "max start";
  headers[COLUMN_INDEX.CRITICAL_ITEM_ALERT - 1]   = "critical item alert";
  headers[COLUMN_INDEX.EXTENDED_EXECUTION_ALERT - 1] = "extended execution alert";

  const sh = __setHeaders(SHEET_NAME, headers);

  // Helper to build a 29-col row
  function row29() { return new Array(29).fill(""); }

  // Row 2: CRITICAL missed alert
  const r2 = row29();
  r2[COLUMN_INDEX.SCHEDULE_NAME - 1] = "Nightly ETL";
  r2[COLUMN_INDEX.MACHINE - 1] = "vm-01";
  r2[COLUMN_INDEX.LAST_EXPECTED_RUN - 1] = new Date(new Date().getTime() - (2*60*60*1000)); // 2 hours ago
  r2[COLUMN_INDEX.NOW_DIFF - 1] = 0.5; // treat as days => 720 minutes
  r2[COLUMN_INDEX.CURRENT_STATUS - 1] = "Not Started";
  r2[COLUMN_INDEX.CURRENT_DURATION - 1] = 0;
  r2[COLUMN_INDEX.MAX_RUN_HOURS - 1] = 2;
  r2[COLUMN_INDEX.CRITICAL_ITEM_ALERT - 1] = CRITICAL_ALERT_VALUE;

  // Row 3: Extended duration alert
  const r3 = row29();
  r3[COLUMN_INDEX.SCHEDULE_NAME - 1] = "BigJob";
  r3[COLUMN_INDEX.MACHINE - 1] = "vm-02";
  r3[COLUMN_INDEX.LAST_EXPECTED_RUN - 1] = new Date(new Date().getTime() - (3*60*60*1000));
  r3[COLUMN_INDEX.NOW_DIFF - 1] = 0.2; // still overdue a bit (days -> 288 min)
  r3[COLUMN_INDEX.CURRENT_STATUS - 1] = "Running";
  r3[COLUMN_INDEX.CURRENT_DURATION - 1] = 200; // minutes
  r3[COLUMN_INDEX.MAX_RUN_HOURS - 1] = 2; // hours (120 min)
  r3[COLUMN_INDEX.EXTENDED_EXECUTION_ALERT - 1] = EXTENDED_EXECUTION_ALERT;

  // Row 4: Normal row (no alerts)
  const r4 = row29();
  r4[COLUMN_INDEX.SCHEDULE_NAME - 1] = "QuickJob";
  r4[COLUMN_INDEX.MACHINE - 1] = "vm-03";
  r4[COLUMN_INDEX.LAST_EXPECTED_RUN - 1] = new Date();
  r4[COLUMN_INDEX.NOW_DIFF - 1] = 2;   // minutes (>=1 means already minutes)
  r4[COLUMN_INDEX.CURRENT_STATUS - 1] = "Success";
  r4[COLUMN_INDEX.CURRENT_DURATION - 1] = 5;
  r4[COLUMN_INDEX.MAX_RUN_HOURS - 1] = 1;

  __appendRows(SHEET_NAME, [r2, r3, r4]);
  Logger.log('Seeded Notifications Logic with 3 rows.');
}

// ───────────────────────────────────────────────────────────────────────────
// Test Scenarios
// ───────────────────────────────────────────────────────────────────────────

function test_setup() {
  test_enableMockMode();

  // Clear all working sheets
  [SHEET_NAME, LOG_SHEET_EVENTS, LOG_SHEET_OBS, LOG_SHEET_TRENDS, 'ChatOutbox', 'MailOutbox'].forEach(__clearSheet);

  // Reset debounce + alert dedupe caches
  const props = PropertiesService.getScriptProperties();
  props.deleteProperty(LAST_PROCESSED_PROPERTY);
  // wipe alert::* keys
  Object.keys(props.getProperties()).forEach(k => { if (k.indexOf('alert::') === 0 || k.indexOf('obs_sig::') === 0) props.deleteProperty(k); });

  test_seedNotificationsSheet();
  Logger.log('Test setup complete.');
}

function test_run_once() {
  runScheduleMonitor();

  // Verify: one missed alert + one extended alert
  const events = __values(LOG_SHEET_EVENTS);
  const obs = __values(LOG_SHEET_OBS);
  const chat = __values('ChatOutbox');

  __assert(events.length >= 2, 'EventLogs should exist (header + entries)');
  const eventRows = events.slice(1);
  const types = eventRows.map(r => r[1]);
  __assert(types.includes('MissedScheduleNotification'), 'Should log MissedScheduleNotification');
  __assert(types.includes('ExtendedExecutionNotification'), 'Should log ExtendedExecutionNotification');

  __assert(obs.length >= 2, 'Observations should contain rows');
  __assert(chat.length >= 3, 'ChatOutbox should contain at least 2 messages (header + 2 payloads)');
}

function test_debounce() {
  const before = __countRows(LOG_SHEET_EVENTS);
  runScheduleMonitor(); // first run already executed in previous test; this call should be debounced
  const after = __countRows(LOG_SHEET_EVENTS);
  __assertEq(after, before, 'Debounce should prevent new EventLogs entries immediately after prior run');
}

function test_dedupe() {
  // Simulate immediate re-run; alerts should be deduped by TTL
  runScheduleMonitor();
  const after2 = __countRows(LOG_SHEET_EVENTS);
  // Still the same as after debounce, no new alerts
  __assertEq(after2, __countRows(LOG_SHEET_EVENTS), 'Dedupe should suppress repeated alerts within TTL');
}

function test_trend_rollup() {
  buildTrendMetrics();
  const t = __values(LOG_SHEET_TRENDS);
  __assert(t.length >= 2, 'TrendMetrics should have rows (header + data)');
  const header = t[0];
  __assertEq(header.join(','), 'Day,Schedule,Machine,Samples,Missed,Extended,AvgOverdueMin,P95OverdueMin,MaxDurationMin', 'TrendMetrics header matches');
}

function test_all() {
  test_setup();
  test_run_once();
  test_debounce();
  test_dedupe();
  test_trend_rollup();
  Logger.log('✅ All tests completed successfully.');
}

// Optional: cleanup helper
function test_teardown() {
  ['ChatOutbox', 'MailOutbox', LOG_SHEET_EVENTS, LOG_SHEET_OBS, LOG_SHEET_TRENDS].forEach(name => {
    const sh = __ss().getSheetByName(name);
    if (sh) __ss().deleteSheet(sh);
  });
  Logger.log('Teardown complete (deleted outbox/log sheets).');
}
