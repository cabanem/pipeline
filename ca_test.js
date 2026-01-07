/* eslint-disable no-var */
"use strict";

/**
 * @file Test Harness for Schedule Monitor v3.1
 * @description 
 * A self-contained testing suite. Creates ephemeral environments (sheets/properties)
 * to verify core logic without affecting production data.
 * * Usage: Select 'runAllTests' from the editor dropdown and click Run.
 * @author Emily Cabaniss
 */

/**
 * MAIN TEST RUNNER
 * Executes all defined test cases and reports pass/fail status.
 */
function runAllTests() {
  const harness = new TestContext();
  console.log("🚦 STARTING TEST SUITE...");

  try {
    harness.setup();

    // --- EXECUTE TEST CASES ---
    test_Headers_HappyPath(harness);
    test_Headers_ColumnShift(harness);
    test_Deduplication_Logic(harness); // TC-07
    test_CriticalAlert_Logic(harness);
    test_Infrastructure_Policies(harness);
    
    console.log("✅ ALL TESTS PASSED");
    
  } catch (e) {
    console.error("❌ TEST SUITE FAILED");
    console.error(e.message);
    // Re-throw to make the execution show as 'Failed' in the Apps Script dashboard
    throw e; 
  } finally {
    harness.teardown();
  }
}

// ===================================================================================
// TEST CONTEXT (The Mock Environment)
// ===================================================================================

class TestContext {
  constructor() {
    this.ss = SpreadsheetApp.getActiveSpreadsheet();
    // Use a specific prefix so we can easily identify leftover test artifacts
    this.sheetName = `TEST_SRC_${Date.now()}`; 
    this.sheet = null; // Reference to the main source sheet
    this.propService = PropertiesService.getScriptProperties();
    
    this.usedPropKeys = []; 
    this.createdSheets = []; // NEW: Track all sheets for cleanup
  }

  /**
   * Creates a fresh environment:
   * 1. A new source sheet with valid headers
   * 2. Registers it for cleanup
   */
  setup() {
    console.log(`Creating temp source sheet: ${this.sheetName}`);
    this.sheet = this.ss.insertSheet(this.sheetName);
    this.createdSheets.push(this.sheet); // Register for teardown

    // Build a valid header row based on CONFIG
    // We create a sparse array up to column 30 (AC)
    const headers = new Array(30).fill("");
    
    // Inject required headers from CONFIG to ensure validation passes
    // Note: arrays are 0-indexed, CONFIG cols are 1-indexed
    Object.entries(CONFIG.EXPECTED_HEADERS).forEach(([colIndex, allowedNames]) => {
      headers[colIndex - 1] = allowedNames[0]; // Use the first allowed name
    });

    // Add some dummy headers for context
    headers[CONFIG.COLUMNS.NOW_DIFF - 1] = "Now Diff (min)";
    
    this.sheet.getRange(1, 1, 1, headers.length).setValues([headers]);
    SpreadsheetApp.flush();
  }

  /**
   * Cleans up ALL sheets and properties created during the test run
   */
  teardown() {
    console.log("Cleaning up...");
    
    // 1. Delete ALL Test Sheets
    this.createdSheets.forEach(sheet => {
      try {
        console.log(`Deleting sheet: ${sheet.getName()}`);
        this.ss.deleteSheet(sheet);
      } catch (e) {
        console.warn(`Could not delete ${sheet.getName()}: ${e.message}`);
      }
    });

    // 2. Delete Test Properties
    if (this.usedPropKeys.length > 0) {
      this.usedPropKeys.forEach(key => this.propService.deleteProperty(key));
      console.log(`Deleted ${this.usedPropKeys.length} test properties.`);
    }
  }

  /**
   * Helper: wrapper for PropertiesService that tracks keys for deletion
   */
  setProperty(key, value) {
    const testKey = `TEST::${key}`;
    this.propService.setProperty(testKey, value);
    this.usedPropKeys.push(testKey);
  }

  getProperty(key) {
    return this.propService.getProperty(`TEST::${key}`);
  }
  
  /**
   * Simple Assertion Helper
   */
  assert(condition, message) {
    if (!condition) {
      throw new Error(`Assertion Failed: ${message}`);
    }
    console.log(`   [PASS] ${message}`);
  }
}


// ===================================================================================
// TEST CASES
// ===================================================================================

/** * TC-01: test_Headers_HappyPath
 * Verifies that the correct headers pass validation.
 */
function test_Headers_HappyPath(h) {
  console.log("\n🧪 Running: test_Headers_HappyPath");
  
  const repo = new SheetsRepo(h.ss);
  const result = validateHeaders(repo, h.sheetName, CONFIG.EXPECTED_HEADERS);
  
  h.assert(result.valid === true, "Validation should pass for correct headers");
  h.assert(result.errors.length === 0, "Error list should be empty");
}

/** * TC-02: test_Headers_ColumnShift
 * Verifies that shifting columns causes validation failure.
 */
function test_Headers_ColumnShift(h) {
  console.log("\n🧪 Running: test_Headers_ColumnShift");
  
  // Shift columns by inserting a new column at A
  h.sheet.insertColumns(1);
  
  const repo = new SheetsRepo(h.ss);
  const result = validateHeaders(repo, h.sheetName, CONFIG.EXPECTED_HEADERS);
  
  h.assert(result.valid === false, "Validation should fail when columns are shifted");
  h.assert(result.errors.length > 0, "Should return specific errors");
  
  // Restore sheet state for next tests (delete the inserted column)
  h.sheet.deleteColumn(1);
}

/** * TC-07: test_Deduplication_Logic
 * Verifies Alert Deduplication Logic (The "Server" Memory)
 */
function test_Deduplication_Logic(h) {
  console.log("\n🧪 Running: test_Deduplication_Logic (TC-07)");

  // 1. Mock the Property Service
  const mockProps = {
    getProperty: (k) => h.getProperty(k),
    setProperty: (k, v) => h.setProperty(k, v),
    setProperties: (obj) => {
      Object.entries(obj).forEach(([k,v]) => h.setProperty(k, v));
    }
  };

  // 2. Initialize Cache with 60 min TTL
  const dedupe = new AlertDedupeCache(mockProps, 60, {}); 

  const schedule = "Backup_DB";
  const machine = "Server_01";
  const alertType = "missed";

  // A. First Trigger -> Should Fire
  const result1 = dedupe.shouldFire(schedule, machine, alertType);
  h.assert(result1 === true, "First alert should fire");
  
  // Simulate the flush() at the end of execution
  dedupe.flush();

  // B. Immediate Retry -> Should NOT Fire
  const memoryReload = {};
  
  // FIX: Sanitize the inputs to match the Class logic
  // The class strips underscores, so we must too when looking up the key
  const safeSched = schedule.replace(/[^a-zA-Z0-9]/g, "");
  const safeMach = machine.replace(/[^a-zA-Z0-9]/g, "");
  const expectedKey = `alert::${alertType}::${safeSched}::${safeMach}`;
  
  memoryReload[expectedKey] = h.getProperty(expectedKey);

  const dedupeRun2 = new AlertDedupeCache(mockProps, 60, memoryReload);
  const result2 = dedupeRun2.shouldFire(schedule, machine, alertType);
  h.assert(result2 === false, "Immediate retry should be blocked by TTL");

  // C. Expired TTL -> Should Fire
  // We manually hack the property store to make the last run 61 minutes ago
  const oldTime = Date.now() - (61 * 60 * 1000);
  h.setProperty(expectedKey, String(oldTime));
  
  // Re-initialize with the old timestamp
  const memoryReloadOld = {};
  memoryReloadOld[expectedKey] = String(oldTime);
  
  const dedupeRun3 = new AlertDedupeCache(mockProps, 60, memoryReloadOld);
  const result3 = dedupeRun3.shouldFire(schedule, machine, alertType);
  h.assert(result3 === true, "Expired TTL should allow alert to fire again");
}

/** * TC-04: test_CriticalAlert_Logic
 * Verifies Critical Alert Business Logic
 * Direct checks against row processing logic
 */
function test_CriticalAlert_Logic(h) {
  console.log("\n🧪 Running: test_CriticalAlert_Logic");
  
  // Write a "Critical" row to the test sheet
  // We need to map values to the specific column indices in CONFIG
  const rowData = new Array(30).fill("");
  rowData[CONFIG.COLUMNS.SCHEDULE_NAME - 1] = "Critical_Job";
  rowData[CONFIG.COLUMNS.MACHINE - 1] = "Test_Machine";
  rowData[CONFIG.COLUMNS.CRITICAL_ALERT - 1] = CONFIG.ALERTS.CRITICAL_VALUE; // "NOT RAN ALERT"
  rowData[CONFIG.COLUMNS.NOW_DIFF - 1] = 20; // 20 mins overdue (Threshold is 15)
  
  h.sheet.getRange(2, 1, 1, rowData.length).setValues([rowData]);
  
  // Read it back using the Repo
  const repo = new SheetsRepo(h.ss);
  // We use readData but point it to our test sheet
  const data = repo.readData(h.sheetName);
  const testRow = data[1]; // Index 1 is the data row (0 is header)
  
  // Parse it
  const parsed = getRowData(testRow);
  const flags = getAlertFlags(testRow);
  
  h.assert(flags.critical === true, "Critical flag should be true");
  h.assert(parsed.overdueMinutes === 20, "Overdue minutes should be 20");
  
  // Logic check
  const shouldAlert = flags.critical && (parsed.overdueMinutes >= CONFIG.ALERTS.MIN_OVERDUE_MINUTES);
  h.assert(shouldAlert === true, "Logic should dictate an alert for this row");
}

/** * TC-I: test_Infrastructure_Policies
 * Infrastructure testing: Verifies Sheet Creation Policies.
 * 1. Loggers MUST create sheets if missing (Self-Healing).
 * 2. Sheets MUST be created with the exact headers defined in the classes.
 * 3. Readers (Source) MUST NOT create sheets if missing (Read-Only Safety).
 */
function test_Infrastructure_Policies(h) {
  console.log("\n🧪 Running: test_Infrastructure_Policies");
  const repo = new SheetsRepo(h.ss);
  const ts = Date.now();

  // Helper to verify headers match exactly
  const verifyHeaders = (sheetName, expectedHeaders) => {
    const sh = h.ss.getSheetByName(sheetName);
    h.assert(sh !== null, `Sheet ${sheetName} should have been created`);
    h.createdSheets.push(sh); // Register for teardown

    const actual = sh.getRange(1, 1, 1, sh.getLastColumn()).getValues()[0];
    h.assert(
      JSON.stringify(actual) === JSON.stringify(expectedHeaders),
      `Headers for ${sheetName} must match class definition`
    );
  };

  // --- SUB-TEST 1: EventLogs Creation ---
  const eventSheetName = `TEST_Events_${ts}`;
  const eventLogger = new EventsLogger(repo, eventSheetName);
  
  eventLogger.log("Test", "A", "B", "Details");
  eventLogger.flush(); // Should trigger creation
  
  verifyHeaders(eventSheetName, eventLogger.header);


  // --- SUB-TEST 2: Observations Creation ---
  const obsSheetName = `TEST_Obs_${ts}`;
  const obsLogger = new ObservationsLogger(repo, obsSheetName);
  
  // Create dummy data to flush
  const dummyRow = {
    scheduleName: "Test", machineName: "Test", ifRanToday: "Yes",
    lastExpectedRun: new Date(), overdueMinutes: 0, currentStatus: "OK",
    currentDuration: 0, maxRunHoursExpected: 1, notRanReason: ""
  };
  obsLogger.append(1, dummyRow, {critical:false, extended:false});
  obsLogger.flush(); // Should trigger creation

  verifyHeaders(obsSheetName, obsLogger.header);


  // --- SUB-TEST 3: TrendMetrics Creation ---
  const trendSheetName = `TEST_Trends_${ts}`;
  // To test trends, we need the OBSERVATIONS sheet (obsSheetName) to exist and have data
  // (We created and populated it in Sub-Test 2, so we reuse it here)
  
  const trendAgg = new TrendAggregator(repo, obsSheetName, trendSheetName);
  trendAgg.buildDaily(); // Should read Obs, calc stats, and create Trends sheet

  // Trend header is defined inside buildDaily, so we define the expectation here
  const expectedTrendHeader = ["Day","Schedule","Machine","Samples","Missed","Extended","AvgOverdue"];
  verifyHeaders(trendSheetName, expectedTrendHeader);


  // --- SUB-TEST 4: Source Safety Check ---
  // Ensure we NEVER auto-create the source logic sheet if it's missing
  const missingSource = "NON_EXISTENT_SOURCE_SHEET";
  const data = repo.readData(missingSource);
  
  h.assert(data.length === 0, "Reading missing source should return empty");
  const checkSh = h.ss.getSheetByName(missingSource);
  h.assert(checkSh === null, "System must NEVER auto-create the Source Logic sheet");
}
