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
    this.sheetName = `TEST_ENV_${Date.now()}`;
    this.sheet = null;
    this.propService = PropertiesService.getScriptProperties();
    // Track keys created during test for cleanup
    this.usedPropKeys = []; 
  }

  /**
   * Creates a fresh environment:
   * 1. A new sheet with valid headers
   * 2. A clean property store interface
   */
  setup() {
    console.log(`Creating temp sheet: ${this.sheetName}`);
    this.sheet = this.ss.insertSheet(this.sheetName);
    
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
   * Cleans up sheets and properties
   */
  teardown() {
    console.log("Cleaning up...");
    // 1. Delete Temp Sheet
    if (this.sheet) {
      this.ss.deleteSheet(this.sheet);
    }

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

/**
 * TC-01: Verifies that the correct headers pass validation.
 */
function test_Headers_HappyPath(h) {
  console.log("\n🧪 Running: test_Headers_HappyPath");
  
  const repo = new SheetsRepo(h.ss);
  const result = validateHeaders(repo, h.sheetName, CONFIG.EXPECTED_HEADERS);
  
  h.assert(result.valid === true, "Validation should pass for correct headers");
  h.assert(result.errors.length === 0, "Error list should be empty");
}

/**
 * TC-02: Verifies that shifting columns causes validation failure.
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

/**
 * TC-07: Verifies Alert Deduplication Logic (The "Server" Memory)
 */
function test_Deduplication_Logic(h) {
  console.log("\n🧪 Running: test_Deduplication_Logic (TC-07)");

  // 1. Mock the Property Service
  // We wrap the real service to intercept keys and prefix them with "TEST::"
  const mockProps = {
    getProperty: (k) => h.getProperty(k),
    setProperty: (k, v) => h.setProperty(k, v),
    setProperties: (obj) => {
      Object.entries(obj).forEach(([k,v]) => h.setProperty(k, v));
    }
  };

  // 2. Initialize Cache with 60 min TTL
  // We use an empty object {} for memoryCache to simulate a fresh run
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
  // We create a NEW instance to simulate a fresh script execution (serverless state)
  // We must re-read the properties we just flushed
  const memoryReload = {};
  // Manually mimicking the "load all properties" optimization step
  // In real life, the key is constructed inside the class, so we reconstruct it here for the test setup
  const expectedKey = `alert::${alertType}::${schedule}::${machine}`;
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

/**
 * TC-04: Verifies Critical Alert Business Logic
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
