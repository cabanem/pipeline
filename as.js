/**
 * Connector Lifecycle Planner — Google Apps Script Setup
 * - Adds data validation, conditional formatting, wrapping, freeze header
 * - Auto-timestamps "Last Update"
 * - Computes "Completion %" based on checkboxes/flags
 *
 * Usage:
 * 1) Import connector_lifecycle_planner.csv into a new Google Sheet.
 * 2) Name the first sheet exactly: Connectors
 * 3) Extensions -> Apps Script -> paste this file -> Save -> Run setupSheet()
 * 4) Grant permissions when prompted.
 */

const SHEET_NAME = 'Connectors';

function setupSheet() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sh = ss.getSheetByName(SHEET_NAME);
  if (!sh) throw new Error(`Sheet "${SHEET_NAME}" not found`);

  // Header indices (1-based). Keep in sync with CSV headers.
  const headers = sh.getRange(1,1,1,sh.getLastColumn()).getValues()[0];
  const col = Object.fromEntries(headers.map((h,i)=>[h, i+1]));

  // Freeze header row
  sh.setFrozenRows(1);

  // Wrap long text columns
  ["Goal / Scope Summary","Notes / Issues / Next Steps","Lessons Learned"].forEach(h => {
    sh.getRange(2, col[h], sh.getMaxRows()-1, 1).setWrap(true);
  });

  // Suggested column widths
  sh.setColumnWidths(col["Connector Name"], 1, 220);
  sh.setColumnWidths(col["Goal / Scope Summary"], 1, 360);
  sh.setColumnWidths(col["Notes / Issues / Next Steps"], 1, 360);
  sh.setColumnWidths(col["Lessons Learned"], 1, 320);

  // Dropdown validations
  const phases = ["Plan","Build","Test","Release","Maintain"];
  const authTypes = ["OAuth2","JWT","API key"];
  const httpPolicy = ["Workato verbs","Thin wrapper"];
  const schemaSource = ["object_definitions","OpenAPI derived","Manual"];
  const securityPosture = ["Least privilege","Broad","Unknown"];
  const testStatus = ["Not started","In progress","Passed","Failed"];
  const releaseApproval = ["Pending","Approved","Blocked"];
  const yesNo = ["TRUE","FALSE"]; // use checkboxes on some flags instead

  // Add drop-downs
  addDropdown(sh, col["Phase"], phases);
  addDropdown(sh, col["Auth Type"], authTypes);
  addDropdown(sh, col["HTTP Policy"], httpPolicy);
  addDropdown(sh, col["Schema Source"], schemaSource);
  addDropdown(sh, col["Security Posture"], securityPosture);
  addDropdown(sh, col["Test Status"], testStatus);
  addDropdown(sh, col["Release Approval"], releaseApproval);

  // Turn certain columns into checkboxes
  [
    "Error Normalization",
    "Pagination Implemented",
    "Telemetry Envelope",
    "Sample Output Validated",
    "Code Reviewed",
    "Docs Updated"
  ].forEach(h => {
    sh.getRange(2, col[h], sh.getMaxRows()-1, 1).insertCheckboxes();
  });

  // Conditional formatting rules
  const rules = sh.getConditionalFormatRules();

  // Row red if Phase=Test and Test Status=Failed
  rules.push(SpreadsheetApp.newConditionalFormatRule()
    .whenFormulaSatisfied(`=AND($${colToA1(col["Phase"])}2="Test",$${colToA1(col["Test Status"])}2="Failed")`)
    .setBackground("#f4c7c3")
    .setRanges([sh.getDataRange()])
    .build());

  // Row green if Release Approval=Approved
  rules.push(SpreadsheetApp.newConditionalFormatRule()
    .whenFormulaSatisfied(`=$${colToA1(col["Release Approval"])}2="Approved"`)
    .setBackground("#c6efce")
    .setRanges([sh.getDataRange()])
    .build());

  // Yellow if Telemetry Envelope unchecked
  rules.push(SpreadsheetApp.newConditionalFormatRule()
    .whenFormulaSatisfied(`=$${colToA1(col["Telemetry Envelope"])}2=FALSE`)
    .setBackground("#fff2cc")
    .setRanges([sh.getDataRange()])
    .build());

  sh.setConditionalFormatRules(rules);

  // Completion % formula: count TRUE in a set of checkbox columns divided by count of those columns
  const checkCols = [
    col["Error Normalization"],
    col["Pagination Implemented"],
    col["Telemetry Envelope"],
    col["Sample Output Validated"],
    col["Code Reviewed"],
    col["Docs Updated"]
  ];

  const firstDataRow = 2;
  const lastRow = sh.getMaxRows();
  for (let r = firstDataRow; r <= lastRow; r++) {
    const formula = `=IF(COUNTA(A${r})=0,"",ROUND( ( ` +
      checkCols.map(c => `IF(${colToA1(c)}${r}=TRUE,1,0)`).join("+") +
      ` ) / ${checkCols.length} * 100 ,0))`;
    sh.getRange(r, col["Completion %"]).setFormula(formula);
  }

  // Optional protection for key columns (comment out if not needed)
  // protectColumns_(sh, [col["Connector Name"], col["Version"], col["Owner"]]);

  // Timestamp "Last Update" on any edit across certain columns
  setInstallableOnEditTrigger_();
}

function addDropdown(sh, columnIndex, values) {
  const range = sh.getRange(2, columnIndex, sh.getMaxRows()-1, 1);
  const rule = SpreadsheetApp.newDataValidation()
    .requireValueInList(values, true)
    .setAllowInvalid(false)
    .build();
  range.setDataValidation(rule);
}

function colToA1(n){
  // Convert column index (1-based) to A1 letter(s)
  let s = "";
  while (n > 0) {
    let m = (n - 1) % 26;
    s = String.fromCharCode(65 + m) + s;
    n = Math.floor((n - m) / 26);
  }
  return s;
}

// Optional: Protect columns from edits by non-owners
function protectColumns_(sh, columns) {
  columns.forEach(c => {
    const range = sh.getRange(2, c, sh.getMaxRows()-1, 1);
    const protection = range.protect().setDescription('Protected key column');
    protection.removeEditors(protection.getEditors()); // only owner can edit
  });
}

/**
 * Installable trigger to timestamp Last Update on any edit to the row.
 * Run setupSheet() once; it creates the trigger.
 */
function setInstallableOnEditTrigger_() {
  const triggers = ScriptApp.getProjectTriggers();
  const exists = triggers.some(t => t.getHandlerFunction() === 'onEditStamp_');
  if (!exists) {
    ScriptApp.newTrigger('onEditStamp_')
      .forSpreadsheet(SpreadsheetApp.getActiveSpreadsheet())
      .onEdit()
      .create();
  }
}

function onEditStamp_(e) {
  try {
    if (!e || !e.range) return;
    const sh = e.range.getSheet();
    if (sh.getName() !== SHEET_NAME) return;
    const headers = sh.getRange(1,1,1,sh.getLastColumn()).getValues()[0];
    const col = Object.fromEntries(headers.map((h,i)=>[h, i+1]));
    const row = e.range.getRow();
    if (row === 1) return; // ignore header edits
    sh.getRange(row, col["Last Update"]).setValue(new Date());
  } catch (err) {
    // no-op
  }
}
