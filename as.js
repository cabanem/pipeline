function copyMappedColumns() {
  const ss = SpreadsheetApp.getActive();
  const source = ss.getSheetByName('dump');
  const dest   = ss.getSheetByName('Results_2');

  if (!source || !dest) {
    throw new Error('Check that sheets "dump" and "Results_2" exist.');
  }

  const lastRow = source.getLastRow();
  if (lastRow < 2) {
    // No data rows (only headers or empty sheet)
    return;
  }

  // Ensure destination has enough rows
  if (dest.getMaxRows() < lastRow) {
    dest.insertRowsAfter(dest.getMaxRows(), lastRow - dest.getMaxRows());
  }

  // [sourceColLetter, destColLetter]
  const mappings = [
    ['A','B'],
    ['B','C'],
    ['C','D'],
    ['E','E'],
    ['G','F'],
    ['H','G'],
    ['N','H'],
    ['AP','K'],
    ['BU','I'],
  ];

  mappings.forEach(([srcCol, destCol]) => {
    const srcRange  = source.getRange(`${srcCol}2:${srcCol}${lastRow}`);  // skip header
    const values    = srcRange.getValues();
    const destRange = dest.getRange(`${destCol}2:${destCol}${lastRow}`);  // keep dest header
    destRange.setValues(values);
  });
}

/**
 * Runs on a time-based trigger.
 * If new rows were added to 'dump' since last check, runs copyMappedColumns().
 */
function checkDumpAndCopy() {
  const ss = SpreadsheetApp.getActive();
  const source = ss.getSheetByName('dump');
  if (!source) {
    throw new Error('Sheet "dump" not found.');
  }

  const props = PropertiesService.getScriptProperties();
  const currentLastRow = source.getLastRow();
  const stored = props.getProperty('dump_last_row');
  const previousLastRow = stored ? Number(stored) : 1;

  // Only act when new rows have been appended
  if (currentLastRow > previousLastRow) {
    copyMappedColumns();
    props.setProperty('dump_last_row', String(currentLastRow));
  } else if (!stored) {
    // First run, just set baseline
    props.setProperty('dump_last_row', String(currentLastRow));
  }
}

/**
 * One-time: create a 5-minute time-based trigger for checkDumpAndCopy().
 */
function createFiveMinuteTrigger() {
  ScriptApp.newTrigger('checkDumpAndCopy')
    .timeBased()
    .everyMinutes(5)
    .create();
}

/**
 * Optional one-time helper: initialize the baseline row count manually.
 * Run this once if you want to set the "current" state without copying.
 */
function initDumpWatcherState() {
  const ss = SpreadsheetApp.getActive();
  const source = ss.getSheetByName('dump');
  if (!source) {
    throw new Error('Sheet "dump" not found.');
  }
  const lastRow = source.getLastRow();
  PropertiesService.getScriptProperties().setProperty('dump_last_row', String(lastRow));
}
