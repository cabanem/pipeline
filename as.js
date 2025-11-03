/** Workato — Send Full Table (single call) + Run Log
 *  - Menu: Initialize Config & Log / Send Table to Workato
 *  - Reads contiguous headers from HEADER_ROW_INDEX to define the table
 *  - Includes values from formula/array-formula columns
 *  - Sends one POST to Workato; logs outcome to SendLog
 */

const CFG_DEFAULTS = {
  WEBHOOK_URL: '',
  DATA_SHEET: 'Table',          // name of sheet with the 4-column table
  HEADER_ROW_INDEX: '1',        // row number where headers live
  USE_DISPLAY_VALUES: 'FALSE',  // TRUE to send formatted text; FALSE to send typed values
  LOG_SHEET: 'SendLog',
  CORRELATION_HEADER: 'x-correlation-id',
  HEADERS_JSON: '{}',           // extra headers if needed: {"x-team":"ops"}
  QUERY_JSON: '{}',              // optional query params to append: {"env":"prod"}
  // Failure email config (scaffold into Config)
  FAILURE_EMAIL_RECIPIENTS: '',
  FAILURE_EMAIL_CC: '',
  FAILURE_EMAIL_BCC: '',
  FAILURE_EMAIL_SUBJECT: 'Error in Google Apps Script: Workato Table Sender',
  FAILURE_EMAIL_NAME: 'Workato Table Sender',
  FAILURE_EMAIL_ON_EXCEPTION: 'TRUE',
  FAILURE_EMAIL_ON_NON2XX: 'TRUE'
};

// ===== Menu =====
function onOpen() {
  SpreadsheetApp.getUi()
    .createMenu('Workato')
    .addItem('Initialize Config & Log', 'initConfigAndLog_')
    .addSeparator()
    .addItem('Send Table to Workato', 'sendTableToWorkato')
    .addToUi();
}

// ===== Public action =====
function sendTableToWorkato() {
  const cfg = readConfig_();

  let sheet, headers, rows = [];
  let correlationId = '';
  let url = '';
  let bytes = 0, latency = 0;
  let httpCode = '', status = 'SUCCESS', err = '', respSnippet = '';

  try {
    sheet = getSheetByNameOrThrow_(cfg.DATA_SHEET);
    const headerRow = Number(cfg.HEADER_ROW_INDEX || '1');

    // Build table from contiguous headers and non-blank rows
    headers = readContiguousHeaders_(sheet, headerRow);
    if (headers.length === 0) throw new Error('No headers found on header row.');

    const values = readDataValues_(sheet, headerRow, headers.length, toBool_(cfg.USE_DISPLAY_VALUES));
    const trimmed = trimTrailingBlankRows_(values);
    rows = trimmed
      .map(r => rowArrayToObject_(headers, r))
      .filter(obj => !isAllEmptyObject_(obj));

    // Payload + request
    correlationId = buildCorrelationId_(sheet.getName());
    const payload = { table: { sheet: sheet.getName(), headers, rows }, correlation_id: correlationId };
    url = buildUrlWithQuery_(cfg.WEBHOOK_URL, parseJsonSafe_(cfg.QUERY_JSON));
    const headersOut = buildHeaders_(cfg, correlationId);

    const body = JSON.stringify(payload);
    bytes = Utilities.newBlob(body).getBytes().length;

    const t0 = Date.now();
    const resp = UrlFetchApp.fetch(url, {
      method: 'post',
      contentType: 'application/json',
      payload: body,
      headers: headersOut,
      muteHttpExceptions: true
    });
    latency = Date.now() - t0;

    httpCode = String(resp.getResponseCode());
    if (httpCode[0] !== '2') {
      status = 'FAILED';
      respSnippet = safeSnippet_(resp.getContentText());
      err = 'Non-2xx response';

      if (toBool_(cfg.FAILURE_EMAIL_ON_NON2XX)) {
        sendFailureEmail_(cfg, {
          message: `HTTP ${httpCode} from Workato`,
          stack: '',
          context: {
            sheet: sheet.getName(),
            rows_sent: rows.length,
            bytes_sent: bytes,
            http_code: httpCode,
            response_snippet: respSnippet,
            correlation_id: correlationId,
            url
          }
        });
      }
    }
  } catch (e) {
    status = 'FAILED';
    err = String(e && e.message ? e.message : e);

    if (toBool_(cfg.FAILURE_EMAIL_ON_EXCEPTION)) {
      sendFailureEmail_(cfg, {
        message: err,
        stack: e && e.stack ? e.stack : '',
        context: {
          sheet: (sheet && sheet.getName) ? sheet.getName() : String(cfg.DATA_SHEET || ''),
          rows_sent: rows.length || 0,
          bytes_sent: bytes || 0,
          correlation_id: correlationId || '',
          url
        }
      });
    }
  } finally {
    // Always log
    logRun_(cfg, {
      sheet: sheet ? sheet.getName() : String(cfg.DATA_SHEET || ''),
      rows_sent: rows.length || 0,
      bytes_sent: bytes || 0,
      http_code: httpCode || '',
      status,
      latency_ms: latency || 0,
      correlation_id: correlationId || '',
      error: err || '',
      response_snippet: respSnippet || ''
    });
  }

  SpreadsheetApp.getActive().toast(
    `${status}: sent ${rows.length} row(s), http=${httpCode || 'n/a'}, bytes=${bytes}`
  );
}

// ===== Config & Log setup =====
function initConfigAndLog_() {
  const ss = SpreadsheetApp.getActive();
  // Config
  let cfg = ss.getSheetByName('Config') || ss.insertSheet('Config');
  if (cfg.getLastRow() <= 1) {
    cfg.clear();
    cfg.getRange(1,1,1,2).setValues([['key','value']]).setFontWeight('bold');
    const rows = Object.entries(CFG_DEFAULTS);
    cfg.getRange(2,1,rows.length,2).setValues(rows);
  }
  // Log
  let log = ss.getSheetByName(CFG_DEFAULTS.LOG_SHEET) || ss.insertSheet(CFG_DEFAULTS.LOG_SHEET);
  if (log.getLastRow() === 0) {
    log.getRange(1,1,1,10).setValues([[
      'timestamp_utc','sheet','rows_sent','bytes_sent','http_code',
      'status','latency_ms','correlation_id','error','response_snippet'
    ]]).setFontWeight('bold');
  }
  SpreadsheetApp.getActive().toast('Config & Log ready. Set WEBHOOK_URL in Config.');
}

// ===== Data reading helpers =====
function readContiguousHeaders_(sheet, headerRow) {
  const lastCol = sheet.getLastColumn();
  const headerVals = sheet.getRange(headerRow, 1, 1, Math.max(1, lastCol)).getValues()[0];
  const headers = [];
  for (let i=0;i<headerVals.length;i++) {
    const h = String(headerVals[i] || '').trim();
    if (i === 0 && h === '') break;         // nothing at A?
    if (h === '') break;                    // stop at first blank -> contiguous block
    headers.push(h);
  }
  return headers;
}

function readDataValues_(sheet, headerRow, colCount, useDisplay) {
  const startRow = headerRow + 1;
  const lastRow = sheet.getLastRow();
  const numRows = Math.max(0, lastRow - headerRow);
  if (numRows === 0) return [];
  const range = sheet.getRange(startRow, 1, numRows, colCount);
  return useDisplay ? range.getDisplayValues() : range.getValues();
}

function trimTrailingBlankRows_(rows) {
  let last = rows.length - 1;
  for (; last >= 0; last--) {
    const r = rows[last];
    if (r.some(v => String(v ?? '').trim() !== '')) break;
  }
  return rows.slice(0, last + 1);
}

function rowArrayToObject_(headers, arr) {
  const obj = {};
  for (let i=0;i<headers.length;i++) obj[headers[i]] = arr[i];
  return obj;
}

function isAllEmptyObject_(obj) {
  return Object.keys(obj).every(k => {
    const v = obj[k];
    return v === null || v === undefined || String(v).trim() === '';
  });
}

// ===== HTTP & logging helpers =====
function buildHeaders_(cfg, correlationId) {
  const out = Object.assign({}, parseJsonSafe_(cfg.HEADERS_JSON));
  out['Content-Type'] = 'application/json';
  if (cfg.CORRELATION_HEADER) out[cfg.CORRELATION_HEADER] = correlationId;
  return out;
}

function buildUrlWithQuery_(base, params) {
  if (!base) throw new Error('Config.WEBHOOK_URL is required.');
  const keys = Object.keys(params || {});
  if (!keys.length) return base;
  const q = keys.map(k => encodeURIComponent(k) + '=' + encodeURIComponent(params[k])).join('&');
  return base + (base.includes('?') ? '&' : '?') + q;
}

function logRun_(cfg, evt) {
  const ss = SpreadsheetApp.getActive();
  const name = (cfg && cfg.LOG_SHEET) ? cfg.LOG_SHEET : CFG_DEFAULTS.LOG_SHEET;
  const sh = ss.getSheetByName(name) || ss.insertSheet(name);
  sh.appendRow([
    new Date().toISOString(),
    evt.sheet||'',
    evt.rows_sent||0,
    evt.bytes_sent||0,
    evt.http_code||'',
    evt.status||'',
    evt.latency_ms||0,
    evt.correlation_id||'',
    evt.error||'',
    safeSnippet_(evt.response_snippet||'')
  ]);
}

function buildCorrelationId_(sheetName) {
  const rnd = Utilities.getUuid().slice(0,8);
  return `gsheets/${sheetName}/${Date.now()}/${rnd}`;
}

// ===== Utils =====
function readConfig_() {
  const ss = SpreadsheetApp.getActive();
  const cfgSheet = ss.getSheetByName('Config');
  const cfg = Object.assign({}, CFG_DEFAULTS);
  if (cfgSheet && cfgSheet.getLastRow() >= 2) {
    const rows = cfgSheet.getRange(2,1,cfgSheet.getLastRow()-1,2).getValues();
    rows.forEach(([k,v]) => { if (k) cfg[String(k)] = String(v); });
  }
  // Script Properties override (optional for secrets)
  const props = PropertiesService.getScriptProperties().getProperties();
  Object.keys(props || {}).forEach(k => { if (props[k] != null) cfg[k] = props[k]; });
  return cfg;
}

function parseJsonSafe_(s) { try { return s ? JSON.parse(s) : {}; } catch (_){ return {}; } }
function toBool_(v) { const s = String(v).trim().toLowerCase(); return s==='true'||s==='1'||s==='yes'||s==='y'; }
function safeSnippet_(txt, max=600) { return txt ? String(txt).slice(0, max) : ''; }
function getSheetByNameOrThrow_(name) {
  const sh = SpreadsheetApp.getActive().getSheetByName(name);
  if (!sh) throw new Error(`Missing sheet: ${name}`);
  return sh;
}

function sendFailureEmail_(cfg, err) {
  try {
    const toList  = parseEmailList_(cfg.FAILURE_EMAIL_RECIPIENTS || cfg.FAILURE_EMAIL_RECIPIENT);
    const ccList  = parseEmailList_(cfg.FAILURE_EMAIL_CC);
    const bccList = parseEmailList_(cfg.FAILURE_EMAIL_BCC);
    if (!toList.length) return;

    const scriptId = ScriptApp.getScriptId();
    let projectName = 'Apps Script';
    try { projectName = DriveApp.getFileById(scriptId).getName(); } catch (_){}

    const subject = cfg.FAILURE_EMAIL_SUBJECT || `Error in Google Apps Script: ${projectName}`;
    const htmlBody = [
      `<p><b>Project:</b> <code>${escapeHtml_(projectName)}</code></p>`,
      `<p><b>Script ID:</b> <code>${escapeHtml_(scriptId)}</code></p>`,
      err && err.message ? `<p><b>Error Message:</b> ${escapeHtml_(String(err.message))}</p>` : '',
      err && err.stack ? `<p><b>Stack Trace:</b></p><pre>${escapeHtml_(String(err.stack))}</pre>` : '',
      err && err.context ? `<hr><p><b>Context</b></p><pre>${escapeHtml_(JSON.stringify(err.context, null, 2))}</pre>` : ''
    ].filter(Boolean).join('\n');

    const adv = {
      name: cfg.FAILURE_EMAIL_NAME || `${projectName} Script`,
      htmlBody
    };
    if (ccList.length)  adv.cc  = ccList.join(',');
    if (bccList.length) adv.bcc = bccList.join(',');

    try {
      GmailApp.sendEmail(toList.join(','), subject, '', adv);
    } catch (_) {
      MailApp.sendEmail({
        to: toList.join(','),
        subject,
        htmlBody,
        name: adv.name,
        cc:  ccList.join(','),
        bcc: bccList.join(',')
      });
    }
  } catch (e) {
    Logger.log('Failed to send failure email: ' + (e && e.message ? e.message : e));
  }
}

function parseEmailList_(s) {
  if (!s) return [];
  return String(s)
    .split(/[,\n;]+/)         // commas, semicolons, or newlines
    .map(x => x.trim())
    .filter(Boolean);
}

function escapeHtml_(s){
  return String(s).replace(/[&<>"']/g, c => (
    { '&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;', "'":'&#39;' }[c]
  ));
}

