/***** CONFIG *****************************************************************/
const CFG = {
  // REQUIRED: set to your target Drive folder ID where builds will be stored
  RELEASE_ROOT_FOLDER_ID: 'PUT_RELEASE_FOLDER_ID_HERE',
  // OPTIONAL: webhook for build summaries (Google Chat or Slack-compatible)
  CHAT_WEBHOOK_URL: 'PUT_WEBHOOK_URL_OR_EMPTY',
  // Map audiences to Docs template IDs
  TEMPLATES: {
    exec: 'DOC_TEMPLATE_ID_EXEC',
    qa: 'DOC_TEMPLATE_ID_QA'
    // compliance: 'DOC_TEMPLATE_ID_COMPLIANCE'
  }
};
/******************************************************************************/

// Convenience getters
function SS() { return SpreadsheetApp.getActive(); }
function sh(name) { return SS().getSheetByName(name); }

// Menu
function onOpen() {
  SS().addMenu('TestPlan', [
    {name: 'Validate SSOT', functionName: 'validate_ssot'},
    {name: 'Render All Audiences', functionName: 'render_all_audiences'},
    {name: 'Nightly Truth Build (Run Now)', functionName: 'truth_build'},
    {name: 'Cut Baseline', functionName: 'cut_baseline'}
  ]);
}

// --- Core: Validation --------------------------------------------------------
function validate_ssot() {
  const problems = [];
  const reqs = readTable('requirements');
  const tcs  = readTable('test_cases');
  const risks= readTable('risks');

  // ID uniqueness + regex
  problems.push(...checkIds(reqs, /^REQ-[A-Z]+-\d{3}$/, 'requirements'));
  problems.push(...checkIds(tcs,  /^TC-[A-Z]+-\d{3}$/,  'test_cases'));
  problems.push(...checkIds(risks, /^RISK-[A-Z]+-\d{3}$/,'risks'));

  // Enums
  problems.push(...checkEnum(reqs, 'priority', ['P0','P1','P2','P3']));
  problems.push(...checkEnum(reqs, 'status',   ['draft','proposed','approved','deprecated']));
  problems.push(...checkEnum(tcs,  'env',      ['dev','staging','prod-sim']));
  problems.push(...checkEnum(tcs,  'status',   ['draft','ready','blocked','deprecated']));
  problems.push(...checkEnum(risks,'type',     ['security','privacy','operational','quality','compliance','other']));
  problems.push(...checkEnum(risks,'severity', ['low','medium','high','critical']));
  problems.push(...checkEnum(risks,'status',   ['open','mitigated','accepted','retired']));

  // Traceability (REQ has ≥1 TC; TC maps to existing REQ)
  const reqIds = new Set(reqs.map(r => r.id));
  const tcReqLinks = flattenCsv(tcs, 'requirement_ids');
  const orphanTCs = tcs.filter(t => !splitCsv(t.requirement_ids).every(id => reqIds.has(id)));
  const reqCoverageMap = coverageCounts(reqs, tcs);
  const orphanReqs = reqs.filter(r => (reqCoverageMap[r.id] || 0) === 0);

  // Risks mapped?
  const riskIds = new Set(risks.map(r => r.id));
  const reqRiskLinks = flattenCsv(reqs, 'risk_ids').filter(id => id);
  const unknownRiskRefs = reqRiskLinks.filter(id => !riskIds.has(id));

  if (orphanTCs.length) problems.push(`Orphan test_cases (bad or missing requirement_ids): ${orphanTCs.map(t=>t.id).join(', ')}`);
  if (orphanReqs.length) problems.push(`Uncovered requirements (0 test cases): ${orphanReqs.map(r=>r.id).join(', ')}`);
  if (unknownRiskRefs.length) problems.push(`Unknown risks referenced by requirements: ${[...new Set(unknownRiskRefs)].join(', ')}`);

  // Deprecated rules
  reqs.filter(r => r.status === 'deprecated' && !r.version_deprecated)
      .forEach(r => problems.push(`Requirement ${r.id} deprecated without version_deprecated`));
  tcs.filter(t => t.status === 'deprecated' && !t.version_deprecated)
     .forEach(t => problems.push(`Test case ${t.id} deprecated without version_deprecated`));

  // Write summary
  if (problems.length) {
    SpreadsheetApp.getUi().alert(`Validation FAILED:\n- ${problems.join('\n- ')}`);
  } else {
    SpreadsheetApp.getUi().alert('Validation OK ✅');
  }
  return {problems, reqs, tcs, risks, orphanReqs, orphanTCs};
}

function checkIds(rows, regex, tab) {
  const seen = new Set();
  const problems = [];
  rows.forEach(r => {
    if (!regex.test(r.id || '')) problems.push(`${tab}: ${r.id} fails regex`);
    if (seen.has(r.id)) problems.push(`${tab}: duplicate id ${r.id}`);
    seen.add(r.id);
  });
  return problems;
}

function checkEnum(rows, field, allowed) {
  const probs = [];
  rows.forEach(r => {
    if (r[field] && !allowed.includes(String(r[field]).trim()))
      probs.push(`Enum violation: ${r.id}.${field}='${r[field]}' not in [${allowed.join(', ')}]`);
  });
  return probs;
}

// --- Core: Build & Render ----------------------------------------------------
function render_all_audiences() {
  const audiences = Object.keys(CFG.TEMPLATES);
  const baseline = getBaseline();
  audiences.forEach(a => render_audience(a, baseline));
}

function truth_build() {
  const baseline = getBaseline();
  const audiences = Object.keys(CFG.TEMPLATES);
  const val = validate_ssot();
  const ssotHash = hashSSOT();
  audiences.forEach(aud => {
    const {docId, docHash, metrics, ok} = render_audience(aud, baseline, ssotHash);
    write_build_ledger({
      audience: aud,
      baseline,
      ssot_hash: ssotHash,
      doc_file_id: docId,
      doc_hash: docHash,
      coverage_pct: metrics.coverage_pct,
      risks_high_open: metrics.risks_high_open,
      orphans_req: metrics.orphans_req,
      orphans_tc: metrics.orphans_tc,
      invariants_ok: ok
    });
    notify_build(aud, baseline, ok, docId, metrics);
  });
}

function render_audience(audience, baseline, ssotHashOpt) {
  const templateId = CFG.TEMPLATES[audience];
  if (!templateId) throw new Error(`No template configured for audience: ${audience}`);

  const {reqs, tcs, risks} = {
    reqs: readTable('requirements'),
    tcs:  readTable('test_cases'),
    risks:readTable('risks')
  };

  const metrics = compute_metrics(reqs, tcs, risks);
  const ssotHash = ssotHashOpt || hashSSOT();

  // Copy template
  const dstName = `${baseline} - ${audience} - ${Utilities.formatDate(new Date(), Session.getScriptTimeZone(), 'yyyyMMdd-HHmmss')}`;
  const dstId = DriveApp.getFileById(templateId).makeCopy(dstName, DriveApp.getFolderById(CFG.RELEASE_ROOT_FOLDER_ID)).getId();
  const doc = DocumentApp.openById(dstId);
  const body = doc.getBody();

  // Simple placeholder replacement
  replaceAll(body, '{{BASELINE}}', baseline);
  replaceAll(body, '{{DATE}}', new Date().toISOString().slice(0,10));
  replaceAll(body, '{{COVERAGE_PCT}}', String(metrics.coverage_pct));
  replaceAll(body, '{{RISKS_HIGH_OPEN}}', String(metrics.risks_high_open));
  replaceAll(body, '{{ORPHANS_REQ}}', String(metrics.orphans_req));
  replaceAll(body, '{{ORPHANS_TC}}', String(metrics.orphans_tc));

  // Insert QA tables for QA audience (example)
  if (audience === 'qa') {
    // We’ll append a table of TC → REQ mappings
    body.appendParagraph('\nTest Cases & Requirement Mapping').setHeading(DocumentApp.ParagraphHeading.HEADING2);
    const tableData = [['TC ID','Objective','Env','Status','Requirements']];
    tcs.forEach(tc => {
      tableData.push([tc.id, tc.objective || '', tc.env || '', tc.status || '', (tc.requirement_ids || '')]);
    });
    body.appendTable(tableData);
  }

  // Exec overlay example (optional)
  if (audience === 'exec') {
    const overlay = readSingle('overlays_exec');
    replaceAll(body, '{{EXEC_KPIS}}', overlay.baseline_kpis_summary || '');
    replaceAll(body, '{{KEY_CHANGES}}', overlay.key_changes_summary || '');
    replaceAll(body, '{{RELEASE_SCOPE}}', overlay.release_scope_summary || '');
  }

  // Invariant: no unreplaced placeholders
  const unreplaced = body.getText().match(/{{[^}]+}}/g) || [];
  doc.saveAndClose();

  const ok = unreplaced.length === 0;
  const docHash = hashDoc(dstId);

  return {docId: dstId, docHash, metrics, ok, unreplaced};
}

// --- Metrics & helpers -------------------------------------------------------
function compute_metrics(reqs, tcs, risks) {
  const cov = coverageCounts(reqs, tcs);
  const covered = Object.values(cov).filter(n => n > 0).length;
  const coverage_pct = reqs.length ? Math.round((covered / reqs.length) * 100) : 100;

  const risks_high_open = risks.filter(r => (r.severity === 'high' || r.severity === 'critical') && r.status !== 'mitigated' && r.status !== 'retired').length;

  const orphans_req = reqs.filter(r => (cov[r.id] || 0) === 0).length;
  const reqIds = new Set(reqs.map(r => r.id));
  const orphans_tc = tcs.filter(t => !splitCsv(t.requirement_ids).every(id => reqIds.has(id))).length;

  return {coverage_pct, risks_high_open, orphans_req, orphans_tc};
}

function coverageCounts(reqs, tcs) {
  const counts = {};
  reqs.forEach(r => counts[r.id] = 0);
  tcs.forEach(tc => splitCsv(tc.requirement_ids).forEach(rid => { if (counts[rid] != null) counts[rid]++; }));
  return counts;
}

function replaceAll(body, needle, val) {
  body.replaceText(escapeRegExp(needle), val == null ? '' : String(val));
}
function escapeRegExp(s) {
  return s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

// --- Hashing -----------------------------------------------------------------
function hashSSOT() {
  const tabs = ['requirements','test_cases','risks'];
  const chunks = tabs.map(t => JSON.stringify(readTable(t))).join('|');
  const h = Utilities.computeDigest(Utilities.DigestAlgorithm.SHA_256, chunks);
  return Utilities.base64Encode(h);
}
function hashDoc(fileId) {
  const blob = DriveApp.getFileById(fileId).getBlob();
  const h = Utilities.computeDigest(Utilities.DigestAlgorithm.SHA_256, blob.getBytes());
  return Utilities.base64Encode(h);
}

// --- Ledger & Notifications --------------------------------------------------
function write_build_ledger(entry) {
  const s = sh('builds');
  const buildId = `BUILD-${Utilities.formatDate(new Date(), Session.getScriptTimeZone(), 'yyyyMMdd-HHmmss')}`;
  const who = Session.getActiveUser().getEmail() || 'script';
  const link = `https://docs.google.com/document/d/${entry.doc_file_id}/edit`;
  s.appendRow([
    buildId,
    entry.baseline,
    entry.audience,
    entry.invariants_ok ? 'PASS' : 'FAIL',
    entry.ssot_hash,
    entry.doc_file_id,
    entry.doc_hash,
    entry.coverage_pct,
    entry.risks_high_open,
    entry.orphans_req,
    entry.orphans_tc,
    entry.invariants_ok,
    who,
    new Date(),
    link
  ]);
}

function notify_build(audience, baseline, ok, docId, metrics) {
  if (!CFG.CHAT_WEBHOOK_URL) return;
  const text = [
    `*${ok ? 'PASS ✅' : 'FAIL ❌'}* ${baseline} → ${audience}`,
    `Doc: https://docs.google.com/document/d/${docId}/edit`,
    `Coverage: ${metrics.coverage_pct}% | High/Crit Risks Open: ${metrics.risks_high_open} | Orphans: REQ=${metrics.orphans_req}, TC=${metrics.orphans_tc}`
  ].join('\n');
  UrlFetchApp.fetch(CFG.CHAT_WEBHOOK_URL, {
    method: 'post',
    contentType: 'application/json',
    payload: JSON.stringify({text})
  });
}

// --- Baseline management -----------------------------------------------------
function getBaseline() {
  // Read from a named cell in any tab; fall back to default
  const range = sh('requirements').getRange('J1'); // e.g., put current baseline in J1
  const v = (range.getValue() || '').toString().trim();
  return v || 'TESTPLAN 0.1.0';
}

function cut_baseline() {
  // Minimal: stamp a new baseline semver (manual edit in cell J1), then run a truth build
  const ui = SpreadsheetApp.getUi();
  const resp = ui.prompt('Cut Baseline', 'Enter new baseline (e.g., TESTPLAN 1.3.0):', ui.ButtonSet.OK_CANCEL);
  if (resp.getSelectedButton() !== ui.Button.OK) return;
  const baseline = resp.getResponseText().trim();
  sh('requirements').getRange('J1').setValue(baseline);
  truth_build();
}

// --- Data access -------------------------------------------------------------
function readTable(tab) {
  const s = sh(tab);
  const values = s.getDataRange().getValues();
  const header = values.shift().map(h => String(h).trim().toLowerCase());
  return values.filter(r => r.some(c => c !== '' && c != null))
               .map(row => Object.fromEntries(header.map((h,i) => [h, row[i]])));
}

function readSingle(tab) {
  const rows = readTable(tab);
  return rows[0] || {};
}

function splitCsv(s) {
  if (!s) return [];
  return String(s).split(',').map(x => x.trim()).filter(Boolean);
}

function flattenCsv(rows, field) {
  return rows.flatMap(r => splitCsv(r[field]));
}

// --- On-edit guardrails (IDs, enums, changelog) ------------------------------
function onEdit(e) {
  try {
    if (!e || !e.range || !e.range.getSheet()) return;
    const sheetName = e.range.getSheet().getName();
    const edited = (e.value || '').toString().trim();
    const row = e.range.getRow();
    const col = e.range.getColumn();
    const header = e.range.getSheet().getRange(1, 1, 1, e.range.getSheet().getLastColumn()).getValues()[0]
      .map(h => String(h).trim().toLowerCase());
    const field = header[col-1];

    // ID regex enforcement
    if (field === 'id') {
      let ok = true;
      if (sheetName === 'requirements') ok = /^REQ-[A-Z]+-\d{3}$/.test(edited);
      if (sheetName === 'test_cases')   ok = /^TC-[A-Z]+-\d{3}$/.test(edited);
      if (sheetName === 'risks')        ok = /^RISK-[A-Z]+-\d{3}$/.test(edited);
      if (!ok) {
        e.range.setBackground('#ffcccc');
        SpreadsheetApp.getUi().alert(`Invalid ID format on ${sheetName}!`);
      } else {
        e.range.setBackground(null);
      }
    }

    // Write changelog for significant tabs (skip header row)
    if (row > 1 && ['requirements','test_cases','risks'].includes(sheetName)) {
      const idCell = e.range.getSheet().getRange(row, header.indexOf('id')+1).getValue();
      sh('changelog').appendRow([new Date(), Session.getActiveUser().getEmail(), sheetName, idCell, field, e.oldValue || '', e.value || '']);
    }
  } catch (err) {
    // don’t block edits on errors
    console.error(err);
  }
}
