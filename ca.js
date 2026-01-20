/**
 * @file 30_DashboardService.gs
 * @description Creates/refreshes dashboard + view tabs and manages visibility/protection.
 */

class DashboardService {
  static ensureAll(ctx, stats = null) {
    const cfg = ctx.config;
    if (!cfg.DASHBOARD || !cfg.DASHBOARD.ENABLE) return;

    const ss = SpreadsheetApp.getActiveSpreadsheet();
    this._ensureOutputSheets_(ss, ctx);

    // 1) Build/refresh views
    this._ensureViewRecipes_(ss, ctx);

    // 2) Build/refresh dashboard
    this._ensureDashboardHome_(ss, ctx, stats);

    // 3) Apply sheet visibility rules
    this.applyVisibility(ctx);

    // 4) Apply protections (warning-only by default)
    this.applyProtections(ctx);
  }
  /**
   * Call after a successful inventory sync.
   * @param {AppContext} ctx
   * @param {Object} stats counts/metadata to show on dashboard
   */
  static postInventorySync(ctx, stats = {}) {
    const now = new Date();
    const iso = now.toISOString();

    // Store a “last sync” stamp for humans + debugging
    try {
      ConfigStore.setScript("LAST_INVENTORY_SYNC_AT", iso);
    } catch (e) {}

    // Inject the stamp into the dashboard as well
    const merged = Object.assign({ last_sync_at: iso }, stats || {});
    this.ensureAll(ctx, merged);
  }
  static applyVisibility(ctx) {
    const cfg = ctx.config;
    if (!cfg.DASHBOARD || !cfg.DASHBOARD.ENABLE) return;
    if (!cfg.DASHBOARD.HIDE_BACKEND_IN_BASIC) return;

    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const sheets = ss.getSheets();

    const isAdv = (typeof UiMode !== "undefined") ? UiMode.isAdvanced() : false;

    const visibleInBasic = new Set([
      cfg.SHEETS.DASHBOARD_HOME,
      cfg.SHEETS.VIEW_RECIPES,
      cfg.SHEETS.AI_ANALYSIS,
      cfg.SHEETS.PROCESS_MAPS
    ].filter(Boolean));

    sheets.forEach(sh => {
      const name = sh.getName();
      if (isAdv) {
        sh.showSheet();
      } else {
        if (visibleInBasic.has(name)) sh.showSheet();
        else sh.hideSheet();
      }
    });
  }
  static applyProtections(ctx) {
    const cfg = ctx.config;
    if (!cfg.DASHBOARD || !cfg.DASHBOARD.ENABLE) return;

    const warningOnly = Boolean(cfg.DASHBOARD.PROTECT_BACKEND_WARNING_ONLY);
    const ss = SpreadsheetApp.getActiveSpreadsheet();

    const backendKeys = [
      "PROJECTS",
      "FOLDERS",
      "RECIPES",
      "PROPERTIES",
      "TABLES",
      "LOOKUP_TABLES",
      "DEPENDENCIES",
      "CALL_EDGES",
      "LOGIC",
      "DEBUG",
      "LOGIC_INPUT"
    ];

    backendKeys.forEach(k => {
      const name = cfg.SHEETS[k];
      if (!name) return;
      const sh = ss.getSheetByName(name);
      if (!sh) return;

      this._ensureProtection_(sh, `WorkatoSync backend: ${name}`, warningOnly);
    });
  }

  // ---------------------------------------------------------------------------------------
  // Dashboard_Home
  // ---------------------------------------------------------------------------------------
  static _ensureDashboardHome_(ss, ctx, stats) {
    const cfg = ctx.config;
    const name = cfg.SHEETS.DASHBOARD_HOME || "Dashboard_Home";
    const sh = this._getOrCreateSheet_(ss, name);

    // Light reset: keep it deterministic. (If you want custom layout, set OVERWRITE_VIEWS=false and adjust.)
    if (cfg.DASHBOARD.OVERWRITE_VIEWS) {
      sh.clear();
      sh.setFrozenRows(0);
      sh.setFrozenColumns(0);
    }

    // Title
    sh.getRange("A1").setValue("Workato Sync — Dashboard");
    sh.getRange("A1").setFontWeight("bold").setFontSize(14);

    // Status block
    sh.getRange("A3").setValue("Last inventory sync");
    sh.getRange("A4").setValue("Base URL");
    sh.getRange("A5").setValue("User (best-effort)");

    const last = (stats && stats.last_sync_at)
      ? String(stats.last_sync_at)
      : String(ConfigStore.get("LAST_INVENTORY_SYNC_AT", { preferUser: false, defaultValue: "" }) || "");

    sh.getRange("B3").setValue(last || "(unknown)");
    sh.getRange("B4").setValue(String(cfg.API.BASE_URL || ""));
    try {
      const u = ctx.inventoryService.getCurrentUser();
      sh.getRange("B5").setValue(u && u.name ? u.name : "(unknown)");
    } catch (e) {
      sh.getRange("B5").setValue("(unknown)");
    }

    // Counts block
    sh.getRange("A7").setValue("Counts");
    sh.getRange("A7").setFontWeight("bold");

    const rows = [
      ["Projects", `=IFERROR(COUNTA(${cfg.SHEETS.PROJECTS}!A2:A),0)`],
      ["Folders", `=IFERROR(COUNTA(${cfg.SHEETS.FOLDERS}!A2:A),0)`],
      ["Recipes", `=IFERROR(COUNTA(${cfg.SHEETS.RECIPES}!A2:A),0)`],
      ["Properties", `=IFERROR(COUNTA(${cfg.SHEETS.PROPERTIES}!A2:A),0)`],
      ["Data tables", `=IFERROR(COUNTA(${cfg.SHEETS.TABLES}!A2:A),0)`],
      ["Lookup tables", `=IFERROR(COUNTA(${cfg.SHEETS.LOOKUP_TABLES}!A2:A),0)`],
      ["Dependencies (rows)", `=IFERROR(COUNTA(${cfg.SHEETS.DEPENDENCIES}!A2:A),0)`],
      ["Call edges (rows)", `=IFERROR(COUNTA(${cfg.SHEETS.CALL_EDGES}!A2:A),0)`],
      ["AI analyses (rows)", `=IFERROR(COUNTA(${cfg.SHEETS.AI_ANALYSIS}!A2:A),0)`],
      ["Process maps (rows)", `=IFERROR(COUNTA(${cfg.SHEETS.PROCESS_MAPS}!A2:A),0)`]
    ];

    sh.getRange(8, 1, rows.length, 2).setValues(rows);

    // Quick links
    sh.getRange("D3").setValue("Quick links");
    sh.getRange("D3").setFontWeight("bold");
    sh.getRange("D4").setFormula(`=HYPERLINK("#gid="&SHEETID("${cfg.SHEETS.VIEW_RECIPES}"), "Go to View_Recipes")`);
    sh.getRange("D5").setFormula(`=HYPERLINK("#gid="&SHEETID("${cfg.SHEETS.AI_ANALYSIS}"), "Go to Output_AI_Analysis")`);
    sh.getRange("D6").setFormula(`=HYPERLINK("#gid="&SHEETID("${cfg.SHEETS.PROCESS_MAPS}"), "Go to Output_Process_Maps")`);

    // Light formatting
    sh.autoResizeColumns(1, 5);
    sh.getRange("A3:A5").setFontWeight("bold");
    sh.getRange("A8:A" + (7 + rows.length)).setFontWeight("bold");
  }

  // ---------------------------------------------------------------------------------------
  // View_Recipes (curated selection surface)
  // ---------------------------------------------------------------------------------------
  static _ensureViewRecipes_(ss, ctx) {
    const cfg = ctx.config;
    const name = cfg.SHEETS.VIEW_RECIPES || "View_Recipes";
    const sh = this._getOrCreateSheet_(ss, name);

    if (cfg.DASHBOARD.OVERWRITE_VIEWS) {
      sh.clear();
    }

    const headers = cfg.HEADERS.VIEW_RECIPES || [
      "Recipe ID", "Name", "Status", "Project", "Folder", "Last Run At",
      "# Dependencies", "# Calls Out", "Has AI?", "Has Maps?"
    ];

    sh.getRange(1, 1, 1, headers.length).setValues([headers]);
    sh.getRange(1, 1, 1, headers.length).setFontWeight("bold").setBackground("#d9d9d9");
    sh.setFrozenRows(1);

    // Main recipe table (A2:F) as a single QUERY so rows always align
    // Pulls from Inventory_Recipes (A=ID..F=LastRunAt)
    sh.getRange("A2").setFormula(
      `=QUERY(${cfg.SHEETS.RECIPES}!A2:F, "select Col1,Col2,Col3,Col4,Col5,Col6 where Col1 is not null", 0)`
    );

    // Helper tables (hidden) for counts
    // Dependencies count: L2:M
    sh.getRange("L1").setValue("dep_recipe_id");
    sh.getRange("M1").setValue("dep_count");
    sh.getRange("L2").setFormula(
      `=QUERY(${cfg.SHEETS.DEPENDENCIES}!A2:F, "select Col1, count(Col1) where Col1 is not null group by Col1 label count(Col1) ''", 0)`
    );

    // Calls out count: N2:O
    sh.getRange("N1").setValue("call_recipe_id");
    sh.getRange("O1").setValue("call_count");
    sh.getRange("N2").setFormula(
      `=QUERY(${cfg.SHEETS.CALL_EDGES}!A2:K, "select Col1, count(Col1) where Col1 is not null group by Col1 label count(Col1) ''", 0)`
    );

    // Dependencies (G)
    sh.getRange("G2").setFormula(
      `=ARRAYFORMULA(IF(A2:A="",,IFERROR(VLOOKUP(A2:A, L2:M, 2, FALSE), 0)))`
    );

    // Calls Out (H)
    sh.getRange("H2").setFormula(
      `=ARRAYFORMULA(IF(A2:A="",,IFERROR(VLOOKUP(A2:A, N2:O, 2, FALSE), 0)))`
    );

    // Has AI? (I)
    sh.getRange("I2").setFormula(
      `=ARRAYFORMULA(IF(A2:A="",,IF(IFERROR(COUNTIF(${cfg.SHEETS.AI_ANALYSIS}!A2:A, A2:A),0)>0, "YES", "")))`
    );

    // Has Maps? (J)
    sh.getRange("J2").setFormula(
      `=ARRAYFORMULA(IF(A2:A="",,IF(IFERROR(COUNTIF(${cfg.SHEETS.PROCESS_MAPS}!A2:A, A2:A),0)>0, "YES", "")))`
    );

    // Hide helper columns (L:O) so humans don’t see plumbing
    sh.hideColumns(12, 4); // L=12, hide L,M,N,O

    // Add a filter for humans
    try {
      const lastCol = 10;
      const lastRow = Math.max(2, sh.getLastRow());
      const range = sh.getRange(1, 1, lastRow, lastCol);
      if (!range.getFilter()) range.createFilter();
    } catch (e) {}

    // Reasonable widths
    try { sh.autoResizeColumns(1, 10); } catch (e) {}
  }

  static _ensureOutputSheets_(ss, ctx) {
    // These are referenced by View_Recipes + Dashboard_Home formulas
    this._ensureSheetWithHeaderKey_(ss, ctx, "AI_ANALYSIS", "AI_ANALYSIS");
    this._ensureSheetWithHeaderKey_(ss, ctx, "PROCESS_MAPS", "PROCESS_MAPS");
  }
  static _ensureSheetWithHeaderKey_(ss, ctx, sheetKey, headerKey) {
    const cfg = ctx.config;
    const sheetName = cfg.SHEETS[sheetKey];
    if (!sheetName) return;

    const headers = cfg.HEADERS[headerKey];
    const sh = ss.getSheetByName(sheetName) || ss.insertSheet(sheetName);

    // If sheet is empty, initialize header row (don’t clear existing data)
    if (headers && sh.getLastRow() === 0) {
      sh.getRange(1, 1, 1, headers.length).setValues([headers]);
      sh.getRange(1, 1, 1, headers.length)
        .setFontWeight("bold")
        .setBackground(cfg.CONSTANTS.STYLE_HEADER_BG || "#d9d9d9");
      sh.setFrozenRows(1);
    }
  }

  // ---------------------------------------------------------------------------------------
  // Protection helper
  // ---------------------------------------------------------------------------------------
  static _ensureProtection_(sheet, desc, warningOnly) {
    const protections = sheet.getProtections(SpreadsheetApp.ProtectionType.SHEET) || [];
    let p = protections.find(x => String(x.getDescription() || "") === desc);
    if (!p) {
      p = sheet.protect();
      p.setDescription(desc);
    }

    // Warning-only avoids permissions headaches in shared sheets
    p.setWarningOnly(Boolean(warningOnly));
  }
  static _getOrCreateSheet_(ss, name) {
    return ss.getSheetByName(name) || ss.insertSheet(name);
  }
}

// ---------------------------------------------------------------------------------------
// Manual entrypoints (optional but useful)
// ---------------------------------------------------------------------------------------
function rebuildDashboard() {
  const ctx = new AppContext();
  DashboardService.ensureAll(ctx, { last_sync_at: new Date().toISOString(), manual: true });
  SpreadsheetApp.getActiveSpreadsheet().toast("Dashboard rebuilt.", "Workato Sync", 3);
}

function applySheetVisibility() {
  const ctx = new AppContext();
  DashboardService.applyVisibility(ctx);
  SpreadsheetApp.getActiveSpreadsheet().toast("Sheet visibility applied.", "Workato Sync", 3);
}
