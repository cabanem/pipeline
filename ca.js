/**
 * @file 01_Core_Config.gs
 */
// -------------------------------------------------------------------------------------------------------
// CONFIGURATION
// -------------------------------------------------------------------------------------------------------
/**
 * @typedef {Object} APIConfig
 * @property {string} TOKEN - The Workato API bearer token.
 * @property {string} BASE_URL - The Workato API endpoint.
 * @property {number} PER_PAGE - Records per request.
 * @property {number} MAX_CALLS - Safety limit for recursive API calls.
 * @property {number} THROTTLE_MS - Delay (ms) between heavy processing loops.
 * @property {number} RECIPE_LIMIT_DEBUG - Limit on how many recipes to process.
 */

/**
 * @typedef {Object} AppConfigObject
 * @property {APIConfig} API - API connection settings.
 * @property {Object.<string, string>} SHEETS - Mapping of resource types to sheet names.
 * @property {Object.<string, string[]>} HEADERS - Definitions of column headers.
 * @property {Object} CONSTANTS - Internal constants for parsing logic and styling.
 * @property {boolean} VERBOSE - Toggle for detailed logging.
 */
/**

/**
 * @class
 * @classdesc Static container for application schema definitions.
 */
class SchemaDef {
  /**
   * Defines the user-facing names of the Google Sheets tabs.
   * Keys correspond to internal reference IDs used in AppConfig.
   */
  static get SHEETS() {
    return {
      DASHBOARD_HOME: "Dashboard_Home",
      VIEW_RECIPES: "View_Recipes",

      PROJECTS: "Inventory_Projects",
      FOLDERS: "Inventory_Folders",
      RECIPES: "Inventory_Recipes",
      PROPERTIES: "Inventory_Properties",
      TABLES: "Inventory_Data_Tables",
      LOOKUP_TABLES: "Inventory_Lookup_Tables",
      DEPENDENCIES: "Analysis_Dependencies",
      CALL_EDGES: "Analysis_Call_Edges",
      LOGIC: "Debug_Recipe_Logic",
      DEBUG: "System_Logs",
      LOGIC_INPUT: "Input_Requests",
      AI_ANALYSIS: "Output_AI_Analysis",
      PROCESS_MAPS: "Output_Process_Maps"
    };
  }
  /**
   * Defines the column headers for every sheet type.
   * ORDER MATTERS: These must match the order of elements produced in DataMapper.
   */
  static get HEADERS() {
    return {
      // ViewRecipes
      VIEW_RECIPES: [ "Recipe ID", "Name", "Status", "Project", "Folder", "Last run at", "# Dependencies", "# Calls out", "Has AI?", "Has maps?" ],
      // InventoryService -> DataMapper.mapProjectsToRows
      PROJECTS: [ "Project ID", "Name", "Description", "Created At" ],

      // InventoryService -> DataMapper.mapFoldersToRows
      FOLDERS: [ "Folder ID", "Name", "Parent Folder", "Project" ],

      // InventoryService -> DataMapper.mapRecipesToRows
      RECIPES: [ "Recipe ID", "Name", "Status", "Project", "Folder", "Last Run At" ],

      // InventoryService -> DataMapper.mapPropertiesToRows
      PROPERTIES: [ "Property ID", "Name", "Value", "Created At", "Updated At" ],

      // AnalyzerService -> DataMapper.mapDependenciesToRows
      DEPENDENCIES: [ "Parent Recipe ID", "Project", "Folder", "Dependency Type", "Dependency ID", "Dependency Name" ],
      TABLES: [ "Table ID", "Name", "Description", "Columns", "Record count", "Updated at" ],
      LOOKUP_TABLES: [ "Table ID", "Name", "Description", "Columns", "Record count", "Updated at" ],

      // AnalyzerService -> DataMapper.mapCallEdgesToRows
      CALL_EDGES: [
        "Parent Recipe ID",
        "Parent Recipe Name",
        "Project",
        "Folder",
        "Step Path",
        "Step Name",
        "Branch Context",
        "Provider",
        "Child Recipe ID",
        "Child Recipe Name",
        "ID Key"
      ],

      // AnalyzerService -> DataMapper via parseLogicRows
      // [recipeId, recipeName, step#, indent, provider, actionName, description, details]
      LOGIC: [
        "Recipe ID",
        "Recipe Name",
        "Step #",
        "Indentation",
        "Provider",
        "Action",
        "Description",
        "Details/Code"
      ],

      // SheetService.readRequests uses index 0 of this array for validation
      LOGIC_INPUT: [ "Recipe ID (Input List)"  ],

      // SheetService.appendDebugRows -> DataMapper.mapDebugLogsToRows
      DEBUG: [ "Timestamp", "Recipe ID", "Recipe Name", "Status", "Drive Link", "JSON Payload" ],

      // GeminiService -> WorkatoSyncApp.runAiAnalysis
      AI_ANALYSIS: [
        "Recipe ID",
        "Recipe Name",
        "Objective",
        "Trigger",
        "High Level Flow",
        "Hotspots",
        "External Apps",
        "Called Recipes",
        "Risks & Notes",
        "Structured Preview",
        "Graph Metrics",
        "Link: AI Analysis",
        "Link: Call Graph",
        "Link: Full Graph",
        "Timestamp"
      ],

      // WorkatoSyncApp.runProcessMaps
      PROCESS_MAPS: [
        "Root Recipe ID",
        "Root Name",
        "Mode",
        "Depth",
        "Call Graph (Mermaid)",
        "Process Graph (Mermaid)",
        "Generation Notes",
        "Link: Call Graph",
        "Link: Full Graph",
        "Timestamp"
      ]
    };
  }
  /**
   * System-wide constants used for styling, limits, and parsing configuration.
   */
  static get CONSTANTS() {
    return {
      // Formatting
      STYLE_HEADER_BG: "#d9d9d9", // Standard Light Grey
      
      // Parsing & Generation
      MERMAID_LABEL_MAX: 60,      // Max chars for a node label in Mermaid diagrams
      
      // Google Sheets Limits
      // Sheets has a cell limit of 50,000 characters. We set safety buffer.
      CELL_CHAR_LIMIT: 48000      
    };
  }
}
/**
 * @class
 * @classdesc Static configuration container.
 * * Centralizes all settings, constants, and API parameters.
 */
class AppConfig {
  /**
   * Retrieves the current configuration object.
   * @returns {AppConfigObject} The full application configuration.
   */
  static get() {
    const scriptProps = PropertiesService.getScriptProperties();

    return {
      API: {
        // User-specific overrides first, then script defaults
        TOKEN: ConfigStore.get('WORKATO_TOKEN', { preferUser: true, defaultValue: "" }),
        BASE_URL: (ConfigStore.get('WORKATO_BASE_URL', {
          preferUser: true,
          defaultValue: 'https://app.eu.workato.com/api'
        }) || 'https://app.eu.workato.com/api').replace(/\/$/, ''),
        PER_PAGE: 100,
        MAX_CALLS: 500,
        THROTTLE_MS: 100,       
        RECIPE_LIMIT_DEBUG: 100,
        // Transitive recipe-call depth
        PROCESS_MAP_DEPTH: 3,

        // Full process maps (step-level)
        PROCESS_MAP_MODE_DEFAULT: "calls+full", // "calls" | "full" | "calls+full"
        PROCESS_MAP_MAX_NODES: 250,            // safety cap for step-graph size
        PROCESS_MAP_EXPORT_TABLES: true,       // write PROCESS_NODES/PROCESS_EDGES sheets

        MAX_RETRIES: 3
      },
      SHEETS: SchemaDef.SHEETS,
      HEADERS: SchemaDef.HEADERS,
      CONSTANTS: SchemaDef.CONSTANTS,
      DEBUG: {
        ENABLE_LOGGING: true,
        LOG_TO_SHEET: true,
        LOG_TO_DRIVE: true,
        DRIVE_FOLDER_NAME: "workato_workspace_debug_logs"
      },
      VERTEX: {
        //GOOGLE_CLOUD_PROJECT_ID: scriptProps.getProperty('GOOGLE_CLOUD_PROJECT_ID'),
        GOOGLE_CLOUD_PROJECT_ID: ConfigStore.get('GOOGLE_CLOUD_PROJECT_ID', { preferUser: false, defaultValue: "" }),
        MODEL_ID: 'gemini-2.5-pro',
        LOCATION: 'us-central1',
        GENERATION_CONFIG: {
          TEMPERATURE: 0.2,
          MAX_OUTPUT_TOKENS: 10000 // 5000 is too few
        },
        PROMPT_MAX_CHARS: 60000,
        MERMAID_PROMPT_MAX_CHARS: 120000,
        LOGIC_DIGEST_MAX_LINES: 220,
        MAX_RETRIES: 3
      },
      DASHBOARD: {
        ENABLE: true,
        OVERWRITE_VIEWS: true,                // re-write formulas/headesrs
        HIDE_BACKEND_IN_BASIC: true,          // hide raw tabs when UiMode is basic
        PROTECT_BACKEND_WARNING_ONLY: true    // “are you sure?” barrier without permission hassles
      },
      VERBOSE: true
    };
  }
}
/**
 * @class
 * @classdesc Configuration store
 */
class ConfigStore {
  static userProps() { return PropertiesService.getUserProperties(); }
  static scriptProps() { return PropertiesService.getScriptProperties(); }

  /**
   * Get a property with precedence control.
   * @param {string} key
   * @param {{ preferUser?: boolean, defaultValue?: any }} [opts]
   */
  static get(key, opts = {}) {
    const preferUser = (opts.preferUser !== undefined) ? Boolean(opts.preferUser) : true;
    const u = this.userProps().getProperty(key);
    const s = this.scriptProps().getProperty(key);
    const def = (opts.defaultValue !== undefined) ? opts.defaultValue : null;
    return preferUser ? (u ?? s ?? def) : (s ?? u ?? def);
  }

  static setUser(key, value) {
    this.userProps().setProperty(key, String(value ?? ""));
  }
  static setScript(key, value) {
    this.scriptProps().setProperty(key, String(value ?? ""));
  }
  static deleteUser(key) {
    this.userProps().deleteProperty(key);
  }
  static deleteScript(key) {
    this.scriptProps().deleteProperty(key);
  }
}

/**
 * @file 30_DashboardService.gs
 * @description Creates/refreshes dashboard + view tabs and manages visibility/protection.
 */

class DashboardService {
  static ensureAll(ctx, stats = null) {
    const cfg = ctx.config;
    if (!cfg.DASHBOARD || !cfg.DASHBOARD.ENABLE) return;

    const ss = SpreadsheetApp.getActiveSpreadsheet();

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
      `=ARRAYFORMULA(IF(A2:A="",,IF(COUNTIF(${cfg.SHEETS.AI_ANALYSIS}!A2:A, A2:A)>0, "YES", "")))`
    );

    // Has Maps? (J)
    sh.getRange("J2").setFormula(
      `=ARRAYFORMULA(IF(A2:A="",,IF(COUNTIF(${cfg.SHEETS.PROCESS_MAPS}!A2:A, A2:A)>0, "YES", "")))`
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


/**
 * @file 10_Feature_Runners.gs
 * @description Feature runners. These own orchestration per capability.
 */

class InventorySyncRunner {
  run(ctx) {
    try {
      ctx.logger.verbose("Starting full workspace sync...");

      const currentUser = ctx.inventoryService.getCurrentUser();
      if (currentUser) console.log(`Authenticated as ${currentUser.name || "Unknown user"}`);

      const projects = ctx.inventoryService.getProjects();
      const folders = ctx.inventoryService.getFoldersRecursive(projects);
      const recipes = ctx.inventoryService.getRecipes();
      const properties = ctx.inventoryService.getProperties();

      // If you added tables in your previous PR, keep these lines.
      const dataTables = ctx.inventoryService.getDataTables ? ctx.inventoryService.getDataTables() : [];
      const lookupTables = ctx.inventoryService.getLookupTables ? ctx.inventoryService.getLookupTables() : [];

      ctx.logger.verbose(
        `Fetched totals: ${projects.length} projects, ${folders.length} folders, ${recipes.length} recipes, ${properties.length} properties, ${dataTables.length} data tables, ${lookupTables.length} lookup tables`
      );

      // Lookup maps
      const projectMap = AppHelpers.createLookupMap(projects);
      const folderMap = AppHelpers.createLookupMap(folders);
      const recipeNameMap = AppHelpers.createLookupMap(recipes);
      const dataTableMap = AppHelpers.createLookupMap(dataTables);
      const lookupTableMap = AppHelpers.createLookupMap(lookupTables);
      const tableNameMap = { ...dataTableMap, ...lookupTableMap };

      const cfg = ctx.config;

      const projectRows = [cfg.HEADERS.PROJECTS, ...DataMapper.mapProjectsToRows(projects)];
      const folderRows = [cfg.HEADERS.FOLDERS, ...DataMapper.mapFoldersToRows(folders, folderMap, projectMap)];
      const recipeRows = [cfg.HEADERS.RECIPES, ...DataMapper.mapRecipesToRows(recipes, projectMap, folderMap)];
      const propertyRows = [cfg.HEADERS.PROPERTIES, ...DataMapper.mapPropertiesToRows(properties)];

      // Tables (only if you implemented those mappers/headers)
      const dataTableRows = cfg.HEADERS.TABLES && DataMapper.mapDataTablesToRows
        ? [cfg.HEADERS.TABLES, ...DataMapper.mapDataTablesToRows(dataTables, folderMap)]
        : null;

      const lookupTableRows = cfg.HEADERS.LOOKUP_TABLES && DataMapper.mapLookupTablesToRows
        ? [cfg.HEADERS.LOOKUP_TABLES, ...DataMapper.mapLookupTablesToRows(lookupTables, projectMap)]
        : null;

      // Dependencies + call edges
      let dependencyRows = [cfg.HEADERS.DEPENDENCIES];
      let callEdgeRows = [cfg.HEADERS.CALL_EDGES];
      const depLimit = cfg.API.RECIPE_LIMIT_DEBUG;

      recipes.forEach((recipe, index) => {
        if (index >= depLimit) return;

        const rawDeps = ctx.analyzerService.getDependencies(recipe.id);
        if (rawDeps.length > 0) {
          const rows = DataMapper.mapDependenciesToRows.length >= 5
            ? DataMapper.mapDependenciesToRows(recipe, rawDeps, projectMap, folderMap, tableNameMap)
            : DataMapper.mapDependenciesToRows(recipe, rawDeps, projectMap, folderMap);
          dependencyRows = dependencyRows.concat(rows);
        }

        const callEdges = ctx.analyzerService.getCallEdges(recipe.id);
        if (callEdges.length > 0) {
          callEdgeRows = callEdgeRows.concat(
            DataMapper.mapCallEdgesToRows(recipe, callEdges, projectMap, folderMap, recipeNameMap)
          );
        }

        if (index % 10 === 0) Utilities.sleep(50);
      });

      ctx.logger.verbose("Writing to Sheets...");
      ctx.sheetService.write("PROJECTS", projectRows);
      ctx.sheetService.write("FOLDERS", folderRows);
      ctx.sheetService.write("RECIPES", recipeRows);
      ctx.sheetService.write("PROPERTIES", propertyRows);

      if (dataTableRows) ctx.sheetService.write("TABLES", dataTableRows);
      if (lookupTableRows) ctx.sheetService.write("LOOKUP_TABLES", lookupTableRows);

      ctx.sheetService.write("DEPENDENCIES", dependencyRows);
      ctx.sheetService.write("CALL_EDGES", callEdgeRows);

      ctx.logger.notify("Sync complete. Workspace inventory updated...", false);

      // Dashboard automation (UX-only)
      if (ctx.config.DASHBOARD && ctx.config.DASHBOARD.ENABLE) {
        DashboardService.postInventorySync(ctx, {
          projects: projects.length,
          folders: folders.length,
          recipes: recipes.length,
          properties: properties.length,
          data_tables: dataTables.length,
          lookup_tables: lookupTables.length,
          dependencies: Math.max(0, dependencyRows.length - 1), // exclude header
          call_edges: Math.max(0, callEdgeRows.length - 1)      // exclude header
        });
      }
    } catch (e) {
      AppHelpers.handleError(e);
    }
  }
}

class LogicDebugRunner {
  run(ctx, idsOverride = null) {
    try {
      ctx.logger.verbose("Starting recipe logic debugging...");

      const requestedIds = (Array.isArray(idsOverride) && idsOverride.length > 0)
        ? idsOverride
        : ctx.sheetService.readRequests();

      if (requestedIds.length === 0) {
        ctx.logger.notify("No recipe IDs found (select rows with IDs, or use 'logic_requests').", true);
        return;
      }
      ctx.logger.notify(`Fetching logic for ${requestedIds.length} recipes...`);

      const logicRows = [ctx.config.HEADERS.LOGIC];
      const debugLogs = [];

      requestedIds.forEach((reqId, index) => {
        try {
          const fullRecipe =
            ctx.analyzerService.getRecipeDetails(reqId) ||
            ctx.client.get(`recipes/${reqId}`);
          const recipeName = fullRecipe.name || "Unknown";

          // A. Save to Drive
          let driveUrl = "";
          if (ctx.config.DEBUG.LOG_TO_DRIVE) {
            driveUrl = ctx.driveService.saveLog(reqId, fullRecipe.name, fullRecipe);
          }

          // B. Emit to Sheet
          if (ctx.config.DEBUG.LOG_TO_SHEET) {
            debugLogs.push({ id: reqId, name: recipeName, driveUrl: driveUrl });
          }

          // C. Parse
          const parsedRows = ctx.analyzerService.parseLogicRows(fullRecipe);
          logicRows.push(...parsedRows);

        } catch (e) {
          console.warn(`Failed ID ${reqId}: ${e.message}`);
          logicRows.push([reqId, "ERROR", "-", "-", "-", "-", String(e.message || e), "-"]);
        }

        if (index % 5 === 0) Utilities.sleep(ctx.config.API.THROTTLE_MS);
      });

      ctx.logger.verbose("Writing data to sheets...");
      ctx.sheetService.write("LOGIC", logicRows);

      const debugRows = DataMapper.mapDebugLogsToRows(debugLogs);
      ctx.sheetService.appendDebugRows(debugRows);

      ctx.logger.notify("Logic debugging complete.");
    } catch (e) {
      AppHelpers.handleError(e);
    }
  }
}

class AiAnalysisRunner {
  run(ctx, idsOverride = null) {
    const gemini = new GeminiService();
    const ids = (Array.isArray(idsOverride) && idsOverride.length > 0)
      ? idsOverride
      : ctx.sheetService.readRequests();

    if (ids.length === 0) {
      ctx.logger.notify("No recipe IDs found (select rows with IDs, or use 'logic_requests').");
      return;
    }

    const cfg = ctx.config;
    const charLimit = cfg.CONSTANTS.CELL_CHAR_LIMIT || 48000;
    const maxLines = Number(cfg.VERTEX.LOGIC_DIGEST_MAX_LINES || 220);
    const depth = Number(cfg.API.PROCESS_MAP_DEPTH ?? 2);
    const maxNodes = Number(cfg.API.PROCESS_MAP_MAX_NODES ?? 250);

    const rows = [cfg.HEADERS.AI_ANALYSIS];

    ids.forEach((id, idx) => {
      ctx.logger.notify(`Asking Gemini to analyze Recipe ${id}...`);
      try {
        const recipe =
          ctx.analyzerService.getRecipeDetails(id) ||
          ctx.client.get(`recipes/${id}`);
        const name = recipe?.name || "";

        const graphPack = ctx.analyzerService.buildGraphPack(id, { callDepth: depth, maxNodes, edgeSampleLimit: 70 });

        const logicRows = ctx.analyzerService.parseLogicRows(recipe);
        const digest = AppHelpers.logicDigestFromRows(logicRows, maxLines);

        const structured = gemini.explainRecipeStructured(recipe, graphPack, digest);

        const objective = String(structured.objective || "");
        const trigger = String(structured.trigger || "");
        const flow = Array.isArray(structured.high_level_flow) ? structured.high_level_flow.join("\n") : String(structured.high_level_flow || "");
        const hotspots = Array.isArray(structured.hotspots) ? structured.hotspots.join("\n") : String(structured.hotspots || "");
        const externalApps = Array.isArray(structured.external_apps) ? structured.external_apps.join("\n") : String(structured.external_apps || "");
        const calledRecipes = Array.isArray(structured.called_recipes) ? structured.called_recipes.join("\n") : String(structured.called_recipes || "");
        const risks = Array.isArray(structured.risks_notes) ? structured.risks_notes.join("\n") : String(structured.risks_notes || "");

        const rawPreview = JSON.stringify(structured, null, 2).slice(0, 4000);

        const aiUrl = ctx.driveService.saveText(id, name || `recipe_${id}`, "ai.json", JSON.stringify(structured, null, 2));
        const callsUrl = ctx.driveService.saveText(id, name || `recipe_${id}`, "calls.mmd", graphPack?.call?.mermaid || "");
        const fullUrl  = ctx.driveService.saveText(id, name || `recipe_${id}`, "full.mmd",  graphPack?.process?.mermaid || "");

        const aiLink    = aiUrl    ? `=HYPERLINK("${aiUrl}", "View AI full")` : "";
        const callsLink = callsUrl ? `=HYPERLINK("${callsUrl}", "View calls mermaid")` : "";
        const fullLink  = fullUrl  ? `=HYPERLINK("${fullUrl}", "View full mermaid")` : "";

        const preview = rawPreview.length >= 4000
          ? rawPreview + "\n…(truncated preview; see Drive link)"
          : rawPreview;

        const metricsJson = JSON.stringify({
          call: {
            depth: graphPack?.call?.depth,
            node_count: graphPack?.call?.node_count,
            edge_count: graphPack?.call?.edge_count
          },
          process: {
            node_count: graphPack?.process?.node_count,
            edge_count: graphPack?.process?.edge_count,
            kind_counts: graphPack?.process?.kind_counts,
            call_targets: graphPack?.process?.call_targets
          }
        });

        rows.push([
          String(recipe?.id || id),
          name,
          objective,
          trigger,
          flow,
          hotspots,
          externalApps,
          calledRecipes,
          risks,
          preview,
          metricsJson.length > charLimit ? metricsJson.slice(0, 2000) + "…(truncated)" : metricsJson,
          aiLink,
          callsLink,
          fullLink,
          new Date().toISOString()
        ]);

      } catch (e) {
        console.error(e);
        const errRow = Array(cfg.HEADERS.AI_ANALYSIS.length).fill("");
        errRow[0] = String(id);
        errRow[1] = "Error";
        errRow[2] = String(e.message || e);
        errRow[14] = new Date().toISOString();
        rows.push(errRow);
      }

      if (idx % 2 === 0) Utilities.sleep(cfg.API.THROTTLE_MS);
    });

    ctx.sheetService.write("AI_ANALYSIS", rows);
    ctx.logger.notify("AI analysis complete.");
  }
}

class ProcessMapsRunner {
  run(ctx, options = {}, idsOverride = null) {
    try {
      ctx.logger.verbose("Starting process map generation (v2 - Library)...");

      const requestedIds = (Array.isArray(idsOverride) && idsOverride.length > 0)
        ? idsOverride
        : ctx.sheetService.readRequests();

      if (requestedIds.length === 0) {
        ctx.logger.notify("No recipe IDs found (select rows with IDs, or use 'logic_requests').", true);
        return;
      }

      const mode = String(options.mode || ctx.config.API.PROCESS_MAP_MODE_DEFAULT || "calls+full");
      const depth = Number(options.callDepth ?? ctx.config.API.PROCESS_MAP_DEPTH ?? 0);
      const maxNodes = Number(options.maxNodes ?? ctx.config.API.PROCESS_MAP_MAX_NODES ?? 250);
      const CHAR_LIMIT = ctx.config.CONSTANTS.CELL_CHAR_LIMIT || 48000;

      const rows = [ctx.config.HEADERS.PROCESS_MAPS];

      requestedIds.forEach((rootId, idx) => {
        const pack = ctx.analyzerService.buildGraphPack(rootId, { callDepth: depth, maxNodes: maxNodes });

        const rootName = pack.root_name || "";
        let callMermaid = "";
        let fullMermaid = "";
        let notes = [];
        let callDriveLink = "";
        let fullDriveLink = "";

        if (mode.includes("calls")) {
          callMermaid = pack.call.mermaid || "";
          notes = notes.concat(pack.call.notes || []);

          if (callMermaid.length > CHAR_LIMIT) {
            const url = ctx.driveService.saveText(rootId, rootName, "calls.mmd", callMermaid);
            callDriveLink = url ? `=HYPERLINK("${url}", "View calls mermaid")` : "Save failed";
            callMermaid = callMermaid.substring(0, CHAR_LIMIT - 200) + "\n...(TRUNCATED)";
            notes.push("Calls mermaid truncated.");
          }
        }

        if (mode.includes("full")) {
          fullMermaid = pack.process.mermaid || "";
          notes = notes.concat(pack.process.notes || []);

          if (fullMermaid.length > CHAR_LIMIT) {
            const url = ctx.driveService.saveText(rootId, rootName, "full.mmd", fullMermaid);
            fullDriveLink = url ? `=HYPERLINK("${url}", "View full mermaid")` : "Save failed";
            fullMermaid = fullMermaid.substring(0, CHAR_LIMIT - 200) + "\n...(TRUNCATED)";
            notes.push("Full mermaid truncated.");
          }
        }

        rows.push([
          String(rootId),
          rootName,
          mode,
          String(depth),
          callMermaid,
          fullMermaid,
          notes.slice(0, 20).join("\n"),
          callDriveLink,
          fullDriveLink,
          new Date().toISOString()
        ]);

        if (idx % 2 === 0) Utilities.sleep(ctx.config.API.THROTTLE_MS);
      });

      ctx.sheetService.write("PROCESS_MAPS", rows);
      ctx.logger.notify("Process maps generated.");
    } catch (e) {
      AppHelpers.handleError(e);
    }
  }
}


/**
 * @file 21_UI_Menu.gs
 */
// -------------------------------------------------------------------------------------------------------
// USER INTERFACE
// -------------------------------------------------------------------------------------------------------
/**
 * @class
 * @classdesc Manages all direct interactions with the user (Menus, Prompts, Alerts).
 * Encapsulates UI logic to keep the global namespace clean.
 */
class UserInterfaceService {
  constructor() {
    this.ui = SpreadsheetApp.getUi();
    // this.props = PropertiesService.getScriptProperties(); // scriptProps
    this.scriptProps = PropertiesService.getScriptProperties();
    this.userProps = PropertiesService.getUserProperties();
  }

  /**
   * Builds and displays the custom menu.
   */
  createMenu() {
    const isAdv = (typeof UiMode !== "undefined") ? UiMode.isAdvanced() : false;

    const root = this.ui.createMenu("Workato Sync");

    // --- Quick Actions (always visible) -------------------------------------
    root.addSubMenu(
      this.ui.createMenu("Quick Actions")
        .addItem("Run workspace inventory sync", "syncInventory")
        .addSeparator()
        .addItem("Analyze selected rows using AI", "fetchRecipeAnalysisSelected")
        .addItem("Generate process maps for selection (calls + full)", "generateProcessMapsSelected")
    );

    // --- Selection-driven tools (always visible) ----------------------------
    root.addSubMenu(
      this.ui.createMenu("Selection-driven")
        .addItem("Debug logic for selected rows", "fetchRecipeLogicSelected")
        .addItem("Analyze selected rows using AI", "fetchRecipeAnalysisSelected")
        .addSeparator()
        .addItem("Generate process maps for selection (calls only)", "generateProcessMapsSelectedCalls")
        .addItem("Generate process maps for selection (full only)", "generateProcessMapsSelectedFull")
        .addItem("Generate process maps for selection (calls + full)", "generateProcessMapsSelected")
    );

    // --- Advanced tools ------------------------------------------------------
    if (isAdv) {
      root.addSubMenu(
        this.ui.createMenu("Advanced")
          .addSubMenu(
            this.ui.createMenu("Requests-sheet driven")
              .addItem("Debug logic (from Input_Requests)", "fetchRecipeLogic")
              .addItem("Analyze using AI (from Input_Requests)", "fetchRecipeAnalysis")
              .addSeparator()
              .addItem("Generate process maps (calls only)", "generateProcessMapsCalls")
              .addItem("Generate process maps (full only)", "generateProcessMapsFull")
              .addItem("Generate process maps (calls + full)", "generateProcessMaps")
          )
          .addSeparator()
          .addItem("Test connectivity", "testWorkatoConnectivity")
          .addSeparator()
          .addSubMenu(
            this.ui.createMenu("Diagnostics")
              .addItem("Debug property report (logs)", "debugPropertyReport")
              .addItem("Migrate scriptProps → userProps", "migrateMyScriptPropsToUserProps")
          )
      );
    }

    // --- Configuration (always visible) -------------------------------------
    root.addSeparator();
    root.addSubMenu(
      this.ui.createMenu("Configuration")
        .addItem("Set Workato API token", "promptToken")
        .addItem("Set base URL", "promptBaseUrl")
        .addItem("Set debug folder ID", "promptFolderId")
        .addSeparator()
        .addItem("Show current config", "showCurrentConfig")
        .addSeparator()
        .addItem("Rebuild dashboard & views", "rebuildDashboard")
        .addItem("Apply sheet visibility", "applySheetVisibility")
    );

    // --- Mode toggle (always visible) ---------------------------------------
    root.addSeparator();
    if (isAdv) root.addItem("Switch to Basic menu", "setUiModeBasic");
    else root.addItem("Switch to Advanced menu", "setUiModeAdvanced");

    root.addToUi();
  }

  /**
   * Prompts the user to update a specific script property.
   * Handles validation, user cancellation, and masking of secrets.
   * @param {string} key - The ScriptProperty key to update.
   * @param {string} title - The title of the prompt dialog.
   * @param {{ isSecret?: boolean, scope?: "user"|"script" }} [opts]
   */
  promptUpdate(key, title, opts = {}) {
    const isSecret = Boolean(opts.isSecret);
    const scope = (opts.scope === "script") ? "script" : "user";
    const result = this.ui.prompt(title, `Enter new value for ${key}:`, this.ui.ButtonSet.OK_CANCEL);

    if (result.getSelectedButton() === this.ui.Button.OK) {
      const input = result.getResponseText().trim();

      // Handle Empty Input (Delete vs Cancel)
      if (input === "") {
        const confirm = this.ui.alert(
          'Delete Property?', 
          `Input was empty. Do you want to DELETE the existing '${key}'?`, 
          this.ui.ButtonSet.YES_NO
        );
        if (confirm === this.ui.Button.YES) {
          if (scope === "script") this.scriptProps.deleteProperty(key);
          else this.userProps.deleteProperty(key);
          this.ui.alert('Property deleted. Script will use code-level defaults.');
        }
        return;
      }

      // Save
      if (scope === "script") this.scriptProps.setProperty(key, input);
      else this.userProps.setProperty(key, input);
      
      // Feedback
      const displayValue = isSecret 
        ? `${input.substring(0, 4)}...${input.substring(input.length - 4)}` 
        : input;
      
      this.ui.alert(`Saved ${key} (${scope}): ${displayValue}`);
    }
  }
  /**
   * Displays the current configuration state in a formatted alert.
   */
  showConfiguration() {
    const user = this.userProps.getProperties();
    const script = this.scriptProps.getProperties();
    const defaults = AppConfig.get().API;
    
    // Logic to determine display strings (Set vs Default vs Missing)
    const tokenStatus =
      user['WORKATO_TOKEN'] ? "******** (User)" :
      script['WORKATO_TOKEN'] ? "******** (Script)" :
      "❌ NOT SET";

    const urlStatus =
      user['WORKATO_BASE_URL'] ? `${user['WORKATO_BASE_URL']} (User)` :
      script['WORKATO_BASE_URL'] ? `${script['WORKATO_BASE_URL']} (Script)` :
      `${defaults.BASE_URL} (Default)`;

    const folderStatus =
      user['DEBUG_FOLDER_ID'] ? `${user['DEBUG_FOLDER_ID']} (User)` :
      script['DEBUG_FOLDER_ID'] ? `${script['DEBUG_FOLDER_ID']} (Script)` :
      "(Auto-generated)";
    
    const msg = [
      `API Token: ${tokenStatus}`,
      `Base URL: ${urlStatus}`,
      `Debug Folder ID: ${folderStatus}`,
      ``,
      `Precedence: User settings override Script settings.`
    ].join('\n');
    
    this.ui.alert('Current Configuration', msg, this.ui.ButtonSet.OK);
  }
  /**
   * Formats and displays connectivity test results.
   * @param {Array<string>} results - Array of status messages.
   */
  showConnectivityReport(results) {
    this.ui.alert("Connectivity Test Results", results.join("\n"), this.ui.ButtonSet.OK);
  }
}
