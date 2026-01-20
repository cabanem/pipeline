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
        PROTECT_BACKEND_WARNING_ONLY: true,   // “are you sure?” barrier without permission hassles
        SHOW_OUTPUT_SHEETS_IN_BASIC: false    // hide Output_* sheets
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



/** @file 04_Google_IO.gs */
/**
 * @class
 * @classdesc Service to encapsulate Google Sheets interactions.
 */
class SheetService {
  constructor() {
    this.config = AppConfig.get();
  }

  /**
   * Writes a 2D array of data to a specific Google Sheet.
   * Clears existing data and applies header formatting.
   * @param {string} sheetKey - Key corresponding to CONFIG.SHEETS.
   * @param {Array<Array<any>>} rows - The data to write.
   */
  write(sheetKey, rows) {
    const sheetName = this.config.SHEETS[sheetKey];
    if (!sheetName) throw new Error(`Sheet key ${sheetKey} not found in config.`);

    Logger.verbose(`Writing ${rows.length} rows to ${sheetName}...`);

    const ss = SpreadsheetApp.getActiveSpreadsheet();
    let sheet = ss.getSheetByName(sheetName) || ss.insertSheet(sheetName);

    sheet.clear();

    if (rows.length > 0) {
      const numRows = rows.length;
      const numCols = rows[0].length;
      
      // Batch write
      const fullRange = sheet.getRange(1, 1, numRows, numCols);
      fullRange.setValues(rows).setHorizontalAlignment("left");
      
      // Format header
      sheet.getRange(1, 1, 1, numCols)
           .setFontWeight("bold")
           .setBackground(this.config.CONSTANTS.STYLE_HEADER_BG)
           .setVerticalAlignment("middle");
      sheet.setFrozenRows(1);
      // Resize only if fewer than 5 columns 
      if (numCols > 1 && numCols < 5) { 
          try { sheet.autoResizeColumns(1, numCols); } catch(e){} 
      }
    }
  }
  /**
   * Reads a list of IDs from the request sheet.
   * @returns {string[]} Array of Recipe IDs found in column A.
   */
  readRequests() {
    const sheetName = this.config.SHEETS.LOGIC_INPUT;
    const headerText = this.config.HEADERS.LOGIC_INPUT[0];
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    let sheet = ss.getSheetByName(sheetName);

    // If sheet doesn't exist, create it and add headers
    if (!sheet) {
      const newSheet = ss.insertSheet(sheetName);
      newSheet.getRange(1, 1).setValue(headerText).setFontWeight("bold").setBackground("#fff2cc");
      return [];
    }

    // Evaluate header integrity
    const currentHeader = sheet.getRange(1, 1).getValue();
    if (currentHeader !== headerText) {
      Logger.notify(`Repairing header in '${sheetName}'...`);
      sheet.getRange(1, 1).setValue(headerText)
           .setFontWeight("bold").setBackground("#fff2cc");
    }

    const lastRow = sheet.getLastRow();
    if (lastRow < 2) return [];

    const values = sheet.getRange(2, 1, lastRow - 1, 1).getValues();
    return values.flat().filter(id => id).map(String);
  }
  /**
   * Appends rows to the debug sheet.
   * Assumes rows are already formatted/chunked by DataMapper.
   * @param {Array<Array<string>>} rows - The ready-to-write data.
   */
  appendDebugRows(rows) {
    if (!this.config.DEBUG.LOG_TO_SHEET || rows.length === 0) return;

    const sheetName = this.config.SHEETS.DEBUG;
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    let sheet = ss.getSheetByName(sheetName);

    // Initialize if absent
    if (!sheet) {
      sheet = ss.insertSheet(sheetName);
      sheet.getRange(1, 1, 1, this.config.HEADERS.DEBUG.length)
            .setValues([this.config.HEADERS.DEBUG])
            .setFontWeight("bold")
            .setBackground("#e6b8af");
      sheet.setFrozenRows(1);
    }

    const startRow = sheet.getLastRow() + 1;
    const numRows = rows.length;
    const maxCols = rows.reduce((m, r) => Math.max(m, (r ? r.length : 0)), 0);
    const normalized = rows.map(r => {
      const row = Array.isArray(r) ? r.slice() : [];
      while (row.length < maxCols) row.push("");
      return row;
    });
    sheet.getRange(startRow, 1, numRows, maxCols).setValues(normalized);
  }
  /** @returns {GoogleAppsScript.Spreadsheet.Spreadsheet} */
  getSpreadsheet() {
    return SpreadsheetApp.getActiveSpreadsheet();
  }
  /**
   * Get or create sheet by *name* (UI/service use).
   * @param {string} sheetName
   */
  getOrCreateByName(sheetName) {
    const ss = this.getSpreadsheet();
    return ss.getSheetByName(sheetName) || ss.insertSheet(sheetName);
  }
  /**
   * Get or create sheet by *config key* (e.g. "PROCESS_MAPS").
   * @param {string} sheetKey
   */
  getOrCreate(sheetKey) {
    const sheetName = this.config.SHEETS[sheetKey];
    if (!sheetName) throw new Error(`Sheet key ${sheetKey} not found in config.`);
    return this.getOrCreateByName(sheetName);
  }

}
/**
 * @class
 * @classdesc Service for handling file I/O with Google Drive.
 */
class DriveService{
  constructor() {
    this.config = AppConfig.get().DEBUG;
  }
  
  /**
   * Saves a JSON payload as a file to the configured debug folder.
   * @param {string|number} id - Recipe ID.
   * @param {string} name - Recipe name.
   * @param {Object} jsonObject - Data to save.
   * @returns {string} The URL of the created file.
   */
  saveLog(id, name, jsonObject) {
    if (!this.config.LOG_TO_DRIVE) return null;

    try {
      const folder = this._getVerifiedFolder();
      const safeName = (name || "Unknown").replace(/[^a-zA-Z0-9-_]/g, '_');
      const timestamp = Utilities.formatDate(new Date(), Session.getScriptTimeZone(), "yyyy-MM-dd_HH-mm-ss");
      const filename = `${timestamp}_ID-${id}_${safeName}.json`;

      let payloadToSave = { ...jsonObject }; // shallow copy
      if (payloadToSave.code && typeof payloadToSave.code === 'string') {
        try {
          payloadToSave.code = JSON.parse(payloadToSave.code);
        } catch (parseError) {
          console.warn(`DriveService: Could not parse 'code' string for recipe, ${id}. Saved as raw string.`);
        }
      }

      const content = JSON.stringify(payloadToSave, null, 2);
      const file = folder.createFile(filename, content, MimeType.PLAIN_TEXT);

      return file.getUrl();
    } catch (e) {
      console.error(`Drive save error: ${e.message}`);
      return null;
    }
  }
  /**
   * Saves arbitrary text conent to the configured debug folder.
   * @param {string|number} id - Root identifier (recipe ID).
   * @param {string} name - Human readable recipe name.
   * @param {string} ext - File extension.
   * @param {string} content - File contents.
   * @returns {string|null} The URL of the created file, or null on failure.
   */
  saveText(id, name, ext, content) {
    if (!this.config.LOG_TO_DRIVE) return null;
    try {
      const folder = this._getVerifiedFolder();
      const safeName = (name || "Unknown").replace(/[^a-zA-Z0-9-_]/g, '_');
      const timestamp = Utilities.formatDate(new Date(), Session.getScriptTimeZone(), "yyyy-MM-dd_HH-mm-ss");
      const filename = `${timestamp}_ID-${id}_${safeName}.${ext || "txt"}`;
      const file = folder.createFile(filename, String(content || ""), MimeType.PLAIN_TEXT);
      return file.getUrl();
    } catch(e) {
      console.error(`Drive saveText error: ${e.message}`);
      return null;
    }
  }
  // --- INTERNALS ---------------------------------------------------------------------------------------
  /** @private Retrieves folder by cached ID, or creates and updates cache */
  _getVerifiedFolder() {
    //const cachedId = this.props.getProperty('DEBUG_FOLDER_ID');
    const cachedId = ConfigStore.get('DEBUG_FOLDER_ID', { preferUser: true, defaultValue: "" });

    // 1. Try the cache
    if (cachedId) {
      try {
        return DriveApp.getFolderById(cachedId);
      } catch (e) {
        console.warn("Cached folder ID invalid. Rediscovering...");
      }
    }

    // 2. Search by name
    const folders = DriveApp.getFoldersByName(this.config.DRIVE_FOLDER_NAME);
    let folder;
    if (folders.hasNext()) {
      folder = folders.next();
    } else {
      // 3. Create
      folder = DriveApp.createFolder(this.config.DRIVE_FOLDER_NAME);
    }

    // 4. Update cache
    //this.props.setProperty('DEBUG_FOLDER_ID', folder.getId()); // if storing in scriptProps
    ConfigStore.setUser('DEBUG_FOLDER_ID', folder.getId()); // storing in userProps
    return folder;
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

    const ss = ctx.sheetService.getSpreadsheet();
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
    const showOutputs = Boolean(cfg.DASHBOARD.SHOW_OUTPUT_SHEETS_IN_BASIC);

    const visibleInBasic = new Set([
      cfg.SHEETS.DASHBOARD_HOME,
      cfg.SHEETS.VIEW_RECIPES,
      ...(showOutputs ? [cfg.SHEETS.AI_ANALYSIS, cfg.SHEETS.PROCESS_MAPS] : [])
    ].filter(Boolean));

    // If we're about to hide the active sheet, switch to Dashboard first
    if (!isAdv) {
      const active = ss.getActiveSheet();
      if (active && !visibleInBasic.has(active.getName())) {
        const dash = ss.getSheetByName(cfg.SHEETS.DASHBOARD_HOME) || ss.getSheets()[0];
        if (dash) ss.setActiveSheet(dash);
      }
    }

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
    const sh = ctx.sheetService.getOrCreateByName(name);

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
    DashboardService._setSheetLink_(sh, ss, "D4", cfg.SHEETS.VIEW_RECIPES, "Go to View_Recipes");
    DashboardService._setSheetLink_(sh, ss, "D5", cfg.SHEETS.AI_ANALYSIS, "Go to Output_AI_Analysis");
    DashboardService._setSheetLink_(sh, ss, "D6", cfg.SHEETS.PROCESS_MAPS, "Go to Output_Process_Maps");

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
    const sh = ctx.sheetService.getOrCreateByName(name);

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
    const sh = ctx.sheetService.getOrCreate(sheetKey);

    // If sheet is empty, initialize header row (don’t clear existing data)
    if (headers && sh.getLastRow() === 0) {
      sh.getRange(1, 1, 1, headers.length).setValues([headers]);
      sh.getRange(1, 1, 1, headers.length)
        .setFontWeight("bold")
        .setBackground(cfg.CONSTANTS.STYLE_HEADER_BG || "#d9d9d9");
      sh.setFrozenRows(1);
    }
  }
  static _setSheetLink_(dashSheet, ss, cellA1, targetSheetName, label) {
    const cell = dashSheet.getRange(cellA1);

    if (!targetSheetName) {
      cell.setValue("Missing target sheet name");
      return;
    }

    const target = ss.getSheetByName(targetSheetName);
    if (!target) {
      cell.setValue(`Missing sheet: ${targetSheetName}`);
      return;
    }

    const gid = target.getSheetId();
    const safeLabel = String(label || targetSheetName).replace(/"/g, '""'); // escape quotes for formulas
    cell.setFormula(`=HYPERLINK("#gid=${gid}", "${safeLabel}")`);
  }
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
