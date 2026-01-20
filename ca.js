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
 * @class
 * @classdesc Centralized definitions for Sheet names, Column Headers, and application constants.
 * This class serves as the single source of truth for the AppConfig class.
 */

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

// -------------------------------------------------------------------------------------------------------
// LOGGING
//-------------------------------------------------------------------------------------------------------
/**
 * @class
 * @classdesc Static utility for logging to both Apps Script console and Sheets UI.
 */
class Logger {
  /**
   * Logs a message to the console only if VERBOSE mode is enabled in Config.
   * @param {string} msg - The message to log.
   */
  static verbose(msg) {
    if (AppConfig.get().VERBOSE) console.log(`[VERBOSE] ${msg}`);
  }
  /**
   * Logs to console and displays a Toast notification in the active Spreadsheet.
   * @param {string} msg - The message to display.
   * @param {boolean} [isError=false] - If true, logs as console.error and styles toast as error.
   */
  static notify(msg, isError = false) {
    if (isError) console.error(msg);
    else console.log(msg);
    try {
      const ss = SpreadsheetApp.getActiveSpreadsheet();
      if (ss) ss.toast(msg, isError ? "Error" : "Success", 5);
    } catch (e) {
      // console.log("UI notification skipped.");
    }
  }
}

// -------------------------------------------------------------------------------------------------------
// WORKATO CLIENT
// -------------------------------------------------------------------------------------------------------
/**
 * @class
 */
class WorkatoClient {
  constructor() {
    const apiConfig = AppConfig.get().API;
    const verbose = AppConfig.get().VERBOSE;

    // Initialize external library dependency
    this.client = WorkatoLib.newClient(
      apiConfig.TOKEN,
      apiConfig.BASE_URL,
      {
        verbose: verbose,
        maxRetries: apiConfig.MAX_RETRIES,
        dryRun: false,
        perPage: apiConfig.PER_PAGE
      }
    );
  }

  get(endpoint) {
    return this.client.get(endpoint);
  }
  fetchPaginated(resourcePath) {
    return this.client.fetchPaginated(resourcePath);
  }
}

// -------------------------------------------------------------------------------------------------------
// DOMAIN SERVICE CLASSES
// -------------------------------------------------------------------------------------------------------
/**
 * @class
 * @classdesc Service responsible for fetching high-level Workato entities.
 * * Encapsulates logic for Projects, Recipes, Properties, and Folder Recursion.
 */
class InventoryService {
  /**
   * @param {WorkatoClient} client - An initialized API client instance.
   */
  constructor(client) {
    this.client = client;
    this.config = AppConfig.get();
  }
  /**
   * Fetches all available projects.
   * @returns {Array<Object>} List of project objects.
   */
  getProjects() { return this.client.fetchPaginated('projects'); }
  /**
   * Fetches all recipes.
   * @returns {Array<Object>} List of recipe objects.
   */
  getRecipes() { return this.client.fetchPaginated('recipes'); }
  /**
   * Fetches workspace properties.
   * * Safely handles errors (e.g., 403 Forbidden) if the user lacks permissions.
   * @returns {Array<Object>} List of property objects, or empty array on error.
   */
  getProperties() {
    try {
      return this.client.fetchPaginated('properties');
    } catch (e) {
      console.warn(`SKIPPING PROPERTIES: The API rejected the request (${e.message}).`);
      return [];
    }
  }
  /**
   * Fetches all lookup tables.
   * @returns {Array<object>} List of table objects.
   */
  getLookupTables() {
    return this._fetchPaginatedNormalized_('lookup_tables');
  }
  /**
   * Fetches all data tables.
   * @returns {Array<object>} List of table objects.
   */
  getDataTables() {
    return this._fetchPaginatedNormalized_('data_tables');
  }
  /**
   * Fetches the current authenticated user details.
   * @returns {Object|null} User profile object or null on failure.
   */
  getCurrentUser() {
    try {
      return this.client.get('users/me');
    } catch (e) {
      return null;
    }
  }
  /**
   * Recursively fetches all folders using a Hybrid Sync strategy.
   * 1. Scans Project Roots (folders?project_id=X)
   * 2. Scans Workspace Root (folders)
   * 3. Recursively scans children via queue (folders?parent_id=Y)
   * * @param {Array<Object>} projects - List of projects to seed the search.
   * @returns {Array<Object>} Comprehensive list of all folder objects.
   */
  getFoldersRecursive(projects) {
    let allFolders = [];
    let queue = [];
    let qIndex = 0; // FIFO without O(n) shift()
    let processedIds = new Set();
    const MAX_CALLS = this.config.API.MAX_CALLS;

    Logger.verbose(`Starting folder sync...`);

    // PHASE 1: Project Roots
    for (const project of projects) {
      // Note: We use the raw client.get here because these are single batch checks, not full pagination loops
      try {
        const potentialRoots = this._fetchFolderBatch(`folders?project_id=${project.id}`);
        const rootFolder = potentialRoots.find(f => f.project_id === project.id && f.is_project === true);

        if (rootFolder && !processedIds.has(rootFolder.id)) {
          allFolders.push(rootFolder);
          processedIds.add(rootFolder.id);
          queue.push(rootFolder.id);
        }
      } catch (e) {
        console.warn(`Failed to fetch root for project ${project.id}: ${e.message}`);
      }
    }

    // PHASE 2: Home Folders
    let globalFolders = [];
    try {
      // Prefer complete root set; fall back to legacy single-batch on error
      globalFolders = this.client.fetchPaginated('folders');
    } catch (e) {
      globalFolders = this._fetchFolderBatch('folders');
    }
    globalFolders.forEach(f => {
      if (!processedIds.has(f.id)) {
        allFolders.push(f);
        processedIds.add(f.id);
        queue.push(f.id);
      }
    });

    // PHASE 3: Recursion
    let safetyCounter = 0;
    while (qIndex < queue.length && safetyCounter < MAX_CALLS) {
      let parentId = queue[qIndex++];
      let page = 1;
      let hasMore = true;

      while (hasMore) {
        const url = `folders?parent_id=${parentId}&page=${page}&per_page=${this.config.API.PER_PAGE}`;
        const items = this._fetchFolderBatch(url);

        if (items.length > 0) {
          const newItems = items.filter(f => !processedIds.has(f.id));
          if (newItems.length > 0) {
            allFolders = allFolders.concat(newItems);
            newItems.forEach(f => {
              processedIds.add(f.id);
              queue.push(f.id);
            });
          }
          
          if (items.length < this.config.API.PER_PAGE) hasMore = false;
          else page++;
        } else {
          hasMore = false;
        }
        safetyCounter++;
      }
      
      if (safetyCounter % 20 === 0) Utilities.sleep(50);
    }

    console.log(`Sync complete. Found ${allFolders.length} total folders.`);
    return allFolders;
  }

  // --- INTERNALS ---------------------------------------------------------------------------------------
  /**
   * Helper to fetch a single batch of folders and normalize the response.
   * Accounts for API inconsistencies where folders may return as array or object wrapper.
   * * @param {string} endpoint - The API endpoint to fetch.
   * @returns {Array<Object>} Array of folder objects (empty if error).
   * @private
   */
  _fetchFolderBatch(endpoint) {
    try {
      const json = this.client.get(endpoint);
      return Array.isArray(json) ? json : (json.items || json.result || []);
    } catch (e) {
      // Original script returned empty array on error for folder batches
      return [];
    }
  }
  _fetchPaginatedNormalized_(resourcePath) {
    // 1) Prefer library pagination if it works
    try {
      const res = this.client.fetchPaginated(resourcePath);
      const arr = this._normalizeListResponse_(res);
      if (Array.isArray(arr)) return arr;
    } catch (e) {
      // fall through to manual paging
    }

    // 2) Manual paging (handles endpoints that wrap results under "data")
    const out = [];
    const perPage = Number(this.config.API.PER_PAGE || 100);
    const maxCalls = Number(this.config.API.MAX_CALLS || 500);

    let page = 1;
    let safety = 0;
    while (safety < maxCalls) {
      try {
        const endpoint = `${resourcePath}?page=${page}&per_page=${perPage}`;
        const json = this.client.get(endpoint);
        const items = this._normalizeListResponse_(json);
        if (!items || items.length === 0) break;
        out.push(...items);
        if (items.length < perPage) break;
        page++;
        safety++;
        if (safety % 10 === 0) Utilities.sleep(50);
      } catch (e) {
        console.warn(`SKIPPING ${resourcePath.toUpperCase()}: ${e.message}`);
        break;
      }
    }
    return out;
  }
  _normalizeListResponse_(json) {
    if (Array.isArray(json)) return json;
    if (!json || typeof json !== 'object') return [];
    // data tables list uses { data: [...] }
    if (Array.isArray(json.data)) return json.data;
    if (Array.isArray(json.items)) return json.items;
    if (Array.isArray(json.result)) return json.result;
    return [];
  }
}
/**
 * @class
 * @classdesc Unified service for deep inspection of recipe logic / code.
 * Merges functionality from historical DependencyService and LogicService.
 * RecipeAnalyzer: 1zQz8lK_00xJiyVweBiNUfhr54HqAGY0isdck0lQCYyr134Xmm7fx_ahW
 */
class RecipeAnalyzerService {
  /**
   * @param {WorkatoClient} client
   */
  constructor(client) {
    // 1. Dependency injection (pass client to new lib)
    this.engine = WorkatoGraphLib.newAnalyzer(client, {
      MERMAID_LABEL_MAX: AppConfig.get().CONSTANTS.MERMAID_LABEL_MAX
    });
  }

  // ----- Delegate methods -------------------------------------------------
  getDependencies(recipeId) {
    return this.engine.getDependencies(recipeId);
  }
  getCallEdges(recipeId) {
    return this.engine.getCallEdges(recipeId);
  }
  parseLogicRows(recipe) {
    return this.engine.parseLogicRows(recipe);
  }
  getRecipeDetails(recipeId) {
    return this.engine.getRecipeDetails(recipeId);
  }
  buildGraphPack(rootId, options) {
    return this.engine.buildGraphPack(rootId, options);
  }
}

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

// -------------------------------------------------------------------------------------------------------
// DATA MAPPER
// -------------------------------------------------------------------------------------------------------
/**
 * @class
 * @classdesc Pure utility class for transforming raw API data into 2D arrays for Google Sheets.
 * Decouples business logic (Controllers) from I/O logic (SheetService).
 */
class DataMapper {
  /**
   * Transforms Project objects into sheet rows.
   * @param {Array<Object>} projects - Raw API response.
   * @returns {Array<Array<string>>}
   */
  static mapProjectsToRows(projects) {
    return projects.map(p => [p.id, p.name, p.description, p.created_at]);
  }
  /**
   * Transforms Folder objects into sheet rows, resolving Parent/Project names.
   * @param {Array<Object>} folders - Raw API response.
   * @param {Object} folderMap - Lookup {id: name}.
   * @param {Object} projectMap - Lookup {id: name}.
   * @returns {Array<Array<string>>}
   */
  static mapFoldersToRows(folders, folderMap, projectMap) {
    return folders.map(f => {
      let parentName = "TOP LEVEL";
      if (f.is_project) parentName = "Workspace Root (Home)";
      else if (f.parent_id) parentName = DataMapper._safeLookup(folderMap, f.parent_id);
      
      const projectName = DataMapper._safeLookup(projectMap, f.project_id);
      return [f.id, f.name, parentName, projectName];
    });
  }
  /**
   * Transforms Recipe objects into sheet rows.
   * @param {Array<Object>} recipes - Raw API response.
   * @param {Object} projectMap - Lookup {id: name}.
   * @param {Object} folderMap - Lookup {id: name}.
   * @returns {Array<Array<string>>}
   */
  static mapRecipesToRows(recipes, projectMap, folderMap) {
    return recipes.map(r => [
      r.id,
      r.name,
      r.running ? "ACTIVE" : "STOPPED",
      DataMapper._safeLookup(projectMap, r.project_id),
      DataMapper._safeLookup(folderMap, r.folder_id),
      r.last_run_at || "NEVER"
    ]);
  }
  /**
   * Transforms Property objects into sheet rows.
   * @param {Array<Object>} properties - Raw API response.
   * @returns {Array<Array<string>>}
   */
  static mapPropertiesToRows(properties) {
    return properties.map(p => [p.id, p.name, p.value, p.created_at, p.updated_at]);
  }
  // Data tables: schema is an array
  static mapDataTablesToRows(dataTables, folderMap = null) {
    return (dataTables || []).map(t => {
      const cols = DataMapper._columnsFromAnySchema_(t.schema);
      const folderNote = t.folder_id
        ? `Folder: ${DataMapper._safeLookup(folderMap, t.folder_id)}`
        : "";
      const desc = [String(t.description || ""), folderNote].filter(Boolean).join(" | ");
      return [
        String(t.id || ""),
        String(t.name || ""),
        desc,
        cols,
        "", // record count not provided by list endpoint
        String(t.updated_at || "")
      ];
    });
  }
  // Lookup tables: schema is a JSON string
  static mapLookupTablesToRows(lookupTables, projectMap = null) {
    return (lookupTables || []).map(t => {
      const cols = DataMapper._columnsFromAnySchema_(t.schema);
      const scopeNote = t.project_id
        ? `Project: ${DataMapper._safeLookup(projectMap, t.project_id)}`
        : "Scope: Global";
      const desc = [String(t.description || ""), scopeNote].filter(Boolean).join(" | ");
      return [
        String(t.id || ""),
        String(t.name || ""),
        desc,
        cols,
        "", // record count not provided by list endpoint
        String(t.updated_at || "")
      ];
    });
  }
  /**
   * Transforms dependency objects (calculated in Analyzer) into sheet rows.
   * @param {Object} recipe - The parent recipe object.
   * @param {Array<Object>} dependencies - List of deps {type, id, name}.
   * @param {Object} projectMap - Lookup.
   * @param {Object} folderMap - Lookup.
   * @returns {Array<Array<string>>}
   */
  static mapDependenciesToRows(recipe, dependencies, projectMap, folderMap, tableNameMap = null) {
    const projectName = DataMapper._safeLookup(projectMap, recipe.project_id);
    const folderName = DataMapper._safeLookup(folderMap, recipe.folder_id);

    return dependencies.map(dep => {
      const depName =
        String(dep.name || "") ||
        String(
          (tableNameMap && /table/i.test(String(dep.type || "")))
            ? (tableNameMap[String(dep.id)] || "")
            : ""
        );
      return [String(recipe.id), projectName, folderName, String(dep.type || ""), String(dep.id || ""), depName];
    });

  }
  /**
   * Transforms a batch of debug log entries into rows.
   * Handles "Chunking" of large JSON strings to fit into cell limits.
   * * @param {Array<Object>} logEntries - Objects {id, name, json, driveUrl, status}.
   * @returns {Array<Array<string>>}
   */
  static mapDebugLogsToRows(logEntries) {
    const config = AppConfig.get();
    const CHAR_LIMIT = config.CONSTANTS.CELL_CHAR_LIMIT || 48000;
    const LOG_TO_DRIVE = config.DEBUG.LOG_TO_DRIVE;

    return logEntries.map(log => {
      const timestamp = new Date().toISOString();
      
      // Handle Drive status, hyperlink
      let status = log.status || "OK";
      let driveLink = "Not saved";

      if (LOG_TO_DRIVE) {
        if (log.driveUrl) {
          status = "Saved to Drive";
          driveLink = `=HYPERLINK("${log.driveUrl}", "View JSON")`;
        } else if (!log.status) {
          status = "Drive error";
        }
      }

      const row = [timestamp, log.id, log.name, status, driveLink];

      // Handle JSON body
      if (log.json) {
        const jsonString = typeof log.json === 'string' ? log.json : JSON.stringify(log.json, null, 2);
        if (jsonString.length <= CHAR_LIMIT) {
          row.push(jsonString);
        } else {
          let offset = 0;
          while (offset < jsonString.length) {
            row.push(jsonString.substring(offset, offset + CHAR_LIMIT));
            offset += CHAR_LIMIT;
          }
        }
      }
      return row;
    });
  }
  /**
   * Transforms recipe call edge objects into sheet rows.
   * @param {Object} recipe - Parent recipe (from /recipes list).
   * @param {Array<Object>} edges - Call edge objects from RecipeAnalyzerService.getCallEdges().
   * @param {Object} projectMap - Lookup.
   * @param {Object} folderMap - Lookup.
   * @param {Object} recipeNameMap - Lookup {id: name} for child recipe name resolution.
   * @returns {Array<Array<string>>}
   */
  static mapCallEdgesToRows(recipe, edges, projectMap, folderMap, recipeNameMap) {
    const projectName = DataMapper._safeLookup(projectMap, recipe.project_id);
    const folderName = DataMapper._safeLookup(folderMap, recipe.folder_id);

    return (edges || []).map(e => ([
      String(e.parent_recipe_id || recipe.id || ""),
      String(e.parent_recipe_name || recipe.name || ""),
      projectName,
      folderName,
      String(e.step_path || ""),
      String(e.step_name || ""),
      String(e.branch_context || ""),
      String(e.provider || ""),
      String(e.child_recipe_id || ""),
      DataMapper._safeLookup(recipeNameMap, e.child_recipe_id),
      String(e.id_key || "")
    ]));
  }
  /**
   * Transforms a process graph's nodes into sheet rows.
   * @param {string|number} rootId
   * @param {string} rootName
   * @param {{ nodes: Map<string, any> }} graph
   * @returns {Array<Array<string>>}
   */
  static mapProcessNodesToRows(rootId, rootName, graph) {
    const rows = [];
    const nodes = graph?.nodes ? Array.from(graph.nodes.values()) : [];
    nodes.forEach(n => {
      rows.push([
        String(rootId || ""),
        String(rootName || ""),
        String(n.id || ""),
        String(n.step_path || ""),
        String(n.kind || ""),
        String(n.provider || ""),
        String(n.label || ""),
        String(n.branch_context || "")
      ]);
    });
    return rows;
  }
  /**
   * Transforms a process graph's edges into sheet rows.
   * @param {string|number} rootId
   * @param {{ edges: Array<any> }} graph
   * @returns {Array<Array<string>>}
   */
  static mapProcessEdgesToRows(rootId, graph) {
    const rows = [];
    const edges = Array.isArray(graph?.edges) ? graph.edges : [];
    edges.forEach(e => {
      rows.push([
        String(rootId || ""),
        String(e.from || ""),
        String(e.to || ""),
        String(e.label || ""),
        String(e.kind || "")
      ]);
    });
    return rows;
  }

  // --- INTERNALS ---------------------------------------------------------------------------------------
  static _columnsFromAnySchema_(schema) {
    // Lookup tables: schema is a stringified JSON array
    if (typeof schema === "string") {
      try {
        const arr = JSON.parse(schema);
        if (Array.isArray(arr)) {
          return arr.map(c => c.label || c.name).filter(Boolean).join(", ");
        }
      } catch (e) {
        return "";
      }
      return "";
    }
    // Data tables: schema is an array of objects
    if (Array.isArray(schema)) {
      return schema.map(c => c.name).filter(Boolean).join(", ");
    }
    // Some APIs might return columns: [...]
    if (schema && Array.isArray(schema.columns)) {
      return schema.columns.map(c => c.name).filter(Boolean).join(", ");
    }
    return "";
  }
  /**
   * Safely looks up an ID in a map, returning a fallback if missing.
   * @private
   */
  static _safeLookup(map, id) {
    if (!id) return "-";
    const strId = String(id);
    return map && map[strId] ? map[strId] : `[ID: ${id}]`;
  }
}

/**
 * @file 20_UI_Mode.gs
 * @description UI mode controls (basic vs advanced) stored in UserProperties.
 */

class UiMode {
  static key_() { return "UI_MODE"; } // user property
  static get_() {
    return String(ConfigStore.get(this.key_(), { preferUser: true, defaultValue: "basic" }) || "basic")
      .trim()
      .toLowerCase();
  }
  static isAdvanced() { return this.get_() === "advanced"; }

  static set(mode) {
    const m = (String(mode || "").toLowerCase() === "advanced") ? "advanced" : "basic";
    ConfigStore.setUser(this.key_(), m);
    return m;
  }

  static toggle() {
    return this.set(this.isAdvanced() ? "basic" : "advanced");
  }

  static rebuildMenu_() {
    try {
      new UserInterfaceService().createMenu();
      SpreadsheetApp.getActiveSpreadsheet().toast("Menu updated.", "Workato Sync", 3);
    } catch (e) {
      // Swallow UI issues for headless runs
    }
  }
}

// Global handlers (used by menu)
function setUiModeBasic() {
  UiMode.set("basic");
  UiMode.rebuildMenu_();
}
function setUiModeAdvanced() {
  UiMode.set("advanced");
  UiMode.rebuildMenu_();
}
function toggleUiMode() {
  UiMode.toggle();
  UiMode.rebuildMenu_();
}


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

// -------------------------------------------------------------------------------------------------------
// SELECTION UTILS
// -------------------------------------------------------------------------------------------------------
class SelectionUtils {
  /**
   * Extract recipe IDs from the user's current selection.
   * Works if the active sheet has a recognizable ID header (e.g. "ID", "Recipe ID", "Root recipe ID").
   * Fallback: if the user selects a column of IDs directly, we’ll try to parse numeric-ish values.
   *
   * @param {{ headerCandidates?: string[] }} [opts]
   * @returns {string[]} unique recipe IDs
   */
  static getSelectedRecipeIds(opts = {}) {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const sheet = ss.getActiveSheet();
    const range = sheet.getActiveRange();
    if (!range) return [];

    const headerCandidates = (opts.headerCandidates && opts.headerCandidates.length)
      ? opts.headerCandidates
      : ["ID", "Recipe ID", "Root recipe ID", "Parent recipe ID", "Child recipe ID"];

    const lastCol = sheet.getLastColumn();
    if (lastCol < 1) return [];

    const headers = sheet.getRange(1, 1, 1, lastCol).getValues()[0].map(v => String(v || "").trim());
    const idCol = this._findHeaderColumn_(headers, headerCandidates); // 1-based col index or 0

    // Prefer: use the ID column for the selected rows (even if selection isn't in that column)
    if (idCol > 0) {
      const selStartRow = range.getRow();
      const selEndRow = selStartRow + range.getNumRows() - 1;

      const startRow = Math.max(2, selStartRow); // skip header row
      const endRow = Math.max(startRow, selEndRow);
      const numRows = Math.max(0, endRow - startRow + 1);
      if (numRows <= 0) return [];

      const values = sheet.getRange(startRow, idCol, numRows, 1).getValues().flat();
      return this._normalizeIds_(values);
    }

    // Fallback: user selected ID cells directly (try to parse numeric-ish values)
    const raw = range.getValues().flat();
    return this._normalizeIds_(raw);
  }

  // --- INTERNALS ---------------------------------------------------------------------------------------
  static _findHeaderColumn_(headers, candidates) {
    const norm = (s) => String(s || "").trim().toLowerCase();
    const candSet = new Set(candidates.map(norm));

    for (let i = 0; i < headers.length; i++) {
      if (candSet.has(norm(headers[i]))) return i + 1; // 1-based
    }
    return 0;
  }
  static _normalizeIds_(values) {
    const out = [];
    const seen = new Set();

    values.forEach(v => {
      if (v === null || v === undefined || v === "") return;

      // Accept numbers or digit-strings; ignore other noise
      const s = String(v).trim();
      const isNumeric = (typeof v === "number") || /^[0-9]+$/.test(s);
      if (!isNumeric) return;

      const id = String(parseInt(s, 10));
      if (!id || seen.has(id)) return;
      seen.add(id);
      out.push(id);
    });

    return out;
  }
}


/**
 * @file 99_EntryPoints.gs
 * @description Functions outside of classes that act as entry points.
 */

// -------------------------------------------------------------------------------------------------------
// HANDLES
// -------------------------------------------------------------------------------------------------------
function onOpen() {
  new UserInterfaceService().createMenu();
}
function promptToken() {
  new UserInterfaceService().promptUpdate('WORKATO_TOKEN', 'Update API Token', { isSecret: true, scope: "user" });
}
function promptBaseUrl() {
  // Base URL can be user-specific or shared; defaulting to user avoids surprises across environments.
  new UserInterfaceService().promptUpdate('WORKATO_BASE_URL', 'Update Base URL', { isSecret: false, scope: "user" });
}
function promptFolderId() {
  new UserInterfaceService().promptUpdate('DEBUG_FOLDER_ID', 'Update Debug Folder ID', { isSecret: false, scope: "user" });
}
function showCurrentConfig() {
  new UserInterfaceService().showConfiguration();
}

// -------------------------------------------------------------------------------------------------------
// GLOBAL EXECUTABLES
// -------------------------------------------------------------------------------------------------------
/**
 * Primary entry point for the script. 
 * Initializes the WorkatoSyncApp controller and runs the sync.
 */
function syncInventory() {
  Commands.run("inventory.sync");
}
/** Analyze recipe */
function fetchRecipeLogic() {
  Commands.run("logic.debug");
}
/** Analyze recipe with AI */
function fetchRecipeAnalysis() {
  Commands.run("ai.analyze");
}
/** Generates process maps: calls + full (default). */
function generateProcessMaps() {
  Commands.run("process.maps", { options: { mode: "calls+full" } });
}
/** Generates process maps: calls only. */
function generateProcessMapsCalls() {
  Commands.run("process.maps", { options: { mode: "calls" } });
}
/** Generates process maps: full process only. */
function generateProcessMapsFull() {
  Commands.run("process.maps", { options: { mode: "full" } });
}
// --- Selection-driven actions -------------------------------------------------------------------------
function fetchRecipeLogicSelected() {
  const ids = SelectionUtils.getSelectedRecipeIds();
  if (!ids.length) {
    Logger.notify("Select rows (or ID cells) in a sheet with recipe IDs first.", true);
    return;
  }
  Commands.run("logic.debug", { ids });
}
function fetchRecipeAnalysisSelected() {
  const ids = SelectionUtils.getSelectedRecipeIds();
  if (!ids.length) {
    Logger.notify("Select rows (or ID cells) in a sheet with recipe IDs first.", true);
    return;
  }
  Commands.run("ai.analyze", { ids });
}
function generateProcessMapsSelected() {
  const ids = SelectionUtils.getSelectedRecipeIds();
  if (!ids.length) {
    Logger.notify("Select rows (or ID cells) in a sheet with recipe IDs first.", true);
    return;
  }
  Commands.run("process.maps", { options: { mode: "calls+full" }, ids });
}
function generateProcessMapsSelectedCalls() {
  const ids = SelectionUtils.getSelectedRecipeIds();
  if (!ids.length) {
    Logger.notify("Select rows (or ID cells) in a sheet with recipe IDs first.", true);
    return;
  }
  Commands.run("process.maps", { options: { mode: "calls" }, ids });
}
function generateProcessMapsSelectedFull() {
  const ids = SelectionUtils.getSelectedRecipeIds();
  if (!ids.length) {
    Logger.notify("Select rows (or ID cells) in a sheet with recipe IDs first.", true);
    return;
  }
  Commands.run("process.maps", { options: { mode: "full" }, ids });
}

/**
 * Migrate from scriptProperties to userProperties
 */
function migrateMyScriptPropsToUserProps() {
  const keys = ["WORKATO_TOKEN", "WORKATO_BASE_URL", "DEBUG_FOLDER_ID"];
  const s = PropertiesService.getScriptProperties();
  const u = PropertiesService.getUserProperties();
  keys.forEach(k => {
    const v = s.getProperty(k);
    if (v && !u.getProperty(k)) u.setProperty(k, v);
  });
  SpreadsheetApp.getUi().alert("Migrated script props to your user props (only for missing values).");
}
/**
 * Validates the connection to the Workato API across all primary endpoints.
 * Uses the Class-based WorkatoClient.
 */
function testWorkatoConnectivity() {
  console.log("--- TESTING CONNECTIVITY ---");
  const client = new WorkatoClient();
  const endpoints = ['projects', 'folders', 'recipes', 'properties', 'lookup_tables', 'data_tables'];
  const results = [];

  endpoints.forEach(endpoint => {
    try {
      const path = `${endpoint}?page=1&per_page=1`;
      const json = client.get(path);
      
      let count = 0;
      if (Array.isArray(json)) count = json.length;
      else if (json.data) count = json.data.length;
      else if (json.items) count = json.items.length;
      else if (json.result) count = json.result.length;
      
      const msg = `[${endpoint.toUpperCase()}] Status: OK (${count}) samples`;
      console.log(msg);
      results.push("✅ " + msg);
    } catch (e) {
      const msg = `[${endpoint.toUpperCase()}] FAILED: ${e.message}`;
      console.error(msg);
      results.push("❌ " + msg);
    }
  });
  try {
    new UserInterfaceService().showConnectivityReport(results);
  } catch {
    console.log(results);
  }
}
function debugPropertyReport() {
  const u = PropertiesService.getUserProperties().getProperties();
  const s = PropertiesService.getScriptProperties().getProperties();
  console.log("USER PROPS:", JSON.stringify(u, null, 2));
  console.log("SCRIPT PROPS:", JSON.stringify(s, null, 2));
  SpreadsheetApp.getUi().alert("Logged user/script properties to execution logs.");
}
