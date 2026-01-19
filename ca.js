/**
 * @file Workato Inventory Sync
 * @description Fetches all resources from Workato and logs them to a dedicated Google Sheet.
 * @author Emily Cabaniss
 * 
 * @see - README ("https://docs.google.com/document/d/18mk8sphXwC7bTRrDj09rnL4FNVuiBNS1oVeM3zuyUcg/edit?tab=t.0")
 * @see - Diagrams ("https://lucid.app/lucidchart/8af28952-b1ae-4eb2-a486-343a0162a587/edit?viewport_loc=-2621%2C-29%2C4037%2C1896%2C-4lm-29-aRvB&invitationId=inv_66f2e22a-b1f9-49b6-b036-ed97b5af2d39")
 * @see - Documentation for the Workato developer API: "https://docs.workato.com/en/workato-api.html"
 * 
 * * WorkatoSyncApp ID: 1sl2ZfkgwX57EIygRwEP7nkXTK8BEXaB60cnFKsqhg2DWic3V0SVAzrYS
 * * RecipeAnalyzer: 1zQz8lK_00xJiyVweBiNUfhr54HqAGY0isdck0lQCYyr134Xmm7fx_ahW
 * * GeminiService: 1mc_Jm9FmSo2yMzjAaVdtD7Ww95Fa2RPLQ1-4Kb5kTtEwkuSfrOBCIzKZ
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
 * @classdesc Service for interacting with Google Vertex AI via the GeminiClient library.
 * GeminiService ID: 1mc_Jm9FmSo2yMzjAaVdtD7Ww95Fa2RPLQ1-4Kb5kTtEwkuSfrOBCIzKZ
 */
class GeminiService {
  constructor() {
    const config = AppConfig.get().VERTEX;
    this.config = config;

    this.client = GeminiLib.newClient(
      config.GOOGLE_CLOUD_PROJECT_ID,
      config.LOCATION,
      config.MODEL_ID
    );

    this.genConfig = config.GENERATION_CONFIG;
  }
  /**
   * Generates a natural language summary of a Workato recipe.
   * @param {Object} recipe - The full recipe object.
   * @returns {string} The AI-generated summary.
   */
  explainRecipe(recipe, graphPack = null, logicDigest = "") {
    const ctx = this._prepareContext(recipe, graphPack, logicDigest);
    const prompt = this._buildPrompt(ctx);
    
    // Delegate to library
    return this.client.generateContent(prompt, {
      generationConfig: this.genConfig
    });
  }
  /**
   * Returns a structured analysis object (JSON) so we can split into columns.
   * @returns {{objective:string,trigger:string,high_level_flow:string[],hotspots:string[],external_apps:string[],called_recipes:string[],risks_notes:string[]}}
   */
  explainRecipeStructured(recipe, graphPack = null, logicDigest = "") {
    const ctx = this._prepareContext(recipe, graphPack, logicDigest);
    const prompt = this._buildStructuredPrompt(ctx);

    // Delegate to Library (using the structured helper)
    const result = this.client.generateStructured(prompt, {
      generationConfig: this.genConfig
    });

    // Fallback if AI fails to return valid JSON
    return result || {
      objective: "Analysis failed",
      trigger: "Unknown",
      high_level_flow: [],
      hotspots: [],
      external_apps: [],
      called_recipes: [],
      risks_notes: ["AI output could not be parsed."]
    };
  }
  
  // --- INTERNALS ---------------------------------------------------------------------------------------
  /**
   * Strips raw recipe data down to the essential logic for the LLM.
   * @private
   */
  _prepareContext(recipe, graphPack, logicDigest) {
    // If 'code' is still a string (hasn't been parsed by DriveService yet), parse it temporarily
    let logicBlock = recipe.code;
    if (typeof logicBlock === 'string') {
      try { logicBlock = JSON.parse(logicBlock); } catch (e) {}
    }

    return {
      name: recipe.name,
      description: recipe.description, // existing manual description
      trigger_app: recipe.trigger_application,
      connected_apps: recipe.action_applications,
      logic_digest: String(logicDigest || ""),
      graphs: graphPack || null
    };
  }
  /** @private */
  _buildPrompt(ctx) {
    const caps = this.config;
    const mermaidCap = Number(caps.MERMAID_PROMPT_MAX_CHARS || 12000);

    const graphs = ctx.graphs || {};
    const call = graphs.call || {};
    const proc = graphs.process || {};

    const callMermaid = (call.mermaid && String(call.mermaid).length <= mermaidCap) ? call.mermaid : "";
    const procMermaid = (proc.mermaid && String(proc.mermaid).length <= mermaidCap) ? proc.mermaid : "";

    const graphMetrics = {
      call: {
        depth: call.depth,
        node_count: call.node_count,
        edge_count: call.edge_count,
        notes: call.notes
      },
      process: {
        node_count: proc.node_count,
        edge_count: proc.edge_count,
        kind_counts: proc.kind_counts,
        call_targets: proc.call_targets,
        notes: proc.notes
      }
    };

    return `
      You are an expert Workato developer and systems architect.
      Only use the provided context. If something isn't present, say "Unknown from provided data."

      Produce:
      1) Objective (1 sentence)
      2) Trigger (what starts it)
      3) High-level flow (5–12 bullets)
      4) Control-flow hotspots (IF/ELSE chains, loops, ON_ERROR paths)
      5) Dependencies
        - External apps
        - Called recipes (from call graph + step-level call nodes)
      6) Risks / notes (cycles, large fan-out, truncation, node caps)

      Recipe meta:
      - Name: ${ctx.name || ""}
      - Description: ${ctx.description || ""}
      - Trigger app: ${ctx.trigger_app || ""}
      - Connected apps: ${JSON.stringify(ctx.connected_apps || [])}

      Flattened steps (may be truncated):
      ${ctx.logic_digest || "(none)"}

      Graph metrics:
      ${JSON.stringify(graphMetrics, null, 2)}

      Call graph edges sample:
      ${(call.edges_sample || []).join("\n")}

      Process graph edges sample:
      ${(proc.edges_sample || []).join("\n")}

      ${callMermaid ? `Mermaid (call graph):\n${callMermaid}\n` : "Mermaid (call graph): (omitted due to size cap)\n"}
      ${procMermaid ? `Mermaid (process graph):\n${procMermaid}\n` : "Mermaid (process graph): (omitted due to size cap)\n"}
      `.trim();
  }
  /** @private */
  _buildStructuredPrompt(ctx) {
    const graphs = ctx.graphs || {};
    const call = graphs.call || {};
    const proc = graphs.process || {};

    // Keep the prompt compact; send summaries + samples, not full Mermaid.
    const graphMetrics = {
      call: {
        depth: call.depth,
        node_count: call.node_count,
        edge_count: call.edge_count,
        notes: call.notes
      },
      process: {
        node_count: proc.node_count,
        edge_count: proc.edge_count,
        kind_counts: proc.kind_counts,
        call_targets: proc.call_targets,
        notes: proc.notes
      }
    };

    return `
      Return ONLY valid JSON (no markdown, no code fences).
      Schema:
      {
        "objective": "string",
        "trigger": "string",
        "high_level_flow": ["string", ...],
        "hotspots": ["string", ...],
        "external_apps": ["string", ...],
        "called_recipes": ["string", ...],
        "risks_notes": ["string", ...]
      }

      Use ONLY the provided context. If unknown, use "" or [].

      Recipe meta:
      - Name: ${ctx.name || ""}
      - Description: ${ctx.description || ""}
      - Trigger app: ${ctx.trigger_app || ""}
      - Connected apps: ${JSON.stringify(ctx.connected_apps || [])}

      Flattened steps (may be truncated):
      ${ctx.logic_digest || "(none)"}

      Graph metrics:
      ${JSON.stringify(graphMetrics)}

      Call graph edges sample:
      ${(call.edges_sample || []).join("\n")}

      Process graph edges sample:
      ${(proc.edges_sample || []).join("\n")}
      `.trim();
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
  /**
   * Transforms dependency objects (calculated in Analyzer) into sheet rows.
   * @param {Object} recipe - The parent recipe object.
   * @param {Array<Object>} dependencies - List of deps {type, id, name}.
   * @param {Object} projectMap - Lookup.
   * @param {Object} folderMap - Lookup.
   * @returns {Array<Array<string>>}
   */
  static mapDependenciesToRows(recipe, dependencies, projectMap, folderMap) {
    const projectName = DataMapper._safeLookup(projectMap, recipe.project_id);
    const folderName = DataMapper._safeLookup(folderMap, recipe.folder_id);

    return dependencies.map(dep => [
      recipe.id, projectName, folderName, dep.type, dep.id, dep.name
    ]);
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

// -------------------------------------------------------------------------------------------------------
// ORCHESTRATOR
// -------------------------------------------------------------------------------------------------------
/**
 * @class
 * @classdesc Main Application Controller.
 * * Orchestrates the fetching, transformation, and writing of Workato data.
 * * WorkatoSyncApp ID: 1sl2ZfkgwX57EIygRwEP7nkXTK8BEXaB60cnFKsqhg2DWic3V0SVAzrYS
 */
class WorkatoSyncApp {
  constructor() {
    this.config = AppConfig.get();
    const client = new WorkatoClient();
    this.client = client; // keep a stable direct fetch handle
    this.inventoryService = new InventoryService(client);
    this.analyzerService = new RecipeAnalyzerService(client);
    this.sheetService = new SheetService();
    this.driveService = new DriveService();
  }
  /**
   * The main execution method. 
   * Performs authentication check, fetches all resources, transforms data, 
   * resolves dependencies, and writes to Sheets.
   */
  runInventorySync() {
    try {
      Logger.verbose("Starting full workspace sync...");

      // 1. Identify present workspace
      const currentUser = this.inventoryService.getCurrentUser();
      if (currentUser) console.log(`Authenticated as ${currentUser.name || "Unknown user"}`);

      // 2. Fetch raw data
      const projects = this.inventoryService.getProjects();
      const folders = this.inventoryService.getFoldersRecursive(projects);
      const recipes = this.inventoryService.getRecipes();
      const properties = this.inventoryService.getProperties();

      Logger.verbose(`Fetched totals: ${projects.length} projects, ${folders.length} folders, ${recipes.length} recipes, ${properties.length} properties`);

      // 3. Create lookup maps
      const projectMap = this._createLookupMap(projects);
      const folderMap = this._createLookupMap(folders);
      const recipeNameMap = this._createLookupMap(recipes);
      
      const projectRows = [this.config.HEADERS.PROJECTS, ...DataMapper.mapProjectsToRows(projects)];
      const folderRows = [this.config.HEADERS.FOLDERS, ...DataMapper.mapFoldersToRows(folders, folderMap, projectMap)];
      const recipeRows = [this.config.HEADERS.RECIPES, ...DataMapper.mapRecipesToRows(recipes, projectMap, folderMap)];
      const propertyRows = [this.config.HEADERS.PROPERTIES, ...DataMapper.mapPropertiesToRows(properties)];

      // 4. Calculate dependencies
      let dependencyRows = [this.config.HEADERS.DEPENDENCIES];
      let callEdgeRows = [this.config.HEADERS.CALL_EDGES];
      const depLimit = this.config.API.RECIPE_LIMIT_DEBUG;
      
      recipes.forEach((recipe, index) => {
        if (index < depLimit) {
          const rawDeps = this.analyzerService.getDependencies(recipe.id);
          if (rawDeps.length > 0) {
            const rows = DataMapper.mapDependenciesToRows(recipe, rawDeps, projectMap, folderMap);
            dependencyRows = dependencyRows.concat(rows);
          }

          // Recipe call edges (process-map formation)
          const callEdges = this.analyzerService.getCallEdges(recipe.id);
          if (callEdges.length > 0) {
            callEdgeRows = callEdgeRows.concat(DataMapper.mapCallEdgesToRows(recipe, callEdges, projectMap, folderMap, recipeNameMap));
          }
          if (index % 10 === 0) Utilities.sleep(50);
        }
      });

      // 5. Write to Sheets
      Logger.verbose("Writing to Sheets...");
      this.sheetService.write('PROJECTS', projectRows);
      this.sheetService.write('FOLDERS', folderRows);
      this.sheetService.write('RECIPES', recipeRows);
      this.sheetService.write('PROPERTIES', propertyRows);
      this.sheetService.write('DEPENDENCIES', dependencyRows);
      this.sheetService.write('CALL_EDGES', callEdgeRows);

      Logger.notify("Sync complete. Workspace inventory updated...", false);

    } catch (e) {
      this._handleError(e);
    }
  }
  /**
   * Reads specific IDs from the input sheet and fetches step-by-step logic.
   */
  runLogicDebug(idsOverride = null) {
    try {
      Logger.verbose("Starting recipe logic debugging...");
      
      // 1. Read input 
      const requestedIds = (Array.isArray(idsOverride) && idsOverride.length > 0)
        ? idsOverride
        : this.sheetService.readRequests();

      if (requestedIds.length === 0) {
        Logger.notify("No recipe IDs found (select rows with IDs, or use 'logic_requests').", true);
        return;
      }
      Logger.notify(`Fetching logic for ${requestedIds.length} recipes...`);

      // 2. Fetch and parse logic
      const logicRows = [this.config.HEADERS.LOGIC];
      const debugLogs = [];

      requestedIds.forEach((reqId, index) => {
        try {
          const fullRecipe =
            this.analyzerService.getRecipeDetails(reqId) ||
            this.client.get(`recipes/${reqId}`);
          const recipeName = fullRecipe.name || "Unknown";

          // A. Save to Drive
          let driveUrl = "";
          if (this.config.DEBUG.LOG_TO_DRIVE) {
            driveUrl = this.driveService.saveLog(reqId, fullRecipe.name, fullRecipe);
          }
          
          // B. Emit to Sheet
          if (this.config.DEBUG.LOG_TO_SHEET) {
            debugLogs.push({ id: reqId, name: recipeName, driveUrl: driveUrl });
          }

          // C. Parse using Analyzer Service
          const parsedRows = this.analyzerService.parseLogicRows(fullRecipe);
          logicRows.push(...parsedRows);

        } catch (e) {
          console.warn(`Failed ID ${reqId}: ${e.message}`);
          logicRows.push([reqId, "ERROR", "-", "-", "-", e.message, "-"]);
        }
        
        // Throttle
        if (index % 5 === 0) Utilities.sleep(this.config.API.THROTTLE_MS);
      });

      // 3. Write output
      Logger.verbose("Writing data to sheets...");
      this.sheetService.write('LOGIC', logicRows);

      const debugRows = DataMapper.mapDebugLogsToRows(debugLogs);
      this.sheetService.appendDebugRows(debugRows);

      Logger.notify("Logic debugging complete.");

    } catch (e) {
      this._handleError(e);
    }
  }
  /**
   * Reads IDs from 'logic_requests', fetches them, sends to Gemini, and writes output.
   */
  runAiAnalysis(idsOverride = null) {
    const gemini = new GeminiService();
    const ids = (Array.isArray(idsOverride) && idsOverride.length > 0)
      ? idsOverride
      : this.sheetService.readRequests();

    if (ids.length === 0) {
      Logger.notify("No recipe IDs found (select rows with IDs, or use 'logic_requests').");
      return;
    }

    const cfg = AppConfig.get();
    const charLimit = cfg.CONSTANTS.CELL_CHAR_LIMIT || 48000;
    const maxLines = Number(cfg.VERTEX.LOGIC_DIGEST_MAX_LINES || 220);
    const depth = Number(cfg.API.PROCESS_MAP_DEPTH ?? 2);
    const maxNodes = Number(cfg.API.PROCESS_MAP_MAX_NODES ?? 250);

    const rows = [this.config.HEADERS.AI_ANALYSIS];

    ids.forEach((id, idx) => {
      Logger.notify(`Asking Gemini to analyze Recipe ${id}...`);
      try {
        const recipe =
          this.analyzerService.getRecipeDetails(id) ||
          this.client.get(`recipes/${id}`);
        const name = recipe?.name || "";

        // 1) Build graphs (same source of truth as your Mermaid exports)
        const graphPack = this.analyzerService.buildGraphPack(id, { callDepth: depth, maxNodes, edgeSampleLimit: 70 });

        // 2) Build a flattened logic digest (LLM-friendly)
        const logicRows = this.analyzerService.parseLogicRows(recipe);
        const digest = this._logicDigestFromRows(logicRows, maxLines);

        // 3) Call Gemini with grounded context
        const structured = gemini.explainRecipeStructured(recipe, graphPack, digest);

        const objective = String(structured.objective || "");
        const trigger = String(structured.trigger || "");
        const flow = Array.isArray(structured.high_level_flow) ? structured.high_level_flow.join("\n") : String(structured.high_level_flow || "");
        const hotspots = Array.isArray(structured.hotspots) ? structured.hotspots.join("\n") : String(structured.hotspots || "");
        const externalApps = Array.isArray(structured.external_apps) ? structured.external_apps.join("\n") : String(structured.external_apps || "");
        const calledRecipes = Array.isArray(structured.called_recipes) ? structured.called_recipes.join("\n") : String(structured.called_recipes || "");
        const risks = Array.isArray(structured.risks_notes) ? structured.risks_notes.join("\n") : String(structured.risks_notes || "");

        const rawPreview = JSON.stringify(structured, null, 2).slice(0, 4000);

        // 4) Drive links (full artifacts)
        const aiUrl = this.driveService.saveText(id, name || `recipe_${id}`, "ai.json", JSON.stringify(structured, null, 2));
        const callsUrl = this.driveService.saveText(id, name || `recipe_${id}`, "calls.mmd", graphPack?.call?.mermaid || "");
        const fullUrl  = this.driveService.saveText(id, name || `recipe_${id}`, "full.mmd",  graphPack?.process?.mermaid || "");

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
        rows.push([String(id), "Error", String(e.message || e), "", "", "", "", new Date().toISOString()]);
      }

      if (idx % 2 === 0) Utilities.sleep(this.config.API.THROTTLE_MS);
    });

    // Batch write (fast + consistent)
    this.sheetService.write('AI_ANALYSIS', rows);
    Logger.notify("AI analysis complete.");
  }
  /**
   * Reads recipe IDs from 'logic_requests' and generates process maps using the Library.
   * @param {{ mode?: string, callDepth?: number, maxNodes?: number }} [options]
   */
  runProcessMaps(options = {}, idsOverride = null) {
    try {
      Logger.verbose("Starting process map generation (v2 - Library)...");

      const requestedIds = (Array.isArray(idsOverride) && idsOverride.length > 0)
        ? idsOverride
        : this.sheetService.readRequests();

      if (requestedIds.length === 0) {
        Logger.notify("No recipe IDs found (select rows with IDs, or use 'logic_requests').", true);
        return;
      }

      const mode = String(options.mode || this.config.API.PROCESS_MAP_MODE_DEFAULT || "calls+full");
      const depth = Number(options.callDepth ?? this.config.API.PROCESS_MAP_DEPTH ?? 0);
      const maxNodes = Number(options.maxNodes ?? this.config.API.PROCESS_MAP_MAX_NODES ?? 250);
      const CHAR_LIMIT = this.config.CONSTANTS.CELL_CHAR_LIMIT || 48000;

      const rows = [this.config.HEADERS.PROCESS_MAPS];

      requestedIds.forEach((rootId, idx) => {
        // 1. Generate via Library (Fetch once, build both)
        const pack = this.analyzerService.buildGraphPack(rootId, { 
          callDepth: depth, 
          maxNodes: maxNodes 
        });

        const rootName = pack.root_name || "";
        let callMermaid = "";
        let fullMermaid = "";
        let notes = [];
        let callDriveLink = "";
        let fullDriveLink = "";

        // 2. Extract Call Graph Mermaid
        if (mode.includes("calls")) {
          callMermaid = pack.call.mermaid || "";
          notes = notes.concat(pack.call.notes || []);

          if (callMermaid.length > CHAR_LIMIT) {
            const url = this.driveService.saveText(rootId, rootName, "calls.mmd", callMermaid);
            callDriveLink = url ? `=HYPERLINK("${url}", "View calls mermaid")` : "Save failed";
            callMermaid = callMermaid.substring(0, CHAR_LIMIT - 200) + "\n...(TRUNCATED)";
            notes.push("Calls mermaid truncated.");
          }
        }

        // 3. Extract Process Graph Mermaid
        if (mode.includes("full")) {
          fullMermaid = pack.process.mermaid || "";
          notes = notes.concat(pack.process.notes || []);

          if (fullMermaid.length > CHAR_LIMIT) {
            const url = this.driveService.saveText(rootId, rootName, "full.mmd", fullMermaid);
            fullDriveLink = url ? `=HYPERLINK("${url}", "View full mermaid")` : "Save failed";
            fullMermaid = fullMermaid.substring(0, CHAR_LIMIT - 200) + "\n...(TRUNCATED)";
            notes.push("Full mermaid truncated.");
          }
        }

        // 4. Aggregate Row
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

        if (idx % 2 === 0) Utilities.sleep(this.config.API.THROTTLE_MS);
      });

      this.sheetService.write('PROCESS_MAPS', rows);
      Logger.notify("Process maps generated.");

    } catch (e) {
      this._handleError(e);
    }
  }

  // --- INTERNALS ---------------------------------------------------------------------------------------
  /** @private */
  _logicDigestFromRows(logicRows, maxLines) {
    const lines = [];
    const slice = Array.isArray(logicRows) ? logicRows.slice(0, maxLines) : [];
    slice.forEach(r => {
      // parseLogicRows: [recipeId, recipeName, step#, indent, provider, actionName, description, details]
      const stepNo = r[2];
      const indent = r[3] || "";
      const provider = r[4] || "";
      const action = r[5] || "";
      const desc = r[6] ? ` — ${String(r[6]).slice(0, 120)}` : "";
      lines.push(`${stepNo}. ${indent}${action} (${provider})${desc}`);
    });
    if (logicRows.length > maxLines) lines.push(`… (${logicRows.length - maxLines} more steps omitted)`);
    return lines.join("\n");
  }
  /** @private */
  _createLookupMap(items) {
    return Object.fromEntries(items.map(i => [String(i.id), i.name]));
  }
  /** @private */
  _handleError(e) {
    let errorMsg = `Sync failed: ${e.message}`;
    if (e.message.includes("Unexpected token")) {
      errorMsg = "Auth Error: Check WORKATO_TOKEN and BASE_URL";
    }
    Logger.notify(errorMsg, true);
    console.error(e.stack);
  }
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
    this.ui.createMenu('Workato Sync')
      .addSubMenu(this.ui.createMenu('Actions')
        .addSubMenu(this.ui.createMenu('Selection driven actions')
          .addItem('Debug logic for selected rows', 'fetchRecipeLogicSelected')
          .addItem('Analyze selected rows using AI', 'fetchRecipeAnalysisSelected')
          .addItem('Generate process maps for selection (calls only)', 'generateProcessMapsSelectedCalls')
          .addItem('Generate process maps for selection (full only)', 'generateProcessMapsSelectedFull')
          .addItem('Generate process maps for selection (calls + full)', 'generateProcessMapsSelected'))
        .addItem('Run workspace inventory sync', 'syncInventory')
        .addItem('Debug selected logic', 'fetchRecipeLogic')
        .addItem('Analyze recipe using AI', 'fetchRecipeAnalysis')
        .addItem('Generate process maps (calls only)', 'generateProcessMapsCalls')
        .addItem('Generate process maps (full process only)', 'generateProcessMapsFull')
        .addItem('Generate process maps (calls + full)', 'generateProcessMaps'))
      .addSeparator()
      .addItem('Test connectivity', 'testWorkatoConnectivity')
      .addSeparator()
      .addSubMenu(this.ui.createMenu('Configuration')
        .addItem('Set Workato API token', 'promptToken')
        .addItem('Set base URL', 'promptBaseUrl')
        .addItem('Set debug folder ID', 'promptFolderId')
        .addSeparator()
        .addItem('Show current config', 'showCurrentConfig'))
      .addToUi();
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
  const app = new WorkatoSyncApp();
  app.runInventorySync();
}
/** Analyze recipe */
function fetchRecipeLogic() {
  const app = new WorkatoSyncApp();
  app.runLogicDebug();
}
/** Analyze recipe with AI */
function fetchRecipeAnalysis() {
  const app = new WorkatoSyncApp();
  app.runAiAnalysis();
}
/** Generates process maps: calls + full (default). */
function generateProcessMaps() {
  const app = new WorkatoSyncApp();
  app.runProcessMaps({ mode: "calls+full" });
}
/** Generates process maps: calls only. */
function generateProcessMapsCalls() {
  const app = new WorkatoSyncApp();
  app.runProcessMaps({ mode: "calls" });
}
/** Generates process maps: full process only. */
function generateProcessMapsFull() {
  const app = new WorkatoSyncApp();
  app.runProcessMaps({ mode: "full" });
}
// --- Selection-driven actions -------------------------------------------------------------------------
function fetchRecipeLogicSelected() {
  const ids = SelectionUtils.getSelectedRecipeIds();
  if (!ids.length) {
    Logger.notify("Select rows (or ID cells) in a sheet with recipe IDs first.", true);
    return;
  }
  new WorkatoSyncApp().runLogicDebug(ids);
}
function fetchRecipeAnalysisSelected() {
  const ids = SelectionUtils.getSelectedRecipeIds();
  if (!ids.length) {
    Logger.notify("Select rows (or ID cells) in a sheet with recipe IDs first.", true);
    return;
  }
  new WorkatoSyncApp().runAiAnalysis(ids);
}
function generateProcessMapsSelected() {
  const ids = SelectionUtils.getSelectedRecipeIds();
  if (!ids.length) {
    Logger.notify("Select rows (or ID cells) in a sheet with recipe IDs first.", true);
    return;
  }
  new WorkatoSyncApp().runProcessMaps({ mode: "calls+full" }, ids);
}
function generateProcessMapsSelectedCalls() {
  const ids = SelectionUtils.getSelectedRecipeIds();
  if (!ids.length) {
    Logger.notify("Select rows (or ID cells) in a sheet with recipe IDs first.", true);
    return;
  }
  new WorkatoSyncApp().runProcessMaps({ mode: "calls" }, ids);
}
function generateProcessMapsSelectedFull() {
  const ids = SelectionUtils.getSelectedRecipeIds();
  if (!ids.length) {
    Logger.notify("Select rows (or ID cells) in a sheet with recipe IDs first.", true);
    return;
  }
  new WorkatoSyncApp().runProcessMaps({ mode: "full" }, ids);
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
  const endpoints = ['projects', 'folders', 'recipes', 'properties'];
  const results = [];

  endpoints.forEach(endpoint => {
    try {
      const path = `${endpoint}?page=1&per_page=1`;
      const json = client.get(path);
      
      let count = 0;
      if (Array.isArray(json)) count = json.length;
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
