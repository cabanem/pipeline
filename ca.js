/**
 * @file Workato Inventory Sync
 * @description Fetches all resources from Workato and logs them to a dedicated Google Sheet.
 * @author Emily Cabaniss
 * 
 * @see - README ("https://docs.google.com/document/d/18mk8sphXwC7bTRrDj09rnL4FNVuiBNS1oVeM3zuyUcg/edit?tab=t.0")
 * @see - Diagrams ("https://lucid.app/lucidchart/8af28952-b1ae-4eb2-a486-343a0162a587/edit?viewport_loc=-2621%2C-29%2C4037%2C1896%2C-4lm-29-aRvB&invitationId=inv_66f2e22a-b1f9-49b6-b036-ed97b5af2d39")
 * @see - Documentation for the Workato developer API: "https://docs.workato.com/en/workato-api.html"
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
    const props = PropertiesService.getScriptProperties();

    return {
      API: {
        TOKEN: props.getProperty('WORKATO_TOKEN'),
        BASE_URL: (props.getProperty('WORKATO_BASE_URL') || 'https://app.eu.workato.com/api').replace(/\/$/, '') ,
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
      SHEETS: {
        RECIPES: 'recipes',
        FOLDERS: 'folders',
        PROJECTS: 'projects',
        PROPERTIES: 'properties',
        DEPENDENCIES: 'recipe_dependencies',
        LOGIC: 'recipe_logic',
        LOGIC_INPUT: 'logic_requests',
        CALL_EDGES: 'recipe_call_edges',
        PROCESS_MAPS: 'process_maps',
        PROCESS_NODES: 'process_nodes',
        PROCESS_EDGES: 'process_edges',
        AI_ANALYSIS: 'ai_analysis',
        DEBUG: 'debugging_log'
      },
      HEADERS: {
        PROJECTS: ["ID", "Name", "Description", "Created at"],
        FOLDERS: ["ID", "Name", "Parent folder", "Project name"],
        RECIPES: ["ID", "Name", "Status", "Project", "Folder", "Last run"],
        DEPENDENCIES: ["Parent recipe ID", "Project", "Folder", "Dependency type", "Dependency ID", "Dependency name"],
        PROPERTIES: ["ID", "Name", "Value", "Created at", "Updated at"],
        LOGIC: ["Recipe ID", "Recipe name", "Step number", "Indent", "Provider", "Action/Name", "Description", "Input / Details"],
        LOGIC_INPUT: ["Enter recipe IDs below (one per row)"],
        CALL_EDGES: ["Parent recipe ID", "Parent recipe name", "Project", "Folder", "Step path", "Step name", "Branch context", "Provider", "Child recipe ID", "Child recipe name", "ID key"],
        PROCESS_MAPS: ["Root recipe ID", "Root recipe name", "Mode", "Call depth", "Mermaid (calls)", "Mermaid (full process)", "Notes", "Drive link (calls)", "Drive link (full)", "Generated at"],
        PROCESS_NODES: ["Root recipe ID", "Root recipe name", "Node ID", "Step path", "Kind", "Provider", "Label", "Branch context"],
        PROCESS_EDGES: ["Root recipe ID", "From node", "To node", "Edge label", "Edge kind"],
        AI_ANALYSIS: ["Recipe ID", "Recipe name", "AI explaination (preview", "Graph metrics (JSON)", "Drive link (AI full)", "Drive link (calls MMD)", "Drive link (full MMD)", "Generated at"],
        DEBUG: ["Timestamp", "Recipe ID", "Recipe name", "Status", "Drive link"]
      },
      CONSTANTS: {
        RECIPE_PROVIDERS: ['workato_recipe_function', 'workato_callable_recipe'],
        FLOW_ID_KEYS: ['flow_id', 'recipe_id', 'callable_recipe_id'],
        STYLE_HEADER_BG: "#efefef",
        CELL_CHAR_LIMIT: 48000,
        MERMAID_LABEL_MAX: 80
      },
      DEBUG: {
        ENABLE_LOGGING: true,
        LOG_TO_SHEET: true,
        LOG_TO_DRIVE: true,
        DRIVE_FOLDER_NAME: "workato_workspace_debug_logs"
      },
      VERTEX: {
        GOOGLE_CLOUD_PROJECT_ID: props.getProperty('GOOGLE_CLOUD_PROJECT_ID'),
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
 * @classdesc Low-level HTTP wrapper for the Workato API.
 * * Handles Authentication, Base URL construction, and Pagination loops.
 */
class WorkatoClient {
  constructor() {
    /** @type {APIConfig} */
    this.config = AppConfig.get().API;
    if (!this.config.TOKEN) {
      throw new Error("Missing 'WORKATO_TOKEN' in Script Properties.");
    }
  }

  /**
   * Executes a single GET request to the Workato API.
   * @param {string} endpoint - The relative API path (e.g., 'recipes' or '/users/me').
   * @returns {Object|Array} The parsed JSON response.
   * @throws {Error} If the API returns a non-200 status code.
   */
  get(endpoint) {
    const cleanPath = endpoint.startsWith('/') ? endpoint : `/${endpoint}`;
    const url = `${this.config.BASE_URL}${cleanPath}`;
    
    const options = {
      method: 'get',
      headers: {
        'Authorization': `Bearer ${this.config.TOKEN}`,
        'Content-Type': 'application/json'
      },
      muteHttpExceptions: true
    };

    let attempts = 0;
    const maxRetries = this.config.MAX_RETRIES || 3;

    while (attempts <= maxRetries) {
      try {
        const response = UrlFetchApp.fetch(url, options);
        const code = response.getResponseCode(); 

        if (code === 200) {
          return JSON.parse(response.getContentText());
        }

        // Handle rate limits or server errors
        if (code === 429 || code >= 500) {
          if (attempts === maxRetries) {
            throw new Error(`API Failed after ${attempts} retries: ${code} - ${response.getContentText()}`);
          }
          Logger.verbose(`API ${code} on ${endpoint}. Retrying in ${Math.pow(2, attempts)}s...`);
          Utilities.sleep(Math.pow(2, attempts) * 1000); // Exponential backoff: 1s, 2s, 4s
          attempts++;
          continue;
        }

        // Fatal Client Errors (400, 401, 403, 404) - Do not retry
        throw new Error(`API Error [${code}]: ${response.getContentText()}`);

      } catch (e) {
        if (attempts === maxRetries) throw e;
        Logger.verbose(`Network/Fetch Error: ${e.message}. Retrying...`);
        Utilities.sleep(1000);
        attempts++;
      }
    }
  }
  /**
   * Fetches all records from a resource by automatically following pagination.
   * @param {string} resourcePath - The resource endpoint (e.g., 'recipes').
   * @returns {Array<Object>} An aggregated array of all items across all pages.
   */
  fetchPaginated(resourcePath) {
    let results = [];
    let page = 1;
    let hasMore = true;

    while (hasMore) {
      const separator = resourcePath.includes('?') ? '&' : '?';
      const pathWithParams = `${resourcePath}${separator}page=${page}&per_page=${this.config.PER_PAGE}`;
      
      const json = this.get(pathWithParams);
      let records = Array.isArray(json) ? json : (json.items || json.result || []);

      if (records.length > 0) {
        results = results.concat(records);
        if (records.length < this.config.PER_PAGE) {
          hasMore = false;
        } else {
          page++;
        }
      } else {
        hasMore = false;
      }

      if (page % 5 === 0) Logger.verbose(`...fetched ${page} pages of ${resourcePath}`);
      if (page > 500) {
        Logger.notify(`Pagination limit reached for ${resourcePath}. Stopping at 50,000 records.`, true);
        break; 
      }
    }
    
    console.log(`Fetched ${results.length} records for ${resourcePath}`);
    return results;
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
    const globalFolders = this._fetchFolderBatch('folders');
    globalFolders.forEach(f => {
      if (!processedIds.has(f.id)) {
        allFolders.push(f);
        processedIds.add(f.id);
        queue.push(f.id);
      }
    });

    // PHASE 3: Recursion
    let safetyCounter = 0;
    while (queue.length > 0 && safetyCounter < MAX_CALLS) {
      let parentId = queue.shift();
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
    const numCols = rows[0].length; 
    sheet.getRange(startRow, 1, numRows, numCols).setValues(rows);
  }
}
/**
 * @class
 * @classdesc Service for handling file I/O with Google Drive.
 */
class DriveService{
  constructor() {
    this.config = AppConfig.get().DEBUG;
    this.props = PropertiesService.getScriptProperties();
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
    const cachedId = this.props.getProperty('DEBUG_FOLDER_ID');

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
    this.props.setProperty('DEBUG_FOLDER_ID', folder.getId());
    return folder;
  }
}
/**
 * @class
 * @classdesc Unified service for deep inspection of recipe logic / code.
 * Merges functionality from historical DependencyService and LogicService.
 */
class RecipeAnalyzerService {
  /**
   * @param {WorkatoClient} client
   */
  constructor(client) {
    this.client = client;
    this.constants = AppConfig.get().CONSTANTS;
    /** @type {Map<string, Object>} */
    this._recipeDetailCache = new Map();
  }

  /**
   * Fetches standard app connections and scans code for recipe calls.
   * @returns {Array<Object>} List of objects { type, id, name }
   */
  getDependencies(recipeId) {
    let json;
    try {
      json = this.client.get(`recipes/${recipeId}`);
    } catch (e) {
      console.warn(`Could not fetch details for recipe ${recipeId}`);
      return [];
    }

    let dependencies = [];

    // 1. Standard apps
    if (json.applications && Array.isArray(json.applications)) {
      json.applications.forEach(app => {
        if (!this.constants.RECIPE_PROVIDERS.includes(app)) {
          dependencies.push({ type: 'Connection', id: app, name: app });
        }
      });
    }

    // 2. Recipe calls (deep scan)
    if (json.code) {
      try {
        const codeObj = JSON.parse(json.code);
        const rootBlock = codeObj.block || codeObj.line || [];
        this._scanBlockForCalls(rootBlock, dependencies);
      } catch (e) {
        console.warn(`Error parsing code for recipe ${recipeId}: ${e.message}`);
      }
    }
    return dependencies;
  }
  /**
   * Fetches recipe details with an in-run cache (reduce API calls).
   * @param {string|number} recipeId
   * @returns {Object|null}
   */
  getRecipeDetails(recipeId) {
    const key = String(recipeId);
    if (this._recipeDetailCache.has(key)) return this._recipeDetailCache.get(key);
    try {
      const json = this.client.get(`recipes/${key}`);
      this._recipeDetailCache.set(key, json);
      return json;
    } catch (e) {
      console.warn(`Could not fetch details for recipe ${key}: ${e.message}`);
      return null;
    }
  }
  /**
   * Extracts recipe call edges for a given recipe.
   * Foundation for transitive call graphs and process maps.
   * @param {string|number} recipeId
   * @returns {Array<Object>} edges
   */
  getCallEdges(recipeId) {
    const json = this.getRecipeDetails(recipeId);
    if (!json || !json.code) return [];

    let edges = [];
    try {
      const codeObj = JSON.parse(json.code);
      const rootBlock = codeObj.block || codeObj.line || [];
      this._scanBlockForCallEdges(rootBlock, edges, {
        parentId: String(json.id || recipeId),
        parentName: json.name || "",
        stepPathPrefix: "",
        branchStack: []
      });
    } catch (e) {
      console.warn(`Error parsing call edges for recipe ${recipeId}: ${e.message}`);
    }
    return edges;
  }
  /**
   * Builds transitive recipe call graph rooted at rootId up to depthLimit.
   * Includes cycle detection, deduplication via "best remaining depth expanded".
   * @param {string|number} rootId
   * @param {number} depthLimit
   * @returns {{ nodes: Map<string, {id:string,name:string}>, edges: Array<object, notes: string[] }}
   */
  buildTransitiveCallGraph(rootId, depthLimit) {
    const nodes = new Map();
    const edges = [];
    const notes = [];

    /** @type {Map<string, number>} */
    const expandedAtDepth = new Map(); // id -> max remaining depth expanded

    const expand = (id, remainingDepth, stack) => {
      const key = String(id);
      
      // FIX 1: Ensure stack exists before checking includes (Safety check)
      const currentStack = stack || [];
      
      if (currentStack.includes(key)) {
        notes.push(`Cycle detected: ${currentStack.join(" -> ")} -> ${key}`);
        return;
      }

      const prev = expandedAtDepth.get(key);
      if (prev !== undefined && prev >= remainingDepth) return;
      expandedAtDepth.set(key, remainingDepth);

      const recipe = this.getRecipeDetails(key);
      if (recipe) nodes.set(key, {id: key, name: recipe.name || "" });
      else nodes.set(key, {id: key, name: ""});

      const localEdges = this.getCallEdges(key);
      localEdges.forEach(e => edges.push(e));

      if (remainingDepth <= 0) return;

      // FIX 2: Corrected typo 'concaat' to 'concat'
      const nextStack = currentStack.concat([key]); 
      
      for (const e of localEdges) {
        const child = String(e.child_recipe_id || "");
        if (!child) continue;
        // FIX 3: Passed 'nextStack' instead of undefined 'next'
        expand(child, remainingDepth - 1, nextStack); 
      }
    };
    expand(String(rootId), Math.max(0, Number(depthLimit || 0)), []);

    return { nodes, edges, notes };
  }
  /**
   * Renders a Mermaid flowchart for recipe call graph.
   * @param {string|number} rootId
   * @param {{ nodes: Map<string, {id:string,name:string}>, edges: Array<object, notes: string[] }} graph
   * @returns {string}
   */
  renderMermaidCallGraph(rootId, graph) {
    const normalizeNodeLabel = (s) => this._mNormalizeNodeLabel(s);
    const normalizeEdgeLabel = (s) => this._mNormalizeEdgeLabel(s);

    const lines = [];
    lines.push("flowchart TD");

    // Ensure root exists
    const rootKey = String(rootId);
    if (!graph.nodes.has(rootKey)) graph.nodes.set(rootKey, {id: rootKey, name: "" });

    // Nodes
    Array.from(graph.nodes.values()).forEach(n => {
      const nodeId = `R${String(n.id).replace(/[^0-9a-zA-Z_]/g, "_")}`;
      const label = normalizeNodeLabel(`${n.name || "Recipe"} (${n.id})`);
      lines.push(`  ${nodeId}["${label}"]`);
    });

    const nodeRef = (id) => `R${String(id).replace(/[^0-9a-zA-Z_]/g, "_")}`;

    // Edges (deduplicate)
    const seen = new Set();
    graph.edges.forEach(e => {
      const p = String(e.parent_recipe_id || "");
      const c = String(e.child_recipe_id || "");
      if (!p || !c) return;

      const labelBits = [];
      if (e.branch_context) labelBits.push(normalizeEdgeLabel(e.branch_context));
      if (e.step_name) labelBits.push(normalizeEdgeLabel(e.step_name));
      const edgeLabel = labelBits.join(" · "); // "&#183; = mid-dot "·"

      const sig = `${p}->${c}|${edgeLabel}`;
      if (seen.has(sig)) return;
      seen.add(sig);

      const left = nodeRef(p);
      const right = nodeRef(c);
      if (edgeLabel) lines.push(` ${left} -->|${edgeLabel}| ${right}`);
      else lines.push(` ${left} --> ${right}`);
    });

    return lines.join("\n");
  }
  /**
   * Generates a flat list of spreadsheet rows representing the logic flow.
   * @param {Object} recipe - Full recipe object.
   */
  parseLogicRows(recipe) {
    if (!recipe.code) return [];
    let rows = [];
    try {
      const codeObj = JSON.parse(recipe.code);
      const rootBlock = codeObj.block || codeObj.line || [];
      this._scanBlockForLogic(rootBlock, 0, recipe.id, recipe.name, rows);
    } catch (e) {
      console.warn(`Error parsing logic for recipe ${recipe.id}: ${e.message}`);
    }
    return rows;
  }
  /**
   * Builds a step-level process graph for a single recipe:
   * - sequential flow
   * - IF/ELSE branches
   * - ON_ERROR branches
   *
   * @param {string|number} recipeId
   * @param {{ maxNodes?: number }} [options]
   * @returns {{ nodes: Map<string, any>, edges: Array<any>, notes: string[], meta: any }}
   */
  buildProcessGraph(recipeId, options = {}) {
    const cfg = AppConfig.get();
    const maxNodes = Number(options.maxNodes ?? cfg.API.PROCESS_MAP_MAX_NODES ?? 250);

    const recipe = this.getRecipeDetails(recipeId);
    const notes = [];
    const nodes = new Map();
    const edges = [];

    const meta = {
      recipe_id: String(recipe?.id || recipeId),
      recipe_name: recipe?.name || ""
    };

    if (!recipe || !recipe.code) {
      notes.push("No recipe code available.");
      return { nodes, edges, notes, meta };
    }

    let codeObj = null;
    try {
      codeObj = (typeof recipe.code === "string") ? JSON.parse(recipe.code) : recipe.code;
    } catch (e) {
      notes.push(`Could not parse recipe code JSON: ${e.message}`);
      return { nodes, edges, notes, meta };
    }

    const rootBlock = codeObj.block || codeObj.line || [];

    const startId = this._pNodeId(`START_${meta.recipe_id}`);
    const endId = this._pNodeId(`END_${meta.recipe_id}`);

    this._pAddNode(nodes, startId, {
      id: startId,
      kind: "start",
      provider: "system",
      step_path: "",
      label: `Start: ${meta.recipe_name || "Recipe"} (${meta.recipe_id})`,
      branch_context: ""
    });
    this._pAddNode(nodes, endId, {
      id: endId,
      kind: "end",
      provider: "system",
      step_path: "",
      label: "End",
      branch_context: ""
    });

    const ctx = {
      recipeId: meta.recipe_id,
      branchStack: [],
      stepPathPrefix: "",
      maxNodes
    };

    const res = this._scanBlockForProcessGraph(rootBlock, ctx, { nodes, edges, notes }, startId, "");
    const last = res?.last || startId;
    this._pAddEdge(edges, last, endId, "", "flow");

    // Cap notice (if we hit max nodes)
    if (nodes.size >= maxNodes) {
      notes.push(`Node cap reached (${maxNodes}). Diagram may be truncated.`);
    }

    return { nodes, edges, notes, meta };
  }
  /**
   * Render a step-level process graph as Mermaid flowchart.
   * @param {string|number} recipeId
   * @param {{ nodes: Map<string, any>, edges: Array<any>, notes: string[], meta: any }} graph
   * @returns {string}
   */
  renderMermaidProcessGraph(recipeId, graph) {
    const lines = [];
    lines.push("flowchart TD");

    const normalizeNodeLabel = (s) => this._mNormalizeNodeLabel(s);
    const normalizeEdgeLabel = (s) => this._mNormalizeEdgeLabel(s);

    // Nodes
    for (const n of Array.from(graph.nodes.values())) {
      const id = n.id;
      const label = normalizeNodeLabel(n.label || id);
      const kind = String(n.kind || "step");

      // Shapes: start/end=stadium, decision=diamond, call=subroutine, merge=circle-ish, default=rect
      if (kind === "start" || kind === "end") {
        lines.push(`  ${id}([\"${label}\"])`);
      } else if (kind === "decision" || kind === "loop") {
        lines.push(`  ${id}{\"${label}\"}`);
      } else if (kind === "call") {
        lines.push(`  ${id}[[\"${label}\"]]`);
      } else if (kind === "merge") {
        lines.push(`  ${id}((\"${label}\"))`);
      } else {
        lines.push(`  ${id}[\"${label}\"]`);
      }
    }

    // Edges (deduplicate)
    const seen = new Set();
    for (const e of (graph.edges || [])) {
      const from = e.from;
      const to = e.to;
      if (!from || !to) continue;
      const lbl = e.label ? normalizeEdgeLabel(e.label) : "";
      const sig = `${from}->${to}|${lbl}|${e.kind || ""}`;
      if (seen.has(sig)) continue;
      seen.add(sig);
      if (lbl) lines.push(`  ${from} -->|${lbl}| ${to}`);
      else lines.push(`  ${from} --> ${to}`);
    }

    return lines.join("\n");
  }
  /**
   * Build a single analysis bundle: summaries + optional Mermaid.
   * This is the "one thing" both Sheets export and Gemini prompts can consume.
   *
   * @param {string|number} rootId
   * @param {{ callDepth?: number, maxNodes?: number, edgeSampleLimit?: number }} [options]
   * @returns {{ root_id:string, root_name:string, call:any, process:any }}
   */
  buildGraphPack(rootId, options = {}) {
    const cfg = AppConfig.get();
    const depth = Number(options.callDepth ?? cfg.API.PROCESS_MAP_DEPTH ?? 0);
    const maxNodes = Number(options.maxNodes ?? cfg.API.PROCESS_MAP_MAX_NODES ?? 250);
    const edgeSampleLimit = Number(options.edgeSampleLimit ?? 60);

    const rootRecipe = this.getRecipeDetails(rootId);
    const rootName = rootRecipe?.name || "";

    const callGraph = this.buildTransitiveCallGraph(rootId, depth);
    const procGraph = this.buildProcessGraph(rootId, { maxNodes });

    const callMermaid = this.renderMermaidCallGraph(rootId, callGraph);
    const procMermaid = this.renderMermaidProcessGraph(rootId, procGraph);

    return {
      root_id: String(rootId),
      root_name: rootName,
      call: {
        depth,
        node_count: callGraph?.nodes?.size || 0,
        edge_count: Array.isArray(callGraph?.edges) ? callGraph.edges.length : 0,
        notes: (callGraph?.notes || []).slice(0, 20),
        edges_sample: this._summarizeCallEdges(callGraph, edgeSampleLimit),
        mermaid: callMermaid
      },
      process: {
        maxNodes,
        node_count: procGraph?.nodes?.size || 0,
        edge_count: Array.isArray(procGraph?.edges) ? procGraph.edges.length : 0,
        notes: (procGraph?.notes || []).slice(0, 20),
        kind_counts: this._summarizeProcessKinds(procGraph),
        call_targets: this._summarizeProcessCallTargets(procGraph, 12),
        edges_sample: this._summarizeProcessEdges(procGraph, edgeSampleLimit),
        mermaid: procMermaid
      }
    };
  }

  /** @private */
  _summarizeCallEdges(callGraph, limit) {
    const edges = Array.isArray(callGraph?.edges) ? callGraph.edges : [];
    const nodes = callGraph?.nodes || new Map();
    const nameOf = (id) => nodes.get(String(id))?.name || "";

    return edges.slice(0, limit).map(e => {
      const p = String(e.parent_recipe_id || "");
      const c = String(e.child_recipe_id || "");
      const bits = [];
      if (e.branch_context) bits.push(e.branch_context);
      if (e.step_name) bits.push(e.step_name);
      const lbl = bits.filter(Boolean).join(" / ");
      return `${nameOf(p) || "Recipe"} (${p}) -> ${nameOf(c) || "Recipe"} (${c})${lbl ? `  [${lbl}]` : ""}`;
    });
  }
  /** @private */
  _summarizeProcessKinds(procGraph) {
    const out = { start: 0, end: 0, step: 0, decision: 0, loop: 0, call: 0, merge: 0, other: 0 };
    const nodes = procGraph?.nodes ? Array.from(procGraph.nodes.values()) : [];
    nodes.forEach(n => {
      const k = String(n.kind || "other");
      if (out[k] !== undefined) out[k] += 1;
      else out.other += 1;
    });
    return out;
  }
  /** @private */
  _summarizeProcessCallTargets(procGraph, limit) {
    const nodes = procGraph?.nodes ? Array.from(procGraph.nodes.values()) : [];
    const targets = [];
    const seen = new Set();
    nodes.forEach(n => {
      if (String(n.kind) !== "call") return;
      const m = String(n.label || "").match(/→\s*([0-9]+)/);
      if (!m) return;
      const id = m[1];
      if (!seen.has(id)) {
        seen.add(id);
        targets.push(id);
      }
    });
    return targets.slice(0, limit);
  }
  /** @private */
  _summarizeProcessEdges(procGraph, limit) {
    const edges = Array.isArray(procGraph?.edges) ? procGraph.edges : [];
    return edges.slice(0, limit).map(e => {
      const kind = e.kind ? ` (${e.kind})` : "";
      const lbl = e.label ? ` [${e.label}]` : "";
      return `${e.from} -> ${e.to}${lbl}${kind}`;
    });
  }

  /** @private Helper for getDependencies */
  _scanBlockForCalls(steps, resultsArray) {
    if (!Array.isArray(steps)) return;

    steps.forEach(step => {
      if (this.constants.RECIPE_PROVIDERS.includes(step.provider)) {
        const input = step.input || {};
        const idKey = this.constants.FLOW_ID_KEYS.find(key => input[key]);

        if (idKey) {
          resultsArray.push({
            type: 'RECIPE CALL',
            id: input[idKey],
            name: `Called via step: "${step.name || 'Unknown'}"`
          });
        }
      }
      // Recurse
      if (step.block) this._scanBlockForCalls(step.block, resultsArray);
      if (step.else_block) this._scanBlockForCalls(step.else_block, resultsArray);
      if (step.error_block) this._scanBlockForCalls(step.error_block, resultsArray);
    });
  }
  /** @private Helper for parseLogicRows */
  _scanBlockForLogic(steps, indentLevel, recipeId, recipeName, rows) {
    if (!Array.isArray(steps)) return;

    steps.forEach((step, index) => {
      const visualIndent = "> ".repeat(indentLevel);
      
      let actionName = step.name || step.as || "Unknown Action";
      if (step.keyword) actionName = `[${step.keyword.toUpperCase()}] ${actionName}`;
      
      const description = step.description || step.comment || "";
      const details = this._extractStepDetails(step);

      rows.push([
        String(recipeId),
        recipeName,
        index + 1,
        visualIndent,
        step.provider || "System",
        actionName,
        description,
        details
      ]);

      if (step.block)       this._scanBlockForLogic(step.block, indentLevel + 1, recipeId, recipeName, rows);
      if (step.else_block)  this._scanBlockForLogic(step.else_block, indentLevel + 1, recipeId, recipeName, rows);
      if (step.error_block) this._scanBlockForLogic(step.error_block, indentLevel + 1, recipeId, recipeName, rows);
    });
  }
  /** @private Helper for getCallEdges */
  _scanBlockForCallEdges(steps, edges, ctx) {
    if (!Array.isArray(steps)) return;

    steps.forEach((step, index) => {
      const stepPath = ctx.stepPathPrefix ? `${ctx.stepPathPrefix}/${index}` : `${index}`;

      const input = step?.input || {};
      const found = this._findIdKeyAndValue(input, this.constants.FLOW_ID_KEYS, 3);
      const looksLikeRecipeCall = 
        Boolean(found) || this.constants.RECIPE_PROVIDERS.includes(step?.provider);

      if (looksLikeRecipeCall && found && found.value) {
        edges.push({
          parent_recipe_id: ctx.parentId,
          parent_recipe_name: ctx.parentName,
          child_recipe_id: String(found.value),
          id_key: found.key,
          provider: step.provider || "unknown",
          step_name: step.name || step.as || "Unknown step",
          step_path: stepPath,
          branch_context: (ctx.branchStack || []).join(" / ")
        });
      }

      // Hande branch context
      const keyword = String(step?.keyword || "").toLowerCase();
      const condSummary = (keyword === "if" || keyword === "elsif")
        ? this._formatConditionSummary(step)
        : "";

      // Main
      if (step.block) {
        const nextStack = ctx.branchStack.slice();
        if (keyword === "if" || keyword === "elsif") {
          nextStack.push(`IF ${condSummary}`.trim());
        }
        this._scanBlockForCallEdges(step.block, edges, {
          ...ctx,
          stepPathPrefix: stepPath,
          branchStack: nextStack
        });
      }

      // Else block
      if (step.else_block) {
        const nextStack = ctx.branchStack.slice();
        nextStack.push("ELSE");
        this._scanBlockForCallEdges(step.else_block, edges, {
          ...ctx,
          stepPathPrefix: stepPath,
          branchStack: nextStack
        });
      }

      // Error block
      if (step.error_block) {
        const nextStack = ctx.branchStack.slice();
        nextStack.push("ON_ERROR");
        this._scanBlockForCallEdges(step.error_block, edges, {
          ...ctx,
          stepPathPrefix: stepPath,
          branchStack: nextStack
        });
      }
    });
  }
  /**
   * Depth-limited search for recipe ID key/value inside step.input.
   * Prioritizes common nests (like params) then falls back to small DFS.
   * @private
   */
  _findIdKeyAndValue(obj, keys, depth) {
    if (!obj || typeof obj !== "object") return null;
    if (depth <= 0) return null;

    // Direct keys
    for (const k of keys) {
      if (obj[k] !== undefined && obj[k] !== null && obj[k] !== "") {
        return { key: k, value: obj[k] };
      }
    }

    // Common nest, parameters
    if (obj.parameters && typeof obj.parameters === "object") {
      for (const k of keys) {
        const v = obj.parameters[k];
        if (v !== undefined && v !== null && v !== "") return { key: k, value: v };
      }
    }

    // Shallow (bounded) DFS
    for (const [k, v] of Object.entries(obj)) {
      if (!v || typeof v !== "object") continue;
      const hit = this._findIdKeyAndValue(v, keys, depth - 1);
      if (hit) return hit;
    }
    return null;
  }
  /** @private Formats IF/ELSIF conditions into short readable str. */
  _formatConditionSummary(step) {
    try {
      const conditions = step?.input?.conditions || [];
      if (!Array.isArray(conditions) || conditions.length === 0) return "";
      const parts = conditions.slice(0, 3).map(c => {
        const lhs = this._cleanDataPill(c.lhs);
        const rhs = this._cleanDataPill(c.rhs);
        return `${lhs} ${c.operand} ${rhs}`.trim();
      });
      const joiner = (step?.input?.operand || "and").toUpperCase();
      let summary = parts.join(` ${joiner}`);
      if (conditions.length > 3) summary += " ...";
      return summary;
    } catch (_) {
      return "";
    }
  }
  /** @private Extracts details like IF conditions or SQL queries */
  _extractStepDetails(step) {
    let details = [];

    // Conditionals
    if (step.keyword === 'if' || step.keyword === 'elsif') {
      const conditions = step.input?.conditions || [];
      conditions.forEach(c => {
        const lhs = this._cleanDataPill(c.lhs);
        const rhs = this._cleanDataPill(c.rhs);
        details.push(`Condition: ${lhs} ${c.operand} ${rhs}`);
      });
      if (step.input?.type === 'compound') {
        details.push(`Logic: ${step.input.operand.toUpperCase()}`);
      }
    }

    // Standard inputs
    if (step.input) {
      if (step.input.flow_id) details.push(`Call Recipe ID: ${step.input.flow_id}`);
      
      const keys = ['to', 'subject', 'from', 'table_id', 'sql', 'message'];
      keys.forEach(key => {
        if (step.input[key]) details.push(`${key}: ${this._cleanDataPill(step.input[key])}`);
      });

      if (step.input.parameters) {
        Object.keys(step.input.parameters).forEach(key => {
          const val = this._cleanDataPill(step.input.parameters[key]);
          if(val) details.push(`Set '${key}' = ${val}`);
        });
      }
    }
    return details.join('\n');
  }
  /** @private Cleans Workato Data Pill strings: #{_dp(...)} */
  _cleanDataPill(rawStr) {
    if (!rawStr || typeof rawStr !== 'string') return rawStr;
    const dpRegex = /#\{_dp\('(.*?)'\)\}/g;
    return rawStr.replace(dpRegex, (match, innerJsonEscaped) => {
      try {
        const jsonStr = innerJsonEscaped.replace(/\\"/g, '"').replace(/\\\\/g, '\\');
        const dpObj = JSON.parse(jsonStr);
        if (dpObj.label) return `{{${dpObj.label}}}`;
        if (dpObj.path && Array.isArray(dpObj.path)) {
           const readablePath = dpObj.path.filter(p => typeof p === 'string' && !p.match(/^[0-9a-f-]{36}$/)).join('.');
           return `{{${readablePath}}}`;
        }
        return "{{Unknown Variable}}";
      } catch (e) { return "{{Variable}}"; }
    });
  }

  // --- MERMAID INTERNALS -------------------------------------------------------------------------------
  /** @private */
  _mMaxLabel() {
    const cfg = AppConfig.get();
    return Number(cfg.CONSTANTS.MERMAID_LABEL_MAX || 80);
  }
  /** @private */
  _mSafeBase(s) {
    return String(s || "")
      .replace(/\r?\n/g, " ")
      .replace(/[\u0000-\u001F\u007F]/g, " ")
      .trim();
  }
  /** @private Collapse Workato datapill blobs BEFORE truncation */
  _mCollapseWorkato(s) {
    return String(s || "").replace(/_dp\([^)]*\)/g, "{{dp}}");
  }
  /** @private Entity-style escapes per Mermaid flowchart docs */
  _mEntityEscape(ch) {
    const map = {
      '"': "#quot;",
      "'": "#39;",
      "(": "#40;",
      ")": "#41;",
      "[": "#91;",
      "]": "#93;",
      "{": "#123;",
      "}": "#125;",
      "<": "#60;",
      ">": "#62;",
      "\\": "#92;",
      "&": "#38;"
    };
    return map[ch] || ch;
  }
  /** @private */
  _mTruncTo(s) {
    const maxLabel = this._mMaxLabel();
    const str = String(s || "");
    return (str.length > maxLabel ? str.slice(0, Math.max(0, maxLabel - 3)) + "..." : str);
  }
  /** @private */
  _mNormalizeNodeLabel(s) {
    let str = this._mSafeBase(this._mCollapseWorkato(s));
    str = str.replace(/"/g, "'");
    str = str.replace(/\s+/g, " ").trim();
    return this._mTruncTo(str);
  }
  /** @private */
  _mNormalizeEdgeLabel(s) {
    let str = this._mSafeBase(this._mCollapseWorkato(s));
    str = str.replace(/\|/g, " / ");
    str = str.replace(/["'(){}\[\]<>\\&]/g, (ch) => this._mEntityEscape(ch));
    str = str.replace(/\s+/g, " ").trim();
    return this._mTruncTo(str);
  }

  // --- PROCESS-GRAPH INTERNALS -------------------------------------------------------------------------
  /** @private */
  _pKeyword(step) {
    return String(step?.keyword || "").toLowerCase();
  }
  /** @private */
  _pIsIf(step) {
    return this._pKeyword(step) === "if";
  }
  /** @private */
  _pIsElsif(step) {
    return this._pKeyword(step) === "elsif";
  }
  /** @private */
  _pDecisionHeader(step) {
    const kw = this._pKeyword(step);
    return (kw === "elsif") ? "ELSIF" : "IF";
  }
  /** @private */
  _pDecisionBranchLabel(step) {
    const head = this._pDecisionHeader(step);
    const cond = this._formatConditionSummary(step);
    return cond ? `${head} ${cond}` : head;
  }
  /** @private Ensure Mermaid-safe node ids */
  _pNodeId(raw) {
    const safe = String(raw || "")
      .replace(/[^0-9a-zA-Z_]/g, "_")
      .replace(/^([0-9])/, "_$1");
    return `N_${safe}`;
  }
  /** @private Add node with cap protection */
  _pAddNode(nodes, id, node) {
    if (!nodes.has(id)) nodes.set(id, node);
  }
  /** @private Add edge */
  _pAddEdge(edges, from, to, label = "", kind = "flow") {
    edges.push({ from, to, label, kind });
  }
  /** @private */
  _pClassifyStep(step) {
    const keyword = this._pKeyword(step);
    if (this._pIsLoopStep(step)) return "loop";
    if (keyword === "if" || keyword === "elsif") return "decision";

    const provider = String(step?.provider || "");
    const input = step?.input || {};
    const found = this._findIdKeyAndValue(input, this.constants.FLOW_ID_KEYS, 3);
    const looksLikeRecipeCall = Boolean(found?.value) || this.constants.RECIPE_PROVIDERS.includes(provider);
    if (looksLikeRecipeCall) return "call";

    return "step";
  }
  /** @private Heuristic loop detection */
  _pIsLoopStep(step) {
    const kw = this._pKeyword(step);
    const name = String(step?.name || step?.as || "").toLowerCase();
    // Keywords are not guaranteed, so we also look at common UI names.
    const kwHit = ["repeat", "repeat_while", "repeat_each", "repeat_for_each", "for_each", "while", "until"].includes(kw);
    const nameHit = name.includes("repeat") || name.includes("for each") || name.includes("foreach");
    return kwHit || nameHit;
  }
  /** @private */
  _pLoopLabel(step) {
    const kw = this._pKeyword(step);
    const name = step?.name || step?.as || "Loop";
    const cond = this._formatConditionSummary(step);
    // If it's a "repeat while"-style loop, condition summary is valuable.
    if (kw.includes("while") || name.toLowerCase().includes("while")) {
      return cond ? `REPEAT WHILE ${cond}` : "REPEAT WHILE";
    }
    // Otherwise, keep it simple and readable.
    return String(name).toUpperCase();
  }
  /** @private */
  _pStepLabel(step, kind) {
    const name = step?.name || step?.as || "Unnamed step";
    const provider = step?.provider || "System";
    const keyword = step?.keyword ? `[${String(step.keyword).toUpperCase()}] ` : "";
    if (kind === "call") {
      const found = this._findIdKeyAndValue(step?.input || {}, this.constants.FLOW_ID_KEYS, 3);
      const target = found?.value ? ` → ${found.value}` : "";
      return `${keyword}${name}${target}`;
    }
    if (kind === "decision") {
      const head = this._pDecisionHeader(step);
      const cond = this._formatConditionSummary(step);
      return cond ? `${head} ${cond}` : `${head}`;
    }
    if (kind === "loop") {
      return this._pLoopLabel(step);
    }
    return `${keyword}${name} (${provider})`;
  }
  /**
   * Walk a block and build control-flow edges.
   *
   * @private
   * @param {Array<any>} steps
   * @param {{ recipeId: string, branchStack: string[], stepPathPrefix: string, maxNodes: number }} ctx
   * @param {{ nodes: Map<string, any>, edges: Array<any>, notes: string[] }} graph
   * @param {string} entryFromNodeId
   * @param {string} entryEdgeLabel
   * @returns {{ first: string|null, last: string }}
   */
  _scanBlockForProcessGraph(steps, ctx, graph, entryFromNodeId, entryEdgeLabel = "") {
    if (!Array.isArray(steps) || steps.length === 0) {
      return { first: null, last: entryFromNodeId };
    }

    let prev = entryFromNodeId;
    let first = null;

    for (let index = 0; index < steps.length; index++) {
      if (graph.nodes.size >= ctx.maxNodes) {
        graph.notes.push(`Stopped parsing at node cap (${ctx.maxNodes}).`);
        break;
      }

      const step = steps[index];
      const stepPath = ctx.stepPathPrefix ? `${ctx.stepPathPrefix}/${index}` : `${index}`;
      const kind = this._pClassifyStep(step);
      const kw = this._pKeyword(step);

      // Loop handling (Repeat while / Repeat for each / etc)
      if (kind === "loop") {
        const loopId = this._pNodeId(`S_${ctx.recipeId}_${stepPath}`);
        this._pAddNode(graph.nodes, loopId, {
          id: loopId,
          kind: "loop",
          provider: step?.provider || "system",
          step_path: stepPath,
          label: this._pStepLabel(step, "loop"),
          branch_context: (ctx.branchStack || []).join(" / ")
        });

        // Connect prev -> loop
        const isFirstEdge = (prev === entryFromNodeId && !first);
        if (isFirstEdge && entryEdgeLabel) this._pAddEdge(graph.edges, prev, loopId, entryEdgeLabel, "flow");
        else this._pAddEdge(graph.edges, prev, loopId, "", "flow");
        if (!first) first = loopId;

        // Loop body (iterate)
        const loopCtx = {
          ...ctx,
          stepPathPrefix: stepPath,
          branchStack: (ctx.branchStack || []).slice().concat([`LOOP ${this._pLoopLabel(step)}`])
        };
        const bodyRes = this._scanBlockForProcessGraph(step.block || [], loopCtx, graph, loopId, "iterate");

        // Back-edge to represent repetition
        if (bodyRes.first) {
          this._pAddEdge(graph.edges, bodyRes.last, loopId, "repeat", "loop");
        } else {
          graph.notes.push(`Loop body empty at step_path=${stepPath}`);
        }

        // Exit/merge after loop
        const afterLoopId = this._pNodeId(`M_${ctx.recipeId}_${stepPath}_after_loop`);
        this._pAddNode(graph.nodes, afterLoopId, {
          id: afterLoopId,
          kind: "merge",
          provider: "system",
          step_path: `${stepPath}/after_loop`,
          label: "After loop",
          branch_context: (ctx.branchStack || []).join(" / ")
        });

        // "done" edge (conceptual exit)
        this._pAddEdge(graph.edges, loopId, afterLoopId, "done", "flow");

        // Optional else_block (often conceptually "empty list" / "no more")
        if (Array.isArray(step.else_block) && step.else_block.length > 0) {
          const elseCtx = {
            ...ctx,
            stepPathPrefix: `${stepPath}/else`,
            branchStack: (ctx.branchStack || []).slice().concat(["LOOP_ELSE"])
          };
          const elseRes = this._scanBlockForProcessGraph(step.else_block, elseCtx, graph, loopId, "empty");
          const elseExit = elseRes.first ? elseRes.last : loopId;
          this._pAddEdge(graph.edges, elseExit, afterLoopId, "", "flow");
        }

        prev = afterLoopId;
        continue;
      }

      // Decision handling (IF/ELSIF w/chain grouping)
      // If we see an IF followed by one or more sibling ELSIF steps, treat them as one chain.
      if (kind === "decision" && kw === "if") {
        const chain = [{ step, stepPath }];
        let j = index + 1;
        while (j < steps.length && this._pIsElsif(steps[j])) {
          const p = ctx.stepPathPrefix ? `${ctx.stepPathPrefix}/${j}` : `${j}`;
          chain.push({ step: steps[j], stepPath: p });
          j++;
        }

        // Only treat as a chain if we actually found ELSIF siblings
        if (chain.length > 1) {
          // Create decision nodes for IF and each ELSIF, wiring false-path to the next condition.
          let lastDecisionId = null;
          const thenExits = [];

          for (let ci = 0; ci < chain.length; ci++) {
            const c = chain[ci];
            const decisionId = this._pNodeId(`S_${ctx.recipeId}_${c.stepPath}`);
            this._pAddNode(graph.nodes, decisionId, {
              id: decisionId,
              kind: "decision",
              provider: c.step?.provider || "system",
              step_path: c.stepPath,
              label: this._pStepLabel(c.step, "decision"),
              branch_context: (ctx.branchStack || []).join(" / ")
            });

            // Connect into the first decision from prev, and subsequent decisions from prior false-path.
            if (ci === 0) {
              const isFirstEdge = (prev === entryFromNodeId && !first);
              if (isFirstEdge && entryEdgeLabel) this._pAddEdge(graph.edges, prev, decisionId, entryEdgeLabel, "flow");
              else this._pAddEdge(graph.edges, prev, decisionId, "", "flow");
              if (!first) first = decisionId;
            } else {
              // prior decision false -> next decision
              this._pAddEdge(graph.edges, lastDecisionId, decisionId, "false", "flow");
            }

            // THEN branch for this condition
            const thenCtx = {
              ...ctx,
              stepPathPrefix: c.stepPath,
              branchStack: (ctx.branchStack || []).slice().concat([this._pDecisionBranchLabel(c.step)])
            };
            const thenRes = this._scanBlockForProcessGraph(c.step.block || [], thenCtx, graph, decisionId, "true");
            thenExits.push(thenRes.first ? thenRes.last : decisionId);

            lastDecisionId = decisionId;
          }

          // ELSE (final false) comes from the last decision's else_block (if any).
          // Prefer the IF's else_block; if absent, take first else_block found in chain.
          let elseBlock = chain[0].step?.else_block;
          if (!Array.isArray(elseBlock) || elseBlock.length === 0) {
            for (const c of chain) {
              if (Array.isArray(c.step?.else_block) && c.step.else_block.length > 0) {
                elseBlock = c.step.else_block;
                break;
              }
            }
          }

          // Merge node for the entire chain
          const chainMergeId = this._pNodeId(`M_${ctx.recipeId}_${chain[0].stepPath}_chain_merge`);
          this._pAddNode(graph.nodes, chainMergeId, {
            id: chainMergeId,
            kind: "merge",
            provider: "system",
            step_path: `${chain[0].stepPath}/chain_merge`,
            label: "Merge",
            branch_context: (ctx.branchStack || []).join(" / ")
          });

          // Connect all THEN exits into the chain merge
          thenExits.forEach(exitId => this._pAddEdge(graph.edges, exitId, chainMergeId, "", "flow"));

          // ELSE path: either a real else_block or direct false->merge
          if (Array.isArray(elseBlock) && elseBlock.length > 0) {
            const elseCtx = {
              ...ctx,
              stepPathPrefix: `${chain[0].stepPath}/else`,
              branchStack: (ctx.branchStack || []).slice().concat(["ELSE"])
            };
            const elseRes = this._scanBlockForProcessGraph(elseBlock, elseCtx, graph, lastDecisionId, "false");
            const elseExit = elseRes.first ? elseRes.last : lastDecisionId;
            this._pAddEdge(graph.edges, elseExit, chainMergeId, "", "flow");
          } else {
            this._pAddEdge(graph.edges, lastDecisionId, chainMergeId, "false", "flow");
          }

          prev = chainMergeId;
          // Skip over the ELSIF siblings we've consumed
          index = j - 1;
          continue;
        }
      }

      // Single decision (standalone IF or standalone ELSIF)
      if (kind === "decision") {
        const decisionId = this._pNodeId(`S_${ctx.recipeId}_${stepPath}`);
        this._pAddNode(graph.nodes, decisionId, {
          id: decisionId,
          kind: "decision",
          provider: step?.provider || "system",
          step_path: stepPath,
          label: this._pStepLabel(step, kind),
          branch_context: (ctx.branchStack || []).join(" / ")
        });

        // entry edge from prev -> decision
        const isFirstEdge = (prev === entryFromNodeId && !first);
        if (isFirstEdge && entryEdgeLabel) this._pAddEdge(graph.edges, prev, decisionId, entryEdgeLabel, "flow");
        else this._pAddEdge(graph.edges, prev, decisionId, "", "flow");
        if (!first) first = decisionId;

        // THEN block
        const thenCtx = {
          ...ctx,
          stepPathPrefix: stepPath,
          branchStack: (ctx.branchStack || []).slice().concat([this._pDecisionBranchLabel(step)])
        };
        const thenRes = this._scanBlockForProcessGraph(step.block || [], thenCtx, graph, decisionId, "true");

        // ELSE block
        const elseCtx = {
          ...ctx,
          stepPathPrefix: `${stepPath}/else`,
          branchStack: (ctx.branchStack || []).slice().concat(["ELSE"])
        };
        const elseRes = this._scanBlockForProcessGraph(step.else_block || [], elseCtx, graph, decisionId, "false");

        // Merge
        const mergeId = this._pNodeId(`M_${ctx.recipeId}_${stepPath}_merge`);
        this._pAddNode(graph.nodes, mergeId, {
          id: mergeId,
          kind: "merge",
          provider: "system",
          step_path: `${stepPath}/merge`,
          label: "Merge",
          branch_context: (ctx.branchStack || []).join(" / ")
        });

        // Connect branch ends to merge (if branch empty, connect decision to merge)
        this._pAddEdge(graph.edges, (thenRes.first ? thenRes.last : decisionId), mergeId, "", "flow");
        this._pAddEdge(graph.edges, (elseRes.first ? elseRes.last : decisionId), mergeId, "", "flow");

        prev = mergeId;
        continue;
      }

      // Normal step node
      const nodeId = this._pNodeId(`S_${ctx.recipeId}_${stepPath}`);
      this._pAddNode(graph.nodes, nodeId, {
        id: nodeId,
        kind,
        provider: step?.provider || "system",
        step_path: stepPath,
        label: this._pStepLabel(step, kind),
        branch_context: (ctx.branchStack || []).join(" / ")
      });

      const isFirstEdge = (prev === entryFromNodeId && !first);
      if (isFirstEdge && entryEdgeLabel) this._pAddEdge(graph.edges, prev, nodeId, entryEdgeLabel, "flow");
      else this._pAddEdge(graph.edges, prev, nodeId, "", "flow");
      if (!first) first = nodeId;

      // Optional nested block (treated as sequential continuation)
      let mainExit = nodeId;
      if (Array.isArray(step.block) && step.block.length > 0) {
        const childCtx = {
          ...ctx,
          stepPathPrefix: stepPath,
          branchStack: (ctx.branchStack || []).slice()
        };
        const childRes = this._scanBlockForProcessGraph(step.block, childCtx, graph, nodeId, "");
        mainExit = childRes.last || nodeId;
      }

      // Error branch
      if (Array.isArray(step.error_block) && step.error_block.length > 0) {
        const mergeId = this._pNodeId(`M_${ctx.recipeId}_${stepPath}_err_merge`);
        this._pAddNode(graph.nodes, mergeId, {
          id: mergeId,
          kind: "merge",
          provider: "system",
          step_path: `${stepPath}/err_merge`,
          label: "Merge",
          branch_context: (ctx.branchStack || []).join(" / ")
        });

        // OK path
        this._pAddEdge(graph.edges, mainExit, mergeId, "ok", "flow");

        // Error path
        const errCtx = {
          ...ctx,
          stepPathPrefix: `${stepPath}/error`,
          branchStack: (ctx.branchStack || []).slice().concat(["ON_ERROR"])
        };
        const errRes = this._scanBlockForProcessGraph(step.error_block, errCtx, graph, nodeId, "error");
        const errExit = errRes.first ? errRes.last : nodeId;
        this._pAddEdge(graph.edges, errExit, mergeId, "", "flow");

        prev = mergeId;
      } else {
        prev = mainExit;
      }
    }

    return { first, last: prev };
  }
}
/**
 * @class
 * @classdesc Service for interacting with Google Vertex AI (Gemini).
 */
class GeminiService {
  constructor() {
    this.config = AppConfig.get().VERTEX;

    this.projectId = this.config.GOOGLE_CLOUD_PROJECT_ID;
    this.modelId = this.config.MODEL_ID;
    this.location = this.config.LOCATION;
    this.generationConfig = this.config.GENERATION_CONFIG;

    if (!this.projectId) {
      console.warn("GeminiService: 'GOOGLE_CLOUD_PROJECT_ID' is missing in Script Properties.");
    }
  }
  /**
   * Generates a natural language summary of a Workato recipe.
   * @param {Object} recipe - The full recipe object.
   * @returns {string} The AI-generated summary.
   */
  explainRecipe(recipe, graphPack = null, logicDigest = "") {
    if (!this.projectId) throw new Error("Missing GOOGLE_CLOUD_PROJECT_ID in Script Properties");

    const ctx = this._prepareContext(recipe, graphPack, logicDigest);
    const prompt = this._buildPrompt(ctx);

    return this._callVertexAI(prompt);
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
  /**
   * Executes the API call to Vertex AI.
   * @private
   */
  _callVertexAI(textPrompt) {
    const endpoint = `https://${this.location}-aiplatform.googleapis.com/v1/projects/${this.projectId}/locations/${this.location}/publishers/google/models/${this.modelId}:generateContent`;
    
    const payload = {
      contents: [{
        role: "user",
        parts: [{ text: textPrompt }]
      }],
      generationConfig: {
        temperature: this.generationConfig.TEMPERATURE,
        maxOutputTokens: this.generationConfig.MAX_OUTPUT_TOKENS
      }
    };

    const options = {
      method: 'post',
      contentType: 'application/json',
      payload: JSON.stringify(payload),
      headers: {
        // Apps Script automatically handles the OAuth token for Google services
        Authorization: `Bearer ${ScriptApp.getOAuthToken()}`
      },
      muteHttpExceptions: true
    };

    const maxRetries = Number(this.config.MAX_RETRIES || 3);
    let attempt = 0;
    while (attempt <= maxRetries) {
      try {
        const response = UrlFetchApp.fetch(endpoint, options);
        const code = response.getResponseCode();
        const json = JSON.parse(response.getContentText());

        if (json.error) {
          // Retry 429 / 5xx (Gemini throttles happen)
          if (code === 429 || code >= 500) {
            if (attempt === maxRetries) throw new Error(`Vertex AI Error: ${json.error.message}`);
            Utilities.sleep(Math.pow(2, attempt) * 1000);
            attempt++;
            continue;
          }
          throw new Error(`Vertex AI Error: ${json.error.message}`);
        }
      
        if (json.candidates && json.candidates[0] && json.candidates[0].content) {
          return json.candidates[0].content.parts[0].text;
        }
        return "No content generated.";

      } catch (e) {
        if (attempt === maxRetries) {
          console.error(e);
          return `AI Analysis Failed: ${e.message}`;
        }
        Utilities.sleep(Math.pow(2, attempt) * 1000);
        attempt++;
      }
    }
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
 */
class WorkatoSyncApp {
  constructor() {
    this.config = AppConfig.get();
    const client = new WorkatoClient();
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
  runLogicDebug() {
    try {
      Logger.verbose("Starting recipe logic debugging...");
      
      // 1. Read input 
      const requestedIds = this.sheetService.readRequests();
      if (requestedIds.length === 0) {
        Logger.notify("No IDs found in the 'logic_requests' sheet.", true);
        return;
      }
      Logger.notify(`Fetching logic for ${requestedIds.length} recipes...`);

      // 2. Fetch and parse logic
      const logicRows = [this.config.HEADERS.LOGIC];
      const debugLogs = [];

      requestedIds.forEach((reqId, index) => {
        try {
          const fullRecipe = this.analyzerService.client.get(`recipes/${reqId}`);
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
  runAiAnalysis() {
    const gemini = new GeminiService();
    const ids = this.sheetService.readRequests();

    if(ids.length === 0) {
      Logger.notify("No IDs to analyze.");
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
        const recipe = this.analyzerService.getRecipeDetails(id) || this.inventoryService.client.get(`recipes/${id}`);
        const name = recipe?.name || "";

        // 1) Build graphs (same source of truth as your Mermaid exports)
        const graphPack = this.analyzerService.buildGraphPack(id, { callDepth: depth, maxNodes, edgeSampleLimit: 70 });

        // 2) Build a flattened logic digest (LLM-friendly)
        const logicRows = this.analyzerService.parseLogicRows(recipe);
        const digest = this._logicDigestFromRows(logicRows, maxLines);

        // 3) Call Gemini with grounded context
        const explanation = gemini.explainRecipe(recipe, graphPack, digest);

        // 4) Drive links (full artifacts)
        const aiUrl = this.driveService.saveText(id, name || `recipe_${id}`, "ai.txt", explanation || "");
        const callsUrl = this.driveService.saveText(id, name || `recipe_${id}`, "calls.mmd", graphPack?.call?.mermaid || "");
        const fullUrl  = this.driveService.saveText(id, name || `recipe_${id}`, "full.mmd",  graphPack?.process?.mermaid || "");

        const aiLink    = aiUrl    ? `=HYPERLINK("${aiUrl}", "View AI full")` : "";
        const callsLink = callsUrl ? `=HYPERLINK("${callsUrl}", "View calls mermaid")` : "";
        const fullLink  = fullUrl  ? `=HYPERLINK("${fullUrl}", "View full mermaid")` : "";

        const preview = (explanation && explanation.length > 4000)
          ? explanation.slice(0, 4000) + "\n…(truncated preview; see Drive link)"
          : (explanation || "");

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
   * Reads recipe IDs from 'logic_requests' and generates process maps.
   * Modes:
   * - "calls"      : transitive recipe-call graph only
   * - "full"       : step-level process map only
   * - "calls+full" : both
   *
   * @param {{ mode?: string, callDepth?: number, maxNodes?: number, exportTables?: boolean }} [options]
   */
  runProcessMaps(options = {}) {
    try {
      Logger.verbose("Starting process map generation...");

      const requestedIds = this.sheetService.readRequests();
      if (requestedIds.length === 0) {
        Logger.notify("No IDs found in the 'logic_requests' sheet.", true);
        return;
      }

      const mode = String(options.mode || this.config.API.PROCESS_MAP_MODE_DEFAULT || "calls+full");
      const depth = Number(options.callDepth ?? this.config.API.PROCESS_MAP_DEPTH ?? 0);
      const maxNodes = Number(options.maxNodes ?? this.config.API.PROCESS_MAP_MAX_NODES ?? 250);
      const exportTables = (options.exportTables !== undefined)
        ? Boolean(options.exportTables)
        : Boolean(this.config.API.PROCESS_MAP_EXPORT_TABLES);

      const CHAR_LIMIT = this.config.CONSTANTS.CELL_CHAR_LIMIT || 48000;

      const rows = [this.config.HEADERS.PROCESS_MAPS];
      const nodeRows = [this.config.HEADERS.PROCESS_NODES];
      const edgeRows = [this.config.HEADERS.PROCESS_EDGES];

      requestedIds.forEach((rootId, idx) => {
        const rootRecipe = this.analyzerService.getRecipeDetails(rootId);
        const rootName = rootRecipe?.name || "";

        let callMermaid = "";
        let fullMermaid = "";
        let notes = [];
        let callDriveLink = "";
        let fullDriveLink = "";

        // --- Calls graph ---
        if (mode === "calls" || mode === "calls+full") {
          const callGraph = this.analyzerService.buildTransitiveCallGraph(rootId, depth);
          callMermaid = this.analyzerService.renderMermaidCallGraph(rootId, callGraph);
          notes = notes.concat((callGraph.notes || []).slice(0, 10));

          if (callMermaid.length > CHAR_LIMIT) {
            const url = this.driveService.saveText(rootId, rootName || `recipe_${rootId}`, "calls.mmd", callMermaid);
            if (url) {
              callDriveLink = `=HYPERLINK("${url}", "View calls mermaid")`;
              callMermaid = callMermaid.substring(0, Math.max(0, CHAR_LIMIT - 200)) + "\n...TRUNCATED IN SHEET (see Drive link).";
              notes.push("Calls mermaid truncated in cell due to cell limits.");
            } else {
              callMermaid = callMermaid.substring(0, Math.max(0, CHAR_LIMIT - 200)) + "\n...TRUNCATED IN SHEET (Drive save failed).";
              notes.push("Calls mermaid truncated in sheet; Drive save failed.");
            }
          }
        }

        // --- Full process map ---
        if (mode === "full" || mode === "calls+full") {
          const procGraph = this.analyzerService.buildProcessGraph(rootId, { maxNodes });
          fullMermaid = this.analyzerService.renderMermaidProcessGraph(rootId, procGraph);
          notes = notes.concat((procGraph.notes || []).slice(0, 10));

          if (exportTables) {
            nodeRows.push(...DataMapper.mapProcessNodesToRows(rootId, rootName, procGraph));
            edgeRows.push(...DataMapper.mapProcessEdgesToRows(rootId, procGraph));
          }

          if (fullMermaid.length > CHAR_LIMIT) {
            const url = this.driveService.saveText(rootId, rootName || `recipe_${rootId}`, "full.mmd", fullMermaid);
            if (url) {
              fullDriveLink = `=HYPERLINK("${url}", "View full mermaid")`;
              fullMermaid = fullMermaid.substring(0, Math.max(0, CHAR_LIMIT - 200)) + "\n...TRUNCATED IN SHEET (see Drive link).";
              notes.push("Full mermaid truncated in cell due to cell limits.");
            } else {
              fullMermaid = fullMermaid.substring(0, Math.max(0, CHAR_LIMIT - 200)) + "\n...TRUNCATED IN SHEET (Drive save failed).";
              notes.push("Full mermaid truncated in sheet; Drive save failed.");
            }
          }
        }

        const notesCell = notes.filter(Boolean).slice(0, 20).join("\n");

        rows.push([
          String(rootId),
          rootName,
          mode,
          String(depth),
          callMermaid,
          fullMermaid,
          notesCell,
          callDriveLink,
          fullDriveLink,
          new Date().toISOString()
        ]);

        if (idx % 2 === 0) Utilities.sleep(this.config.API.THROTTLE_MS);
      });

      this.sheetService.write('PROCESS_MAPS', rows);
      if (exportTables) {
        this.sheetService.write('PROCESS_NODES', nodeRows);
        this.sheetService.write('PROCESS_EDGES', edgeRows);
      }
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
    this.props = PropertiesService.getScriptProperties();
  }

  /**
   * Builds and displays the custom menu.
   */
  createMenu() {
    this.ui.createMenu('Workato Sync')
      .addSubMenu(this.ui.createMenu('Actions')
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
   * * @param {string} key - The ScriptProperty key to update.
   * @param {string} title - The title of the prompt dialog.
   * @param {boolean} [isSecret=false] - If true, masks the output confirmation.
   */
  promptUpdate(key, title, isSecret = false) {
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
          this.props.deleteProperty(key);
          this.ui.alert('Property deleted. Script will use code-level defaults.');
        }
        return;
      }

      // Save
      this.props.setProperty(key, input);
      
      // Feedback
      const displayValue = isSecret 
        ? `${input.substring(0, 4)}...${input.substring(input.length - 4)}` 
        : input;
      
      this.ui.alert(`Saved ${key}: ${displayValue}`);
    }
  }
  /**
   * Displays the current configuration state in a formatted alert.
   */
  showConfiguration() {
    const storedProps = this.props.getProperties();
    const defaults = AppConfig.get().API; // Access static defaults
    
    // Logic to determine display strings (Set vs Default vs Missing)
    const tokenStatus = storedProps['WORKATO_TOKEN'] ? "******** (Set)" : "❌ NOT SET";
    const urlStatus = storedProps['WORKATO_BASE_URL'] || `${defaults.BASE_URL} (Default)`;
    const folderStatus = storedProps['DEBUG_FOLDER_ID'] || "(Auto-generated)";
    
    const msg = [
      `API Token: ${tokenStatus}`,
      `Base URL: ${urlStatus}`,
      `Debug Folder ID: ${folderStatus}`,
      ``,
      `To change these, use the 'Configuration' menu.`
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
// HANDLES
// -------------------------------------------------------------------------------------------------------
function onOpen() {
  new UserInterfaceService().createMenu();
}
function promptToken() {
  new UserInterfaceService().promptUpdate('WORKATO_TOKEN', 'Update API Token', true);
}
function promptBaseUrl() {
  new UserInterfaceService().promptUpdate('WORKATO_BASE_URL', 'Update Base URL', false);
}
function promptFolderId() {
  new UserInterfaceService().promptUpdate('DEBUG_FOLDER_ID', 'Update Debug Folder ID', false);
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
