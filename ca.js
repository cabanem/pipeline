/**
 * @file Workato Inventory Sync
 * @description Fetches all resources from Workato and logs them to a dedicated Google Sheet.
 * @author Emily Cabaniss
 * @see DOCS (Workato developer API): "https://docs.workato.com/en/workato-api.html"
 */

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
    return {
      API: {
        TOKEN: PropertiesService.getScriptProperties().getProperty('WORKATO_TOKEN'),
        BASE_URL: 'https://app.eu.workato.com/api',
        PER_PAGE: 100,
        MAX_CALLS: 500,
        THROTTLE_MS: 100,       
        RECIPE_LIMIT_DEBUG: 100 
      },
      SHEETS: {
        RECIPES: 'recipes',
        FOLDERS: 'folders',
        PROJECTS: 'projects',
        PROPERTIES: 'properties',
        DEPENDENCIES: 'recipe_dependencies'
      },
      HEADERS: {
        PROJECTS: ["ID", "Name", "Description", "Created at"],
        FOLDERS: ["ID", "Name", "Parent folder", "Project name"],
        RECIPES: ["ID", "Name", "Status", "Project", "Folder", "Last Run"],
        DEPENDENCIES: ["Parent Recipe ID", "Dependency Type", "Dependency ID", "Dependency Name"],
        PROPERTIES: ["ID", "Name", "Value", "Created at", "Updated at"]
      },
      CONSTANTS: {
        RECIPE_PROVIDERS: ['workato_recipe_function', 'workato_callable_recipe'],
        FLOW_ID_KEYS: ['flow_id', 'recipe_id', 'callable_recipe_id'],
        STYLE_HEADER_BG: "#efefef"
      },
      VERBOSE: true
    };
  }
}
/**
 * @class
 * @classdesc Static utility for logging to both Apps Script console and Sheets UI.
 */
class Logger {
  /**
   * @description Logs a message to the console only if VERBOSE mode is enabled in Config.
   * @param {string} msg - The message to log.
   */
  static verbose(msg) {
    if (AppConfig.get().VERBOSE) console.log(`[VERBOSE] ${msg}`);
  }
  /**
   * @description Logs to console and displays a Toast notification in the active Spreadsheet.
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
      console.log("UI notification skipped.");
    }
  }
}
/**
 * @class
 * @classdesc Low-level HTTP wrapper for the Workato API.
 * * Handles Authentication, Base URL construction, and Pagination loops.
 */
class WorkatoClient {
  constructor() {
    /** @type {APIConfig} */
    this.config = AppConfig.get().API;
  }
  /**
   * @description Executes a single GET request to the Workato API.
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

    const response = UrlFetchApp.fetch(url, options);
    
    if (response.getResponseCode() !== 200) {
      throw new Error(`API Error [${endpoint}]: ${response.getResponseCode()} - ${response.getContentText()}`);
    }

    return JSON.parse(response.getContentText());
  }
  /**
   * @description Fetches all records from a resource by automatically following pagination.
   * @param {string} resourcePath - The resource endpoint (e.g., 'recipes').
   * @returns {Array<Object>} An aggregated array of all items across all pages.
   */
  fetchPaginated(resourcePath) {
    let results = [];
    let page = 1;
    let hasMore = true;

    while (hasMore) {
      // Append query params manually as per original script logic
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
      if (page > 200) break;
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
   * @description Fetches all available projects.
   * @returns {Array<Object>} List of project objects.
   */
  getProjects() {
    return this.client.fetchPaginated('projects');
  }
  /**
   * @description Fetches all recipes.
   * @returns {Array<Object>} List of recipe objects.
   */
  getRecipes() {
    return this.client.fetchPaginated('recipes');
  }
/**
   * @description Fetches workspace properties.
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

    Logger.verbose(`Starting Hybrid Sync for ${projects.length} Projects + Workspace Root...`);

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
      
      // We need to paginate the children of this specific folder
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
 * @classdesc Service for parsing recipe dependencies and code blocks.
 * * Analyses both standard App Connections and logic-based 'Recipe Calls'.
 */
class DependencyService {
  /**
   * @param {WorkatoClient} client - An initialized API client.
   */
  constructor(client) {
    this.client = client;
    this.constants = AppConfig.get().CONSTANTS;
  }
  /**
   * Fetches dependencies for a specific recipe.
   * Retrieves standard connections AND scans code for child recipe calls.
   * * @param {string|number} recipeId - The ID of the recipe to analyze.
   * @returns {Array<Object>} List of dependency objects (Connections or Recipe Calls).
   */
  getForRecipe(recipeId) {
    // We access the client directly here because we need specific error handling
    // that returns an empty array rather than throwing, preserving original logic.
    let json;
    try {
      json = this.client.get(`recipes/${recipeId}`);
    } catch (e) {
      console.warn(`Could not fetch details for recipe ${recipeId}`);
      return [];
    }

    let dependencies = [];

    // 1. STANDARD APPS
    if (json.applications && Array.isArray(json.applications)) {
      json.applications.forEach(app => {
        if (!this.constants.RECIPE_PROVIDERS.includes(app)) {
          dependencies.push({
            type: 'Connection',
            id: app,
            name: app
          });
        }
      });
    }

    // 2. RECIPE CALLS (Deep Scan)
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
   * Recursive helper to find nested recipe calls within code blocks.
   * * @param {Array<Object>} steps - The code block steps to scan.
   * @param {Array<Object>} resultsArray - The accumulator array for found dependencies.
   * @private
   */
  _scanBlockForCalls(steps, resultsArray) {
    if (!Array.isArray(steps)) return;

    steps.forEach(step => {
      // 1. Check provider
      if (this.constants.RECIPE_PROVIDERS.includes(step.provider)) {
        
        // 2. Find ID key
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
   * * @param {string} sheetKey - Key corresponding to CONFIG.SHEETS (e.g., 'RECIPES').
   * @param {Array<Array<any>>} rows - The data to write.
   * @throws {Error} If the sheetKey does not exist in the configuration.
   */
  write(sheetKey, rows) {
    // sheetKey matches keys in CONFIG.SHEETS (e.g., 'RECIPES', 'FOLDERS')
    const sheetName = this.config.SHEETS[sheetKey];
    if (!sheetName) throw new Error(`Sheet key ${sheetKey} not found in config.`);

    Logger.verbose(`Writing ${rows.length} rows to ${sheetName}...`);

    const ss = SpreadsheetApp.getActiveSpreadsheet();
    let sheet = ss.getSheetByName(sheetName) || ss.insertSheet(sheetName);

    sheet.clear();

    if (rows.length > 0) {
      // Perform the write
      sheet.getRange(1, 1, rows.length, rows[0].length).setValues(rows);
      
      // Apply Formatting
      sheet.getRange(1, 1, 1, rows[0].length)
           .setFontWeight("bold")
           .setBackground(this.config.CONSTANTS.STYLE_HEADER_BG);
      
      sheet.setFrozenRows(1);
    }
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
    
    // Initialize Infrastructure & Services
    const client = new WorkatoClient();
    this.inventoryService = new InventoryService(client);
    this.dependencyService = new DependencyService(client);
    this.sheetService = new SheetService();
  }
  /**
   * The main execution method. 
   * Performs authentication check, fetches all resources, transforms data, 
   * resolves dependencies, and writes to Sheets.
   */
  run() {
    try {
      Logger.verbose("Starting full workspace sync...");

      // 1. Identify present workspace
      const currentUser = this.inventoryService.getCurrentUser();
      if (currentUser) {
        console.log(`Authenticated as ${currentUser.name || "Unknown user"}`);
        console.log(`Connected to workspace: ${currentUser.current_account_name || "Unknown workspace"}`);
      }

      // 2. Fetch all raw data
      const projects = this.inventoryService.getProjects();
      const folders = this.inventoryService.getFoldersRecursive(projects);
      const recipes = this.inventoryService.getRecipes();
      const properties = this.inventoryService.getProperties();

      Logger.verbose(`Fetched totals: ${projects.length} projects, ${folders.length} folders, ${recipes.length} recipes, ${properties.length} properties`);

      // 3. Create lookup maps
      const projectMap = this._createLookupMap(projects);
      const folderMap = this._createLookupMap(folders);
      const recipeMap = this._createLookupMap(recipes);

      // 4. Prep data for sheets (Transformation Layer)
      const projectRows = this._transformProjects(projects);
      const folderRows = this._transformFolders(folders, folderMap, projectMap);
      const recipeRows = this._transformRecipes(recipes, projectMap, folderMap);
      const propertyRows = this._transformProperties(properties);
      
      // 5. Dependency Logic (Complex processing)
      const dependencyRows = this._processDependencies(recipes, recipeMap);

      // 6. Write to Sheets
      Logger.verbose("Writing to Sheets...");
      this.sheetService.write('PROJECTS', projectRows);
      this.sheetService.write('FOLDERS', folderRows);
      this.sheetService.write('RECIPES', recipeRows);
      this.sheetService.write('PROPERTIES', propertyRows);
      this.sheetService.write('DEPENDENCIES', dependencyRows);

      Logger.notify("Sync complete. Workspace inventory updated...", false);

    } catch (e) {
      let errorMsg = `Sync failed: ${e.message}`;
      if (e.message.includes("Unexpected token")) {
        errorMsg = "Authentication error: Check your WORKATO_TOKEN, and BASE_URL";
      }
      Logger.notify(errorMsg, true);
      console.error(e.stack);
    }
  }

  // --- INTERNAL TRANSFORMATION HELPERS ---
  /** @private */
  _createLookupMap(items) {
    return Object.fromEntries(items.map(i => [i.id, i.name]));
  }
  /** @private */
  _transformProjects(projects) {
    const header = this.config.HEADERS.PROJECTS;
    const rows = projects.map(p => [p.id, p.name, p.description, p.created_at]);
    return [header].concat(rows);
  }
  /** @private */
  _transformFolders(folders, folderMap, projectMap) {
    const header = this.config.HEADERS.FOLDERS;
    const rows = folders.map(f => {
      let parentName = "TOP LEVEL";
      if (f.is_project) {
        parentName = "Workspace Root (Home)";
      } else if (f.parent_id) {
        parentName = folderMap[f.parent_id] || `[ID: ${f.parent_id}] (not found)`;
      }
      const projectName = projectMap[f.project_id] || `[ID: ${f.project_id}]`;
      return [f.id, f.name, parentName, projectName];
    });
    return [header].concat(rows);
  }
  /** @private */
  _transformRecipes(recipes, projectMap, folderMap) {
    const header = this.config.HEADERS.RECIPES;
    const rows = recipes.map(r => [
      r.id,
      r.name,
      r.running ? "ACTIVE" : "STOPPED",
      projectMap[r.project_id] || r.project_id,
      folderMap[r.folder_id] || `[Unknown/Deleted: ${r.folder_id}]`,
      r.last_run_at || "NEVER"
    ]);
    return [header].concat(rows);
  }
  /** @private */
  _transformProperties(properties) {
    const header = this.config.HEADERS.PROPERTIES;
    const rows = properties.map(p => [p.id, p.name, p.value, p.created_at, p.updated_at]);
    return [header].concat(rows);
  }
  /**
   * Handles the complex logic of iterating recipes to identify dependencies.
   * Applies throttling and safety limits to avoid execution timeouts.
   * @private
   */
  _processDependencies(recipes, recipeMap) {
    const rows = [this.config.HEADERS.DEPENDENCIES];
    const limit = this.config.API.RECIPE_LIMIT_DEBUG;
    const throttle = this.config.API.THROTTLE_MS;

    recipes.forEach((recipe, index) => {
      if (index < limit) {
        // Delegate fetching to the service
        const deps = this.dependencyService.getForRecipe(recipe.id);
        
        deps.forEach(dep => {
          let finalName = dep.name;
          
          if (dep.type === 'RECIPE CALL') {
            const childRecipeName = recipeMap[dep.id];
            if (childRecipeName) {
              finalName = childRecipeName;
            } else {
              finalName = `[Unknown Recipe ID: ${dep.id}]`;
            }
          }
          rows.push([
            recipe.id,
            dep.type,
            dep.id,
            finalName
          ]);
        });

        // Throttle
        if (index % 5 === 0) Utilities.sleep(throttle);
      }
    });

    return rows;
  }
}

// -------------------------------------------------------------------------------------------------------
// ENTRY POINTS
// -------------------------------------------------------------------------------------------------------
/**
 * Primary entry point for the script. 
 * Initializes the WorkatoSyncApp controller and runs the sync.
 */
function syncWorkatoWorkspace() {
  const app = new WorkatoSyncApp();
  app.run();
}
/**
 * Validates the connection to the Workato API across all primary endpoints.
 * Uses the Class-based WorkatoClient.
 */
function testWorkatoConnectivity() {
  console.log("--- TESTING CONNECTIVITY (OOP Version) ---");
  const client = new WorkatoClient();
  const endpoints = ['projects', 'folders', 'recipes', 'properties'];

  endpoints.forEach(endpoint => {
    try {
      // We manually construct the query here as client.get() expects a path
      const path = `${endpoint}?page=1&per_page=1`;
      
      // The client.get method throws on non-200, so if this runs, it worked.
      const json = client.get(path); 
      
      console.log(`\n--- [${endpoint.toUpperCase()}] Status: OK ---`);
      
      // Determine count for display
      let count = 0;
      if (Array.isArray(json)) count = json.length;
      else if (json.items) count = json.items.length;
      else if (json.result) count = json.result.length;
      
      console.log(`Fetched ${count} sample records.`);
      
    } catch (e) {
      console.error(`EXCEPTION calling /${endpoint}: ${e.message}`);
    }
  });

  console.log("\nDIAGNOSTIC COMPLETE");
}
