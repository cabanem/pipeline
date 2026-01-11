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
        DEPENDENCIES: 'recipe_dependencies',
        LOGIC: 'recipe_logic',
        LOGIC_INPUT: 'logic_requests'
      },
      HEADERS: {
        PROJECTS: ["ID", "Name", "Description", "Created at"],
        FOLDERS: ["ID", "Name", "Parent folder", "Project name"],
        RECIPES: ["ID", "Name", "Status", "Project", "Folder", "Last Run"],
        DEPENDENCIES: ["Parent Recipe ID", "Project", "Folder", "Dependency Type", "Dependency ID", "Dependency Name"],
        PROPERTIES: ["ID", "Name", "Value", "Created at", "Updated at"],
        LOGIC: ["Recipe ID", "Recipe Name", "Step Number", "Indent", "Provider", "Action/Name", "Description"],
        LOGIC_INPUT: ["Enter recipe IDs below (one per row)"]
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

// -------------------------------------------------------------------------------------------------------
// LOGGING
// // -------------------------------------------------------------------------------------------------------
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
      
      // 1. Get the full range for all data
      const fullRange = sheet.getRange(1, 1, numRows, numCols);
      
      // 2. Write values and apply global Left Alignment
      fullRange.setValues(rows)
               .setHorizontalAlignment("left");
      
      // 3. Apply Header Formatting (Bold, Background, Vertical Middle)
      sheet.getRange(1, 1, 1, numCols)
           .setFontWeight("bold")
           .setBackground(this.config.CONSTANTS.STYLE_HEADER_BG)
           .setVerticalAlignment("middle"); // <--- Added this
      
      sheet.setFrozenRows(1);
    }
  }
  /**
   * Reads a list of IDs from the request sheet.
   * @returns {string[]} Array of Recipe IDs found in column A.
   */
  readRequests() {
    const sheetName = this.config.SHEETS.LOGIC_INPUT;
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const sheet = ss.getSheetByName(sheetName);

    // If sheet doesn't exist, create it and add headers
    if (!sheet) {
      const newSheet = ss.insertSheet(sheetName);
      newSheet.getRange(1, 1).setValue(this.config.HEADERS.LOGIC_INPUT[0])
              .setFontWeight("bold").setBackground("#fff2cc"); // Yellow to indicate input
      return [];
    }

    const lastRow = sheet.getLastRow();
    if (lastRow < 2) return []; // No data

    // Get all values in Column A (excluding header)
    const values = sheet.getRange(2, 1, lastRow - 1, 1).getValues();
    
    // Flatten array and filter out empty strings
    return values.flat().filter(id => id).map(String);
  }
}

/**
 * @class
 * @classdesc Service for translating raw recipe code into human-readable logic summaries.
 */
class LogicService {
  /**
   * Generates a flat list of steps for a given recipe.
   * @param {Object} recipe - The recipe object (must include 'code').
   * @returns {Array<Array<string>>} Rows for the spreadsheet.
   */
  parseLogic(recipe) {
    if (!recipe.code) return [];

    let rows = [];
    try {
      const codeObj = JSON.parse(recipe.code);
      // Determine the root block (API variations exist)
      const rootBlock = codeObj.block || codeObj.line || [];
      
      // Start recursive scan at indentation level 0
      this._scanBlock(rootBlock, 0, recipe.id, recipe.name, rows);
    } catch (e) {
      console.warn(`Error parsing logic for recipe ${recipe.id}: ${e.message}`);
    }
    return rows;
  }

  /**
   * Recursive helper to traverse steps and formatting them.
   * @private
   */
  _scanBlock(steps, indentLevel, recipeId, recipeName, rows) {
    if (!Array.isArray(steps)) return;

    steps.forEach((step, index) => {
      // 1. Format the visual indent (e.g., ">> ")
      const visualIndent = "> ".repeat(indentLevel);
      
      // 2. Determine a friendly name for the action
      // Use the user-defined name if it exists, otherwise the system action name
      let actionName = step.name || step.as || "Unknown Action";
      
      // 3. Extract a description (optional)
      // Some steps have a 'description' field or comment
      let description = step.description || "";
      
      // If it's a conditional step (If/Else), make it clear
      if (step.keyword) {
         actionName = `[${step.keyword.toUpperCase()}] ${actionName}`;
      }

      // 4. Push row
      rows.push([
        String(recipeId),
        recipeName,
        index + 1,        // Step number in current block
        visualIndent,     // Visual hierarchy
        step.provider || "System",
        actionName,
        description
      ]);

      // 5. Recurse into nested blocks (If, Else, Loop, Try/Catch)
      if (step.block)       this._scanBlock(step.block, indentLevel + 1, recipeId, recipeName, rows);
      if (step.else_block)  this._scanBlock(step.else_block, indentLevel + 1, recipeId, recipeName, rows);
      if (step.error_block) this._scanBlock(step.error_block, indentLevel + 1, recipeId, recipeName, rows);
    });
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
    this.logicService = new LogicService();
    this.sheetService = new SheetService();
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
      const recipeMap = this._createLookupMap(recipes);

      // 4. Prep data for sheets (Transformation Layer)
      const projectRows = this._transformProjects(projects);
      const folderRows = this._transformFolders(folders, folderMap, projectMap);
      const recipeRows = this._transformRecipes(recipes, projectMap, folderMap);
      const propertyRows = this._transformProperties(properties);
      const dependencyRows = this._processDependencies(recipes, recipeMap, projectMap, folderMap);

      // 6. Write to Sheets
      Logger.verbose("Writing to Sheets...");
      this.sheetService.write('PROJECTS', projectRows);
      this.sheetService.write('FOLDERS', folderRows);
      this.sheetService.write('RECIPES', recipeRows);
      this.sheetService.write('PROPERTIES', propertyRows);
      this.sheetService.write('DEPENDENCIES', dependencyRows);

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
      Logger.verbose("=== STARTING LOGIC DEBUGGER ===");

      // 1. Read input
      const requestedIds = this.sheetService.readRequests();
      if (requestedIds.length === 0) {
        Logger.notify("No IDs found in the 'logic_requests' sheet.", true);
        return;
      }
      Logger.notify(`Fetching logic for ${requestedIds.length} recipes...`);

      // 2. Fetch and parse Llgic
      const rows = [this.config.HEADERS.LOGIC];
      
      requestedIds.forEach((reqId, index) => {
        try {
           // Fetch fresh details for this specific ID
           const fullRecipe = this.dependencyService.client.get(`recipes/${reqId}`);
           const logicRows = this.logicService.parseLogic(fullRecipe);
           
           rows.push(...logicRows);
           Logger.verbose(`Parsed: ${fullRecipe.name || reqId}`);

        } catch (e) {
           console.warn(`Failed ID ${reqId}: ${e.message}`);
           rows.push([reqId, "ERROR", "-", "-", "-", e.message, "-"]);
        }
        
        // Slight throttle
        if (index % 5 === 0) Utilities.sleep(this.config.API.THROTTLE_MS);
      });

      // 3. Write Output
      this.sheetService.write('LOGIC', rows);
      Logger.notify("Logic Debug Complete.");

    } catch (e) {
      this._handleError(e);
    }
  }

  // --- INTERNAL HELPERS ---
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
  _processDependencies(recipes, recipeMap, projectMap, folderMap) {
    const rows = [this.config.HEADERS.DEPENDENCIES];
    const limit = this.config.API.RECIPE_LIMIT_DEBUG;
    const throttle = this.config.API.THROTTLE_MS;

    recipes.forEach((recipe, index) => {
      if (index < limit) {
        // Lookup context
        const projectName = projectMap[String(recipe.project_id)] || `[ID: ${recipe.project_id}]`;
        const folderName = folderMap[String(recipe.folder_id)] || `[ID: ${recipe.folder_id}]`;
        // Delegate fetching to the service
        const deps = this.dependencyService.getForRecipe(recipe.id);
        
        deps.forEach(dep => {
          let finalName = dep.name;
          
          if (dep.type === 'RECIPE CALL') {
            const childRecipeName = recipeMap[Striing(dep.id)];
            if (childRecipeName) {
              finalName = childRecipeName;
            } else {
              finalName = `[Unknown Recipe ID: ${dep.id}]`;
            }
          }
          rows.push([
            recipe.id,
            projectName,
            folderName,
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
  /** * Fetches logic ONLY for recipes requested in the 'logic_requests' sheet.
   * @private 
   */
  _processLogic(allRecipes) {
    // 1. Get requested IDs from the Sheet
    const requestedIds = this.sheetService.readRequests();
    
    if (requestedIds.length === 0) {
      Logger.verbose("No specific recipes requested for Logic Dump. Skipping.");
      return [];
    }

    Logger.verbose(`Processing Logic for ${requestedIds.length} requested recipes...`);
    
    const rows = [this.config.HEADERS.LOGIC];
    const throttle = this.config.API.THROTTLE_MS;

    // 2. Iterate only through the requested IDs
    requestedIds.forEach((reqId, index) => {
      // Find the recipe metadata in our full list (optional, for name lookup)
      // We use String() comparison to be safe
      const recipeMeta = allRecipes.find(r => String(r.id) === String(reqId));
      const recipeName = recipeMeta ? recipeMeta.name : "Unknown / Not in Sync";

      try {
         // Fetch the FULL recipe (including code) directly from API
         // We do this individually because 'allRecipes' usually doesn't have the 'code' block
         const fullRecipe = this.dependencyService.client.get(`recipes/${reqId}`);
         
         // Parse
         const logicRows = this.logicService.parseLogic(fullRecipe);
         rows.push(...logicRows);
         
         Logger.verbose(`Parsed logic for: ${recipeName} (${reqId})`);

      } catch (e) {
         console.warn(`Could not fetch logic for ID ${reqId}: ${e.message}`);
         rows.push([reqId, recipeName, "-", "-", "ERROR", e.message, "-"]);
      }
      
      // Throttle to be kind to the API
      if (index % 5 === 0) Utilities.sleep(throttle);
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
function syncInventory() {
  const app = new WorkatoSyncApp();
  app.runInventorySync();
}
/**
 * Recipe logic debugger
 * Run this to update the 'recipe_logic' tab based on IDs in 'logic_requests'.
 */
function fetchRecipeLogic() {
  const app = new WorkatoSyncApp();
  app.runLogicDebug();
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
