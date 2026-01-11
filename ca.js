/**
 * @file Workato Inventory Sync (OOP Version)
 * @description Fetches all resources from Workato (Projects, Recipes, Folders, Properties) 
 * and logs them to Google Sheets using a modular class-based architecture.
 * @author Emily Cabaniss
 */

// -------------------------------------------------------------------------------------------------------
// CONFIGURATION
// -------------------------------------------------------------------------------------------------------
/** * AppConfig
 * Central configuration handler for the application.
 * Manages API secrets, environment settings, and sheet mapping.
 * Implements validation logic to fail fast if configuration is missing.
 */
class AppConfig {
  /**
   * Initializes configuration by loading script properties and defining constants.
   * automatically triggers validation.
   */
  constructor() {
    /** @private @type {string} The Workato API Bearer Token */
    this._token = PropertiesService.getScriptProperties().getProperty('WORKATO_TOKEN');
    
    /** @type {Object} API Connection settings */
    this.API = {
      BASE_URL: 'https://app.eu.workato.com/api', // Adjust to .us. if needed
      PER_PAGE: 100,
      MAX_CALLS: 500
    };

    /** @type {Object} Mapping of internal resource names to Google Sheet tab names */
    this.SHEETS = {
      RECIPES: 'recipes',
      FOLDERS: 'folders',
      PROJECTS: 'projects',
      PROPERTIES: 'properties',
      DEPENDENCIES: 'recipe_dependencies'
    };

    /** @type {boolean} specific flag for verbose logging */
    this.VERBOSE = true;
    
    this._validate();
  }
  /**
   * Safe accessor for the API Token.
   * @returns {string} The raw API token.
   */
  get token() { return this._token; }

  /**
   * internal validation to ensure critical secrets exist.
   * @throws {Error} If WORKATO_TOKEN is missing from Script Properties.
   * @private
   */
  _validate() {
    if (!this._token) {
      throw new Error("MISSING 'WORKATO_TOKEN' in Script Properties.");
    }
  }
}
/** * Global Configuration Instance.
 * @constant 
 * @type {AppConfig} 
 */
const CONFIG = new AppConfig();

// -------------------------------------------------------------------------------------------------------
// API CLIENT
// -------------------------------------------------------------------------------------------------------
/**
 * Low-level HTTP Client wrapper for Workato API interactions.
 * Centralizes authentication headers, error handling, and JSON parsing.
 */
class WorkatoClient {
  /**
   * @param {string} token - The Bearer token for authentication.
   * @param {string} baseUrl - The root URL for the Workato API.
   */
  constructor(token, baseUrl) {
    this.token = token;
    this.baseUrl = baseUrl;
  }
  /**
   * Executes an HTTP request against the Workato API.
   * @param {string} endpoint - The API path (e.g., '/recipes') or full URL.
   * @param {Object} [options={}] - Additional UrlFetchApp options (method, payload, etc).
   * @returns {Object|Array} The parsed JSON response.
   * @throws {Error} If the API returns a non-200 status code.
   */
  request(endpoint, options = {}) {
    const url = endpoint.startsWith('http') ? endpoint : `${this.baseUrl}${endpoint}`;
    const params = {
      method: options.method || 'get',
      headers: {
        'Authorization': `Bearer ${this.token}`,
        'Content-Type': 'application/json'
      },
      muteHttpExceptions: true,
      ...options
    };

    const response = UrlFetchApp.fetch(url, params);
    const code = response.getResponseCode();
    if (code !== 200) throw new Error(`API Error [${code}]: ${response.getContentText()}`);
    return JSON.parse(response.getContentText());
  }
}

// -------------------------------------------------------------------------------------------------------
// DATA SERVICE
// -------------------------------------------------------------------------------------------------------
/**
 * Business Logic Layer.
 * Handles specific data retrieval patterns like pagination and recursion for Workato resources.
 */
class WorkatoService {
  /**
   * @param {WorkatoClient} client - An initialized API client.
   * @param {AppConfig} config - The application configuration.
   */
  constructor(client, config) {
    this.client = client;
    this.config = config;
  }
  /**
   * Fetches the current authenticated user context.
   * @returns {Object} User profile object including current account name.
   */
  fetchUserContext() {
    return this.client.request('/users/me');
  }
  /**
   * Generic paginated fetcher for standard Workato lists.
   * Automatically iterates through pages until all records are retrieved.
   * @param {string} resourcePath - The API endpoint (e.g., 'recipes').
   * @returns {Array<Object>} Comprehensive list of all records.
   */
  fetchResource(resourcePath) {
    let results = [];
    let page = 1;
    let hasMore = true;

    while (hasMore) {
      const cleanPath = resourcePath.startsWith('/') ? resourcePath : `/${resourcePath}`;
      // Adds '?' or '&' depending on if query params already exist
      const separator = cleanPath.includes('?') ? '&' : '?';
      const url = `${cleanPath}${separator}page=${page}&per_page=${this.config.API.PER_PAGE}`;
      
      const json = this.client.request(url);
      // Handles inconsistencies in API returns (Array vs Object.items vs Object.result)
      const records = Array.isArray(json) ? json : (json.items || json.result || []);

      results = results.concat(records);
      
      // Stop if we got fewer records than requested (end of list) or hit safety limit
      hasMore = records.length === this.config.API.PER_PAGE && page < 200;
      page++;
    }
    return results;
  }
  /**
   * Orchestrates the complex logic of finding folders nested inside Projects
   * and the global workspace root.
   * @param {Array<Object>} projects - List of project objects to scan.
   * @returns {Array<Object>} A flat array of all folder objects found.
   */
  getFoldersRecursive(projects) {
    let allFolders = [];
    let queue = [];
    let processedIds = new Set();

    // Phase 1: Scan Project Roots
    projects.forEach(project => {
      try {
        const roots = this.client.request(`/folders?project_id=${project.id}`);
        const items = Array.isArray(roots) ? roots : (roots.items || []);
        // Find the "Root" folder that represents the project container
        const root = items.find(f => f.project_id === project.id && f.is_project === true);
        
        if (root && !processedIds.has(root.id)) {
          allFolders.push(root);
          processedIds.add(root.id);
          queue.push(root.id);
        }
      } catch (e) { 
        console.warn(`Skipping folders for project ${project.id}`); 
      }
    });

    // Phase 2: Scan Global/Home Folders (Non-Project)
    try {
      const homeFolders = this.client.request('/folders');
      const items = Array.isArray(homeFolders) ? homeFolders : (homeFolders.items || []);
      items.forEach(f => {
        if (!processedIds.has(f.id)) {
          allFolders.push(f);
          processedIds.add(f.id);
          queue.push(f.id);
        }
      });
    } catch (e) { 
      console.warn("Could not fetch home folders."); 
    }

    // Phase 3: Recursive Drill-down
    this._processFolderQueue(queue, allFolders, processedIds);
    return allFolders;
  }
  /**
   * Helper method to process the recursion queue (Breadth-First Search).
   * @param {Array<number>} queue - List of folder IDs to inspect.
   * @param {Array<Object>} allFolders - Accumulator for results.
   * @param {Set<number>} processedIds - Tracker to avoid infinite loops.
   * @private
   */
  _processFolderQueue(queue, allFolders, processedIds) {
    let safetyCounter = 0;
    while (queue.length > 0 && safetyCounter < this.config.API.MAX_CALLS) {
      let parentId = queue.shift();
      try {
        const itemsRaw = this.client.request(`/folders?parent_id=${parentId}`);
        const items = Array.isArray(itemsRaw) ? itemsRaw : (itemsRaw.items || []);
        
        const newItems = items.filter(f => !processedIds.has(f.id));
        allFolders.push(...newItems);
        
        // Add children to queue
        newItems.forEach(f => {
          processedIds.add(f.id);
          queue.push(f.id);
        });
      } catch (e) { /* silent fail for specific folder error */ }
      safetyCounter++;
    }
  }
  /**
   * Fetches specific dependencies for a single recipe.
   * @param {number} recipeId 
   * @returns {Array<Object>} List of dependencies.
   */
  getDependencies(recipeId) {
    try {
      const res = this.client.request(`/recipes/${recipeId}/dependencies`);
      return res.dependencies || [];
    } catch (e) { return []; }
  }
}

// -------------------------------------------------------------------------------------------------------
// UI/SHEET SERVICE
// -------------------------------------------------------------------------------------------------------
/**
 * Abstraction layer for Google Spreadsheet interactions.
 * Decouples sheet formatting and writing from business logic.
 */
class SheetService {
  constructor() {
    this.ss = SpreadsheetApp.getActiveSpreadsheet();
  }
  /**
   * Writes a 2D array to a named sheet, handling creation, clearing, and formatting.
   * @param {string} sheetName - The destination tab name.
   * @param {Array<Array<string|number>>} rows - The data to write.
   */
  write(sheetName, rows) {
    let sheet = this.ss.getSheetByName(sheetName) || this.ss.insertSheet(sheetName);
    sheet.clear();
    if (rows.length > 0) {
      sheet.getRange(1, 1, rows.length, rows[0].length).setValues(rows);
      // Standardize Header Style
      sheet.getRange(1, 1, 1, rows[0].length).setFontWeight("bold").setBackground("#efefef");
      sheet.setFrozenRows(1);
    }
  }
  /**
   * Displays a toast notification in the UI and logs to the console.
   * @param {string} message - The text to display.
   * @param {boolean} [isError=false] - If true, logs as console.error.
   */
  notify(message, isError = false) {
    console[isError ? 'error' : 'log'](message);
    if (this.ss) this.ss.toast(message, isError ? "Error" : "Success", 5);
  }
}

// -------------------------------------------------------------------------------------------------------
// ORCHESTRATOR
// -------------------------------------------------------------------------------------------------------
/**
 * Coordinator class.
 * Connects the Config, Client, Service, and SheetService to execute the sync workflow.
 */
class WorkatoSyncManager {
  /**
   * @param {AppConfig} config - The global configuration.
   */
  constructor(config) {
    this.config = config;
    this.client = new WorkatoClient(config.token, config.API.BASE_URL);
    this.service = new WorkatoService(this.client, config);
    this.sheets = new SheetService();
  }
  /**
   * Main Execution Method.
   * 1. Fetches User Context
   * 2. Fetches Projects, Recipes, Folders, Properties
   * 3. Maps Data to 2D Arrays
   * 4. Writes to Sheets
   */
  runSync() {
    try {
      this.sheets.notify("Starting Sync...");
      
      const user = this.service.fetchUserContext();
      this.sheets.notify(`Authenticated: ${user.name} @ ${user.current_account_name}`);

      // 1. Fetch High Level Resources
      const projects = this.service.fetchResource('projects');
      const folders = this.service.getFoldersRecursive(projects);
      const recipes = this.service.fetchResource('recipes');
      
      let properties = [];
      try { 
        properties = this.service.fetchResource('properties'); 
      } catch(e) { 
        console.warn("Properties skipped/error."); 
      }

      // 2. Create Maps for Name Lookup
      const projectMap = Object.fromEntries(projects.map(p => [p.id, p.name]));
      const folderMap = Object.fromEntries(folders.map(f => [f.id, f.name]));

      // 3. Fetch Dependencies (With Restored Rate Limiting)
      const depRows = [["Parent Recipe ID", "Dependency Type", "Dependency ID", "Dependency Name"]];
      
      // Limit to first 100 recipes to avoid API timeout during testing
      const subset = recipes.slice(0, 100); 
      for (let i = 0; i < subset.length; i++) {
        const r = subset[i];
        const deps = this.service.getDependencies(r.id);
        
        deps.forEach(d => depRows.push([r.id, d.type, d.id, d.name]));

        // Throttling: Sleep 100ms every 5 calls to avoid 429 errors
        if (i % 5 === 0) Utilities.sleep(100);
      }

      // 4. Write All Data
      this.sheets.notify("Writing to Sheets...");
      this.sheets.write(this.config.SHEETS.PROJECTS, this._mapProjects(projects));
      this.sheets.write(this.config.SHEETS.FOLDERS, this._mapFolders(folders, projectMap, folderMap));
      this.sheets.write(this.config.SHEETS.RECIPES, this._mapRecipes(recipes, projectMap, folderMap));
      this.sheets.write(this.config.SHEETS.PROPERTIES, this._mapProperties(properties));
      this.sheets.write(this.config.SHEETS.DEPENDENCIES, depRows);

      this.sheets.notify("Sync Complete.");

    } catch (e) {
      this.sheets.notify(`Sync Failed: ${e.message}`, true);
      console.error(e.stack); 
    }
  }
  /** @private Maps project objects to array rows */
  _mapProjects(data) {
    return [["ID", "Name", "Description", "Created at"]].concat(
      data.map(p => [p.id, p.name, p.description, p.created_at])
    );
  }
  /** @private Maps folder objects to array rows, resolving parent names */
  _mapFolders(data, pMap, fMap) {
    return [["ID", "Name", "Parent folder", "Project name"]].concat(
      data.map(f => {
        let parentName = "TOP LEVEL";
        if (f.is_project) {
          parentName = "Workspace Root (Home)";
        } else if (f.parent_id) {
          parentName = fMap[f.parent_id] || `[ID: ${f.parent_id}] (not found)`;
        }
        const projectName = pMap[f.project_id] || `[ID: ${f.project_id}]`;
        return [f.id, f.name, parentName, projectName];
      })
    );
  }
  /** @private Maps recipe objects to array rows */
  _mapRecipes(data, pMap, fMap) {
    return [["ID", "Name", "Status", "Project", "Folder", "Last Run"]].concat(
      data.map(r => [
        r.id, 
        r.name, 
        r.running ? "ACTIVE" : "STOPPED",
        pMap[r.project_id] || r.project_id,
        fMap[r.folder_id] || `[Unknown/Deleted: ${r.folder_id}]`,
        r.last_run_at || "NEVER"
      ])
    );
  }
  /** @private Maps property objects to array rows */
  _mapProperties(data) {
    return [["ID", "Name", "Value", "Created at", "Updated at"]].concat(
      data.map(p => [p.id, p.name, p.value, p.created_at, p.updated_at])
    );
  }
}

// -------------------------------------------------------------------------------------------------------
// GLOBAL ENTRY POINTS
// -------------------------------------------------------------------------------------------------------
/**
 * Primary UI entry point. 
 * Assign this function to a button or custom menu in the Spreadsheet.
 */
function syncWorkatoWorkspace() {
  new WorkatoSyncManager(CONFIG).runSync();
}
/**
 * Diagnostic helper.
 * Run manually from the Apps Script editor to verify API credentials.
 */
function testConnectivity() {
  try {
    const client = new WorkatoClient(CONFIG.token, CONFIG.API.BASE_URL);
    const user = client.request('/users/me');
    Logger.log(`Connected to: ${user.current_account_name}`);
  } catch (e) {
    Logger.log(`Connection failed: ${e.message}`);
  }
}
