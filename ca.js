/**
 * @file Workato Inventory Sync
 * @description Fetches all resources from Workato and logs them to a dedicated Google Sheet.
 * @author Emily Cabaniss
 * @see DOCS (Workato API - Folders): "https://docs.workato.com/en/workato-api/folders.html"
 * @see DOCS (Workato API - Recipes): "https://docs.workato.com/en/workato-api/recipes.html"
 * @see DOCS (Workato API - Project): "https://docs.workato.com/en/workato-api/project-properties.html"
 * 
 */

/**
 * @typedef {Object} AppConfig
 * @property {Object} API - API connection settings.
 * @property {string} API.TOKEN - The Workato API bearer token.
 * @property {string} API.BASE_URL - The Workato API endpoint.
 * @property {number} API.PER_PAGE - Records per request.
 * @property {number} API.MAX_CALLS - Safety limit for API calls.
 * @property {Object} SHEETS - Mapping of resource types to sheet names.
 * @property {boolean} VERBOSE - Toggle for detailed logging.
 */

/** @type {AppConfig} */
const CONFIG = {
  API: {
    TOKEN: PropertiesService.getScriptProperties().getProperty('WORKATO_TOKEN'),
    BASE_URL: 'https://app.eu.workato.com/api',
    PER_PAGE: 100,
    MAX_CALLS: 500,
  },
  SHEETS: {
    RECIPES: 'recipes',
    FOLDERS: 'folders',
    PROJECTS: 'projects',
    PROPERTIES: 'properties'
  },
  VERBOSE: true
};
Object.freeze(CONFIG);

// -------------------------------------------------------------------------------------------------------
// ENTRYPOINT
// -------------------------------------------------------------------------------------------------------
/**
 * Primary entry point for the script. Coordinates the fetching of projects,
 * folders, recipes, and properties, then orchestrates the sheet writing process.
 * @returns {void}
 */
function syncWorkatoWorkspace() {
  try {
    logVerbose("Starting full workspace sync...");

    // 1. Identify present workspace
    const currentUser = fetchCurrentUser();
    if (currentUser) {
      const workspaceName = currentUser.current_account_name || "Unknown workspace";
      const userName = currentUser.name || "Unknown user";
      console.log(`Authenticated as ${userName}`);
      console.log(`Connected to workspace: ${workspaceName}`);
    }
    
    // 2. Fetch all raw data
    const projects = fetchResource('projects');
    const folders = fetchAllFoldersRecursively(projects); 
    const recipes = fetchResource('recipes');
    
    let properties = [];
    try {
      properties = fetchResource('properties');
    } catch (propError) {
      console.warn(`SKIPPING PROPERTIES: The API rejected the request (${propError.message}).`);
      properties = [];
    }

    logVerbose(`Fetched totals: ${projects.length} projects, ${folders.length} folders, ${recipes.length} recipes, ${properties.length} properties`);

    // 3. Create lookup maps
    const projectMap = Object.fromEntries(projects.map(p => [p.id, p.name]));
    const folderMap = Object.fromEntries(folders.map(f => [f.id, f.name]));

    // 4. Prep data for sheets
    const projectRows = [["ID", "Name", "Description", "Created at"]].concat(
      projects.map(p => [p.id, p.name, p.description, p.created_at])
    );

    const folderRows = [["ID", "Name", "Parent folder", "Project name"]].concat(
      folders.map(f => {
        let parentName = "TOP LEVEL";
        if (f.is_project) {
          parentName = "Workspace Root (Home)";
        } else if (f.parent_id) {
          parentName = folderMap[f.parent_id] || `[ID: ${f.parent_id}] (not found)`;
        }
        const projectName = projectMap[f.project_id] || `[ID: ${f.project_id}]`;
        return [f.id, f.name, parentName, projectName];
      })
    );

    const recipeRows = [["ID", "Name", "Status", "Project", "Folder", "Last Run"]].concat(
      recipes.map(r => [
        r.id,
        r.name,
        r.running ? "ACTIVE" : "STOPPED",
        projectMap[r.project_id] || r.project_id,
        folderMap[r.folder_id] || `[Unknown/Deleted: ${r.folder_id}]`,
        r.last_run_at || "NEVER"
      ])
    );

    const propertyRows = [["ID", "Name", "Value", "Created at", "Updated at"]].concat(
      properties.map(p => [p.id, p.name, p.value, p.created_at, p.updated_at])
    );

    // 5. Write to sheets
    logVerbose("Writing to Sheets...");
    writeToSheet(CONFIG.SHEETS.PROJECTS, projectRows);
    writeToSheet(CONFIG.SHEETS.FOLDERS, folderRows);
    writeToSheet(CONFIG.SHEETS.RECIPES, recipeRows);
    writeToSheet(CONFIG.SHEETS.PROPERTIES, propertyRows); 

    notifyUser("Sync complete. Workspace inventory updated...", false);

  } catch (e) {
    let errorMsg = `Sync failed: ${e.message}`;
    if (e.message.includes("Unexpected token")) {
      errorMsg = "Authentication error: Check your WORKATO_TOKEN, and BASE_URL";
    }
    notifyUser(errorMsg, true);
    console.error(e.stack);
  }
}

// -------------------------------------------------------------------------------------------------------
// UTILITIES
// -------------------------------------------------------------------------------------------------------
/**
 * Generic fetcher for Workato Resources. Handles pagination logic and 
 * normalizes differences between root arrays and paginated objects.
 * @param {string} resourcePath - The API endpoint path (e.g., 'recipes', 'projects').
 * @returns {Array<Object>} An array of resource objects retrieved from the API.
 * @throws {Error} If the API returns a non-200 status code.
 */
function fetchResource(resourcePath) {
  let results = [];
  let page = 1;
  let hasMore = true;

  while (hasMore) {
    const cleanPath = resourcePath.startsWith('/') ? resourcePath : `/${resourcePath}`;
    const url = `${CONFIG.API.BASE_URL}${cleanPath}?page=${page}&per_page=${CONFIG.API.PER_PAGE}`;

    const options = {
      method: 'get',
      headers: {
        'Authorization': `Bearer ${CONFIG.API.TOKEN}`,
        'Content-Type': 'application/json'
      },
      muteHttpExceptions: true
    };

    const response = UrlFetchApp.fetch(url, options);

    if (response.getResponseCode() !== 200) {
      throw new Error(`API Error [${resourcePath}]: ${response.getResponseCode()} - ${response.getContentText()}`);
    }

    const json = JSON.parse(response.getContentText());
    let records = Array.isArray(json) ? json : (json.items || json.result || []);

    if (records.length > 0) {
      results = results.concat(records);
      if (records.length < CONFIG.API.PER_PAGE) {
        hasMore = false;
      } else {
        page++;
      }
    } else {
      hasMore = false;
    }

    if (page % 5 === 0) logVerbose(`...fetched ${page} pages of ${resourcePath}`);
    if (page > 200) break;
  }

  console.log(`Fetched ${results.length} records for ${resourcePath}`);
  return results;
}
/**
 * Recursively fetches all folders by bridging Project IDs to Folder IDs 
 * and scanning the Workspace Root.
 * @param {Array<Object>} availableProjects - List of project objects to scan for roots.
 * @returns {Array<Object>} Comprehensive list of all folder objects.
 */
function fetchAllFoldersRecursively(availableProjects) {
  let allFolders = [];
  let queue = []; 
  let processedIds = new Set();

  console.log(`Starting Hybrid Sync for ${availableProjects.length} Projects + Workspace Root...`);

  // PHASE 1: Project Roots
  for (const project of availableProjects) {
    const url = `${CONFIG.API.BASE_URL}/folders?project_id=${project.id}`;
    const potentialRoots = fetchFolderBatch(url);
    const rootFolder = potentialRoots.find(f => f.project_id === project.id && f.is_project === true);

    if (rootFolder && !processedIds.has(rootFolder.id)) {
      allFolders.push(rootFolder);
      processedIds.add(rootFolder.id);
      queue.push(rootFolder.id); 
    }
  }

  // PHASE 2: Home Folders
  const globalFolders = fetchFolderBatch(`${CONFIG.API.BASE_URL}/folders`);
  globalFolders.forEach(f => {
    if (!processedIds.has(f.id)) {
      allFolders.push(f);
      processedIds.add(f.id);
      queue.push(f.id);
    }
  });

  // PHASE 3: Recursion
  let safetyCounter = 0;
  while (queue.length > 0 && safetyCounter < CONFIG.API.MAX_CALLS) {
    let parentId = queue.shift();
    let page = 1;
    let hasMore = true;

    while (hasMore) {
      let url = `${CONFIG.API.BASE_URL}/folders?parent_id=${parentId}&page=${page}&per_page=${CONFIG.API.PER_PAGE}`;
      const items = fetchFolderBatch(url);

      if (items.length > 0) {
        const newItems = items.filter(f => !processedIds.has(f.id));
        if (newItems.length > 0) {
          allFolders = allFolders.concat(newItems);
          newItems.forEach(f => {
            processedIds.add(f.id);
            queue.push(f.id); 
          });
        }
        if (items.length < CONFIG.API.PER_PAGE) hasMore = false;
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
 * Helper to fetch a single batch of folders from a specific URL.
 * @param {string} url - The constructed API URL for folders.
 * @returns {Array<Object>} Array of folder objects (empty if error).
 */
function fetchFolderBatch(url) {
  try {
    const options = {
      method: 'get',
      headers: {
        'Authorization': `Bearer ${CONFIG.API.TOKEN}`,
        'Content-Type': 'application/json'
      },
      muteHttpExceptions: true
    };

    const response = UrlFetchApp.fetch(url, options);
    if (response.getResponseCode() !== 200) return [];

    const json = JSON.parse(response.getContentText());
    return Array.isArray(json) ? json : (json.items || json.result || []);
  } catch (e) {
    console.error("Fetch Error: " + e.message);
    return [];
  }
}
/**
 * Retrieves details about the authenticated user and the active workspace.
 * @returns {Object|null} User/Account data object, or null if fetch fails.
 */
function fetchCurrentUser() {
  const url = `${CONFIG.API.BASE_URL}/users/me`;
  const options = {
    method: 'get',
    headers: {
      'Authorization': `Bearer ${CONFIG.API.TOKEN}`,
      'Content-Type': 'application/json'
    },
    muteHttpExceptions: true
  };

  const response = UrlFetchApp.fetch(url, options);
  if (response.getResponseCode() !== 200) return null;

  return JSON.parse(response.getContentText());
}
/**
 * Writes a 2D array of data to a Google Sheet. Clears existing data and 
 * applies basic header formatting.
 * * @param {string} sheetName - The name of the target sheet.
 * @param {Array<Array<any>>} data - The rows and columns to write.
 * @returns {void}
 */
function writeToSheet(sheetName, data) {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  let sheet = ss.getSheetByName(sheetName) || ss.insertSheet(sheetName);

  sheet.clear();

  if (data.length > 0) {
    sheet.getRange(1, 1, data.length, data[0].length).setValues(data);
    sheet.getRange(1, 1, 1, data[0].length).setFontWeight("bold").setBackground("#efefef");
    sheet.setFrozenRows(1);
  }
}
/**
 * Displays a toast notification in the Google Sheet UI and logs to the console.
 * @param {string} message - The message to display.
 * @param {boolean} isError - True if the message should be treated as an error.
 * @returns {void}
 */
function notifyUser(message, isError) {
  if (isError) console.error(message);
  else console.log(message);

  try {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    if (ss) ss.toast(message, isError ? "Error" : "Success", 5);
  } catch (e) {
    console.log("UI notification skipped.");
  }
}
/**
 * Logs a message to the console if CONFIG.VERBOSE is enabled.
 * @param {string} msg - The message to log.
 * @returns {void}
 */
function logVerbose(msg) {
  if (CONFIG.VERBOSE) console.log(`[VERBOSE] ${msg}`);
}
// -------------------------------------------------------------------------------------------------------
// DIAGNOSTIC
// -------------------------------------------------------------------------------------------------------
/** * testWorkatoConnectivity
 * Validates the connection to the Workato API across all primary endpoints.
 * @description Iterates through 'projects', 'folders', 'recipes', and 'properties' to verify:
 * 1. The Auth Token is valid.
 * 2. The Base URL is correct (EU vs US).
 * 3. The API returns a 200 OK status.
 * Useful for "Smoke Testing" the configuration before running a full sync.
 * @returns {void} Logs success/error snapshots to the Execution Transcript.
 */
function testWorkatoConnectivity() {
  const endpoints = ['projects', 'folders', 'recipes', 'properties'];

  console.log("--- TESTING CONNECTIVITY ---");
  console.log(`Target: ${CONFIG.API.BASE_URL}`);

  endpoints.forEach(endpoint => {
    try {
      const url = `${CONFIG.API.BASE_URL}/${endpoint}?page=1&per_page=1`;

      const options = {
        method: 'get',
        headers: {
          'Authorization': `Bearer ${CONFIG.API.TOKEN}`,
          'Content-Type': 'application/json'
        },
        muteHttpExceptions: true
      };

      const response = UrlFetchApp.fetch(url, options);
      const code = response.getResponseCode();
      const text = response.getContentText();

      console.log(`\n--- [${endpoint.toUpperCase()}] Status: ${code} ---`);

      if (code === 200) {
        const json = JSON.parse(text);
        console.log(JSON.stringify(json, null, 2));
      } else {
        console.error("ERROR RESPONSE:");
        console.error(text.substring(0, 500));
      }
    } catch (e) {
      console.error(`EXCEPTION calling /${endpoint}: ${e.message}`);
    }
  });

  console.log("\nDIAGNOSTIC COMPLETE")
}
/** * debugProjectFolders
 * @description Deep-dive diagnostic for diagnosing "Missing Sub-folder" issues.
 * It specifically targets projects named "Holiday Request" and "Onboarding" to:
 * 1. Verify the Projects exist.
 * 2. Fetch the Project's root folders via `?project_id=ID`.
 * 3. Analyze the JSON structure (Array vs Object) returned by the API.
 * @note If the API returns a raw Array `[]`, the main script must use `Array.isArray()` checks.
 * @returns {void} Logs the hierarchy structure of the target projects.
 */
function debugProjectFolders() {
  console.log("STARTING DIAGNOSTIC: Project Root Folders");

  // 1. Fetch Projects to get IDs
  // We use a simplified inline fetch here to ensure this test is standalone
  console.log("... Fetching Project List");
  const projectsUrl = `${CONFIG.API.BASE_URL}/projects`;
  const options = {
      method: 'get',
      headers: { 
        'Authorization': `Bearer ${CONFIG.API.TOKEN}`,
        'Content-Type': 'application/json' 
      },
      muteHttpExceptions: true
  };
  
  const projRes = UrlFetchApp.fetch(projectsUrl, options);
  const allProjects = JSON.parse(projRes.getContentText());
  console.log(`Found ${allProjects.length} total projects.`);

  // 2. Filter for the specific projects you mentioned
  // (We use toLowerCase() to be safe with casing)
  const targets = ["Holiday Request", "Onboarding"];
  
  const targetProjects = allProjects.filter(p => 
    targets.some(t => p.name.toLowerCase().includes(t.toLowerCase()))
  );

  if (targetProjects.length === 0) {
    console.error("Could not find projects named 'Holiday Request' or 'Onboarding'. Check exact spelling.");
    return;
  }

  // 3. Test the Folder Endpoint for these specific projects
  targetProjects.forEach(p => {
    console.log(`\n---------------------------------------------------`);
    console.log(`TESTING PROJECT: "${p.name}" (ID: ${p.id})`);
    
    // Construct the URL specifically for this project's root
    const folderUrl = `${CONFIG.API.BASE_URL}/folders?project_id=${p.id}`;
    console.log(`   URL: ${folderUrl}`);

    const folderRes = UrlFetchApp.fetch(folderUrl, options);
    const text = folderRes.getContentText();
    const status = folderRes.getResponseCode();

    console.log(`   HTTP Status: ${status}`);

    if (status === 200) {
      const json = JSON.parse(text);
      
      // Determine if it's an Array or Object (The core issue we suspect)
      let items = [];
      let dataType = "Unknown";
      
      if (Array.isArray(json)) {
        items = json;
        dataType = "Raw Array []";
      } else if (json.items) {
        items = json.items;
        dataType = "Object with .items {}";
      } else if (json.result) {
        items = json.result;
        dataType = "Object with .result {}";
      }

      console.log(`   Data Structure: ${dataType}`);
      console.log(`   Folder Count: ${items.length}`);

      // Log the actual names found to verify "API Recipes" or "Testing" appear
      if (items.length > 0) {
        console.log(`   Found Folders:`);
        items.forEach(f => console.log(`     - [ID: ${f.id}] "${f.name}" (Parent: ${f.parent_id})`));
      } else {
        console.warn(`Returned 0 folders. Check if folders exist at the TOP LEVEL of this project.`);
      }
      
      // Dump raw snippet for deeper inspection
      console.log(`   \n   [RAW SNAPSHOT]: ${text.substring(0, 1000)}...`);

    } else {
      console.error(`API ERROR: ${text}`);
    }
  });
  
  console.log("\nDIAGNOSTIC COMPLETE");
}
/** * inspectSpecificFolder
 * * @description Probes a specific ID against the Workato `/folders/{id}` endpoint.
 * This is used to determine if a "Mystery ID" (like a parent_id found in logs)
 * is actually a valid folder, or if it returns "API Not Found".
 * @const {number} targetId - The ID to inspect (Hardcoded for manual testing).
 * @returns {void} Logs the metadata of the ID if found, or the specific error message if not.
 */
function inspectSpecificFolder() {
  const targetId = 616442; // The ID you want to verify
  
  console.log(`INSPECTING ID: ${targetId}...`);
  
  const url = `${CONFIG.API.BASE_URL}/folders/${targetId}`;
  const options = {
      method: 'get',
      headers: { 
        'Authorization': `Bearer ${CONFIG.API.TOKEN}`,
        'Content-Type': 'application/json' 
      },
      muteHttpExceptions: true
  };

  const response = UrlFetchApp.fetch(url, options);
  
  if (response.getResponseCode() === 200) {
    const folder = JSON.parse(response.getContentText());
    
    console.log(`\nRESULTS:`);
    console.log(`   Name:      "${folder.name}"`); // Expect "Home"
    console.log(`   Parent ID: ${folder.parent_id}`); // Expect null
    console.log(`   Is Project:${folder.is_project || false}`);
    
    if (folder.parent_id === null) {
      console.log(`\nCONFIRMED: This is a Root Level folder.`);
    }
  } else {
    console.error(`Error: ${response.getContentText()}`);
  }
}
/** * confirmWorkspaceId
 * * @description Fetches the authenticated User and Account details. 
 * Can optionally compare a specific ID against these details to identify its origin.
 * @param {number|string} [targetId] - (Optional) An ID to check (e.g., a "mystery" parent_id found in logs).
 * @returns {Object} An object containing the userId and accountId for further use.
 */
function confirmWorkspaceID(targetId = null) {
  console.log("Checking Current User & Workspace Context...");
  
  const user = fetchCurrentUser(); // Uses the main helper function
  
  if (!user) {
    console.error("Could not fetch user details. Check Auth Token.");
    return null;
  }

  // Normalize IDs to strings for safe comparison
  const userId = String(user.id);
  const accountId = String(user.current_account_id);
  const targetStr = targetId ? String(targetId) : null;

  console.log("-----------------------------------------");
  console.log(`User name:   ${user.name}`);
  console.log(`User email:  ${user.email}`);
  console.log(`User ID:     ${userId}`); 
  console.log(`Account ID:  ${accountId || "N/A (Not provided by API)"}`);
  console.log(`Workspace:   ${user.current_account_name}`);
  console.log("-----------------------------------------");

  // If a target ID was passed, check for matches
  if (targetStr) {
    console.log(`Comparing target ID [${targetStr}]...`);
    
    if (userId === targetStr) {
      console.log(`MATCH FOUND: [${targetStr}] is your USER ID.`);
    } else if (accountId === targetStr) {
      console.log(`MATCH FOUND: [${targetStr}] is your ACCOUNT (Workspace) ID.`);
      console.log("   (This ID acts as the 'Root' parent for top-level projects).");
    } else {
      console.log(`NO MATCH: [${targetStr}] is neither your User ID nor Account ID.`);
    }
  }

  return {
    userId: user.id,
    accountId: user.current_account_id,
    workspaceName: user.current_account_name
  };
}
