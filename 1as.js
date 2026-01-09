/**
 * @file Workato Inventory Sync
 * @description Fetches all resources from Workato and logs them to a dedicated Google Sheet.
 * @see DOCS (Workato API - Folders): "https://docs.workato.com/en/workato-api/folders.html"
 * @see DOCS (Workato API - Recipes): "https://docs.workato.com/en/workato-api/recipes.html"
 * @see DOCS (Workato API - Project): "https://docs.workato.com/en/workato-api/project-properties.html"
 * 
 * @author Emily Cabaniss
 */

// -------------------------------------------------------------------------------------------------------
// CONFIGURATION
// -------------------------------------------------------------------------------------------------------
const CONFIG = {
  API: {
    TOKEN: PropertiesService.getScriptProperties().getProperty('WORKATO_TOKEN'),
    BASE_URL: 'https://app.eu.workato.com/api',
    PER_PAGE: 100
  },
  SHEETS: {
    RECIPES: 'recipes',
    FOLDERS: 'folders',
    PROJECTS: 'projects'
  },
  // Set to true to enable logging
  VERBOSE: true
};
Object.freeze(CONFIG);

// -------------------------------------------------------------------------------------------------------
// PRIMARY ENTRY POINT
// -------------------------------------------------------------------------------------------------------
function syncWorkatoWorkspace() {
  try {
    logVerbose("Starting full workspace sync...");

    // 1. Fetch all raw data
    const projects = fetchResource('projects');
    const folders = fetchResource('folders');
    const recipes = fetchResource('recipes')

    logVerbose(`Fetched totals: ${projects.length} projects, ${folders.length} folders, ${recipes.length} recipes`);

    // 2. Create lookup maps
    const projectMap = Object.fromEntries(projects.map(p => [p.id, p.name]));
    const folderMap = Object.fromEntries(folders.map(f => [f.id, f.name]));

    // 3. Prep data for sheets
    // --- Projects ---
    const projectRows = [["ID", "Name", "Description", "Created at"]].concat(
      projects.map(p => [p.id, p.name, p.description, p.created_at])
    );

    // --- Folders ---
    const folderRows = [["ID", "Name", "Parent folder", "Project name"]].concat(
      folders.map(f => {
        let parentName = "TOP LEVEL";
        if (f.parent_id) {
          if (folderMap[f.parent_id]) {
            parentName = folderMap[f.parent_id];
          } else {
            parentName = `[ID: ${f.parent_id}] (not found)`;
            logVerbose(`WARNING: Folder "${f.name}" has parent_id ${f.parent_id} which was not found in the folder list.`);
          }
        }

        const projectName = projectMap[f.project_id] || `[ID: ${f.project_id}]`;

        return [f.id, f.name, parentName, projectName];
      })
    );

    // --- Recipes ---
    const recipeRows = [["ID", "Name", "Status", "Project", "Folder", "Last Run"]].concat(
      recipes.map(r => [
        r.id,
        r.name,
        r.running ? "ACTIVE" : "STOPPED",
        projectMap[r.project_id] || r.project_id,
        folderMap[r.folder_id] || r.folder_id,
        r.last_run_at || "NEVER"
      ])
    );

    // 4. Write to sheets
    logVerbose("Writing to Sheets...");
    writeToSheet(CONFIG.SHEETS.PROJECTS, projectRows);
    writeToSheet(CONFIG.SHEETS.FOLDERS, folderRows);
    writeToSheet(CONFIG.SHEETS.RECIPES, recipeRows);

    notifyUser("Sync complete. Workspace inventory updated...", false);

  } catch (e) {
    let errorMsg = `Sync failed: ${e.message}`;
    if (e.message.includes("Unexpected token")) {
      errorMsg = "Authentication error: Check your WORKATO_TOKEN, and BASE_URL";
    }
    notifyUser(errorMsg, true);
  }
}
// -------------------------------------------------------------------------------------------------------
// UTILITY FUNCTIONS
// -------------------------------------------------------------------------------------------------------
function fetchResource(resourcePath) {
  let results = [];
  let page = 1;
  let hasMore = true;

  while (hasMore) {
    // Construct URL
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

    // Non-200 response
    if (response.getResponseCode() !== 200) {
      throw new Error(`API Error [${resourcePath}]: ${response.getResponseCode()} - ${response.getContentText()}`);
    }

    // Detect root array vs paginated object
    const json = JSON.parse(response.getContentText());
    let records = [];

    if (Array.isArray(json)) {
      records = json;
    } else {
      records = json.items || json.result || [];
    }

    // Paginate
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

    // Log progress for large datasets
    if (page % 5 === 0) logVerbose(`...fetched ${page} pages of ${resourcePath}`);

    // Safety against huge datasets
    if (page > 200) break;
  }

  console.log(`Fetched ${results.length} records for ${resourcePath}`);
  return results;
}
function writeToSheet(sheetName, data) {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  let sheet = ss.getSheetByName(sheetName);

  if (!sheet) {
    sheet = ss.insertSheet(sheetName);
  }

  sheet.clear();

  if (data.length > 0) {
    sheet.getRange(1, 1, data.length, data[0].length).setValues(data);
    sheet.getRange(1, 1, 1, data[0].length).setFontWeight("bold").setBackground("#efefef");
    sheet.setFrozenRows(1);
    // sheet.autoResizeColumns(1, data[0].length); // disabled due to slowing
  }
}
function notifyUser(message, isError) {
  // 1. Log to stackdriver
  if (isError) {
    console.error(message);
  } else {
    console.log(message)
  }

  // 2. Attempt to display alert (non-blocking
  try {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    if (ss) {
      ss.toast(message, isError ? "Error" : "Success", 5);
    }
  } catch (uiError) {
    console.log("UI notification skipped (headless context, UI unavailable).");
  }
}
function logVerbose(msg) {
  if (CONFIG.VERBOSE) {
    console.log(`[VERBOSE] ${msg}`);
  }
}
