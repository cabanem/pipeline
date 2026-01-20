/**
 * @file 00_Core_Context.gs
 * @description AppContext and commands bootstrap.
 */

/**
 * @class
 * @classdesc Central dependency container for a single run invocation.
 * Keeps construction in a central place, so that features don't new() everything.
 */
class AppContext {
  constructor() {
    this.config = AppConfig.get();

    // Core runtime services
    this.client = new WorkatoClient();
    this.inventoryService = new InventoryService(this.client);
    this.analyzerService = new RecipeAnalyzerService(this.client);
    this.sheetService = new SheetService();
    this.driveService = new DriveService();

    // Logger is static, but handle created for consistency.
    this.logger = Logger;
  }
}

/**
 * @class
 * @classdesc Factory helpers for consistent construction.
 */
class AppFactory {
  static createContext() {
    return new AppContext();
  }

  static createApp(ctx = null) {
    return new WorkatoSyncApp(ctx || this.createContext());
  }
}

/**
 * @class
 * @classdesc Command registry and runners.
 * Keeps "what can this app do?" in a sinlge place.
 */

class Commands {
  static _registry_() {
    if (!this.__registry) this.__registry = {};
    return this.__registry;
  }

  static register(name, handlerFn) {
    const reg = this._registry_();
    if (reg[name]) throw new Error(`Command already registered: ${name}`);
    reg[name] = handlerFn;
  }

  static ensureInitialized_() {
    if (this.__init) return;
    this.__init = true;

    // ---- Inventory -------------------------------------------------------
    this.register("inventory.sync", (ctx, args) => {
      return new InventorySyncRunner().run(ctx);
    });

    // ---- Logic debug -----------------------------------------------------
    this.register("logic.debug", (ctx, args) => {
      const ids = Array.isArray(args?.ids) ? args.ids : null;
      return new LogicDebugRunner().run(ctx, ids);
    });

    // ---- AI analysis -----------------------------------------------------
    this.register("ai.analyze", (ctx, args) => {
      const ids = Array.isArray(args?.ids) ? args.ids : null;
      return new AiAnalysisRunner().run(ctx, ids);
    });

    // ---- Process maps ----------------------------------------------------
    this.register("process.maps", (ctx, args) => {
      const ids = Array.isArray(args?.ids) ? args.ids : null;
      const options = args?.options && typeof args.options === "object" ? args.options : {};
      return new ProcessMapsRunner().run(ctx, options, ids);
    });

    // ---- Connectivity ----------------------------------------------------
    this.register("connectivity.test", (_ctx, _args) => {
      return testWorkatoConnectivity();
    });
  }

  /**
   * Run a named command with an optional args object.
   * @param {string} name
   * @param {object} [args]
   * @param {AppContext} [ctx]
   */
  static run(name, args = {}, ctx = null) {
    this.ensureInitialized_();
    const reg = this._registry_();
    const fn = reg[name];
    if (!fn) throw new Error(`Unknown command: ${name}`);
    const context = ctx || AppFactory.createContext();
    return fn(context, args);
  }
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
 * @file 09_Core_Helpers.gs
 * @description Shared helpers used by feature runners.
 */

class AppHelpers {
  /** @returns {Object.<string,string>} id->name */
  static createLookupMap(items) {
    const arr = Array.isArray(items) ? items : [];
    return Object.fromEntries(arr.map(i => [String(i.id), i.name]));
  }

  /**
   * parseLogicRows rows: [recipeId, recipeName, step#, indent, provider, actionName, description, details]
   * @returns {string}
   */
  static logicDigestFromRows(logicRows, maxLines) {
    const lines = [];
    const slice = Array.isArray(logicRows) ? logicRows.slice(0, maxLines) : [];
    slice.forEach(r => {
      const stepNo = r[2];
      const indent = r[3] || "";
      const provider = r[4] || "";
      const action = r[5] || "";
      const desc = r[6] ? ` — ${String(r[6]).slice(0, 120)}` : "";
      lines.push(`${stepNo}. ${indent}${action} (${provider})${desc}`);
    });
    if (Array.isArray(logicRows) && logicRows.length > maxLines) {
      lines.push(`… (${logicRows.length - maxLines} more steps omitted)`);
    }
    return lines.join("\n");
  }

  static handleError(e) {
    let errorMsg = `Sync failed: ${e.message}`;
    if (String(e.message || "").includes("Unexpected token")) {
      errorMsg = "Auth Error: Check WORKATO_TOKEN and BASE_URL";
    }
    Logger.notify(errorMsg, true);
    console.error(e && e.stack ? e.stack : e);
  }
}
