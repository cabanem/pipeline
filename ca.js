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
      return AppFactory.createApp(ctx).runInventorySync();
    });

    // ---- Logic debug -----------------------------------------------------
    this.register("logic.debug", (ctx, args) => {
      const ids = Array.isArray(args?.ids) ? args.ids : null;
      return AppFactory.createApp(ctx).runLogicDebug(ids);
    });

    // ---- AI analysis -----------------------------------------------------
    this.register("ai.analyze", (ctx, args) => {
      const ids = Array.isArray(args?.ids) ? args.ids : null;
      return AppFactory.createApp(ctx).runAiAnalysis(ids);
    });

    // ---- Process maps ----------------------------------------------------
    this.register("process.maps", (ctx, args) => {
      const ids = Array.isArray(args?.ids) ? args.ids : null;
      const options = args?.options && typeof args.options === "object" ? args.options : {};
      return AppFactory.createApp(ctx).runProcessMaps(options, ids);
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
