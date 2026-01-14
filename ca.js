/** ****************************************************************************************
 * FILE: TEST_Assert.gs
 * Minimal assertion + test runner utilities for Google Apps Script (V8).
 ******************************************************************************************/

class Assert {
  static _fmt(v) {
    try { return JSON.stringify(v, null, 2); } catch (_) { return String(v); }
  }

  static ok(value, msg) {
    if (!value) throw new Error(msg || `Expected truthy but got: ${Assert._fmt(value)}`);
  }

  static equal(actual, expected, msg) {
    if (actual !== expected) {
      throw new Error(msg || `Expected ${Assert._fmt(expected)} but got ${Assert._fmt(actual)}`);
    }
  }

  static notEqual(actual, expected, msg) {
    if (actual === expected) {
      throw new Error(msg || `Expected value to differ, but both were ${Assert._fmt(actual)}`);
    }
  }

  static contains(haystack, needle, msg) {
    const h = String(haystack ?? "");
    const n = String(needle ?? "");
    if (!h.includes(n)) {
      throw new Error(msg || `Expected string to contain "${n}", got:\n${h}`);
    }
  }

  static deepEqual(actual, expected, msg) {
    const a = Assert._fmt(actual);
    const e = Assert._fmt(expected);
    if (a !== e) {
      throw new Error(msg || `Expected deepEqual.\nExpected:\n${e}\nActual:\n${a}`);
    }
  }

  static throws(fn, msgContains) {
    let threw = false;
    try {
      fn();
    } catch (e) {
      threw = true;
      if (msgContains) Assert.contains(e.message, msgContains);
    }
    if (!threw) throw new Error("Expected function to throw, but it did not.");
  }
}

class TestRunner {
  constructor() {
    /** @type {Array<{name:string, fn:Function}>} */
    this.tests = [];
  }

  add(name, fn) {
    this.tests.push({ name, fn });
    return this;
  }

  run(options = {}) {
    const writeToSheet = Boolean(options.writeToSheet);
    const results = [];
    const startedAt = new Date();

    for (const t of this.tests) {
      const t0 = Date.now();
      try {
        t.fn();
        results.push({
          name: t.name,
          status: "PASS",
          ms: Date.now() - t0,
          error: ""
        });
      } catch (e) {
        results.push({
          name: t.name,
          status: "FAIL",
          ms: Date.now() - t0,
          error: (e && e.stack) ? e.stack : String(e)
        });
      }
    }

    const pass = results.filter(r => r.status === "PASS").length;
    const fail = results.length - pass;
    const duration = Date.now() - startedAt.getTime();

    console.log(`TESTS COMPLETE: ${pass} passed, ${fail} failed in ${duration}ms`);

    results.forEach(r => {
      const prefix = r.status === "PASS" ? "✅" : "❌";
      console.log(`${prefix} ${r.name} (${r.ms}ms)`);
      if (r.status === "FAIL") console.log(r.error);
    });

    if (writeToSheet) {
      try {
        const ss = SpreadsheetApp.getActiveSpreadsheet();
        const sheetName = "test_results";
        let sheet = ss.getSheetByName(sheetName) || ss.insertSheet(sheetName);
        sheet.clear();
        sheet.getRange(1, 1, 1, 5).setValues([["Timestamp", "Test", "Status", "Duration (ms)", "Error"]])
          .setFontWeight("bold")
          .setBackground("#efefef");
        const ts = new Date().toISOString();
        const rows = results.map(r => [ts, r.name, r.status, r.ms, r.error]);
        if (rows.length) sheet.getRange(2, 1, rows.length, 5).setValues(rows);
        sheet.setFrozenRows(1);
      } catch (e) {
        console.log("Could not write test results sheet: " + e.message);
      }
    }

    if (fail > 0) {
      throw new Error(`Test suite failed: ${fail} failing tests.`);
    }
    return results;
  }
}

/** ****************************************************************************************
 * FILE: TEST_Fixtures.gs
 * Deterministic fixtures for recipe code scanning, branching, cycles, and mermaid rendering.
 ******************************************************************************************/

class Fixtures {
  /**
   * A realistic-ish Workato step tree with:
   * - recipe calls via flow_id, recipe_id, callable_recipe_id
   * - if/else + error blocks
   * - data pill strings in conditions
   */
  static recipeCodeBlock_withBranchesAndCalls() {
    const dpLabel = "#{_dp('{\\\"label\\\":\\\"Ticket ID\\\",\\\"path\\\":[\\\"ticket\\\",\\\"id\\\"]}')}";
    return {
      block: [
        { provider: "gmail", name: "Trigger: New email", input: { subject: "Hello" } },
        {
          keyword: "if",
          name: "Is urgent?",
          input: {
            operand: "and",
            conditions: [
              { lhs: dpLabel, operand: "=", rhs: "P1" }
            ]
          },
          block: [
            {
              provider: "workato_recipe_function",
              name: "Call Escalation",
              input: { flow_id: "200" }
            }
          ],
          else_block: [
            {
              provider: "workato_recipe_function",
              name: "Call Triage",
              input: { recipe_id: "300" }
            }
          ],
          error_block: [
            {
              provider: "workato_callable_recipe",
              name: "Fallback callable",
              input: { callable_recipe_id: "400" }
            }
          ]
        },
        {
          provider: "workato_recipe_function",
          name: "Call Billing helper",
          input: { flow_id: "500" }
        }
      ]
    };
  }

  static recipePayload(id, name, project_id, folder_id, codeObj) {
    return {
      id: String(id),
      name,
      project_id: String(project_id || "p1"),
      folder_id: String(folder_id || "f1"),
      code: JSON.stringify(codeObj)
    };
  }

  /**
   * Graph fixtures for transitive expansion and cycle detection:
   * 100 -> 200 -> 300 -> 200 (cycle)
   */
  static graphRecipes_cycle() {
    const r100 = Fixtures.recipePayload(
      "100",
      "Root",
      "p1",
      "f1",
      { block: [{ provider: "workato_recipe_function", name: "Call 200", input: { flow_id: "200" } }] }
    );

    const r200 = Fixtures.recipePayload(
      "200",
      "Child A",
      "p1",
      "f1",
      { block: [{ provider: "workato_recipe_function", name: "Call 300", input: { flow_id: "300" } }] }
    );

    const r300 = Fixtures.recipePayload(
      "300",
      "Child B",
      "p1",
      "f1",
      { block: [{ provider: "workato_recipe_function", name: "Call 200 again", input: { flow_id: "200" } }] }
    );

    return { r100, r200, r300 };
  }
}
/** ****************************************************************************************
 * FILE: TEST_Doubles.gs
 * Test doubles (fakes) so we can run tests without touching real Sheets/Drive/Workato.
 ******************************************************************************************/

class FakeWorkatoClient {
  constructor(recipeById) {
    this._recipes = recipeById || {};
  }

  get(endpoint) {
    const m = String(endpoint).match(/^recipes\/(\d+|[a-zA-Z0-9_-]+)$/);
    if (m) {
      const id = String(m[1]);
      if (!this._recipes[id]) throw new Error(`404 recipe not found: ${id}`);
      return this._recipes[id];
    }
    throw new Error(`FakeWorkatoClient only supports recipes/{id}. Got: ${endpoint}`);
  }

  fetchPaginated(resourcePath) {
    throw new Error("FakeWorkatoClient.fetchPaginated not implemented in tests.");
  }
}

class FakeSheetService {
  constructor(requestIds) {
    this._requestIds = (requestIds || []).map(String);
    this.writes = {}; // sheetKey -> rows
    this.appendedDebug = [];
  }

  readRequests() { return this._requestIds.slice(); }

  write(sheetKey, rows) {
    this.writes[sheetKey] = rows;
  }

  appendDebugRows(rows) {
    this.appendedDebug = this.appendedDebug.concat(rows);
  }
}

class FakeDriveService {
  constructor() {
    this.saved = []; // {id,name,ext,content}
    this.nextUrl = "https://drive.fake/file/123";
  }

  saveText(id, name, ext, content) {
    this.saved.push({ id: String(id), name, ext, content: String(content || "") });
    return this.nextUrl;
  }

  saveLog(id, name, jsonObject) {
    // Optional: used by runLogicDebug in your app; not needed for these tests.
    return this.nextUrl;
  }
}

/**
 * Temporarily override AppConfig.get() for tests; always restores afterward.
 * @param {Object} testConfig
 * @param {Function} fn
 */
function withTestConfig(testConfig, fn) {
  const original = AppConfig.get;
  AppConfig.get = function () { return testConfig; };
  try { return fn(); } finally { AppConfig.get = original; }
}

/** ****************************************************************************************
 * FILE: TEST_Suite.gs
 * Comprehensive suite focused on the new functionality:
 * - call edges extraction (step path, branch context, id keys)
 * - transitive call graph (depth, cycles, dedupe)
 * - Mermaid rendering (sanitization, truncation, edge labels, dedupe)
 * - mapping rows (DataMapper.mapCallEdgesToRows)
 * - WorkatoSyncApp.runProcessMaps orchestration (writes PROCESS_MAPS)
 ******************************************************************************************/

function runAllTests() {
  const runner = new TestRunner();

  // -----------------------------
  // Unit: RecipeAnalyzerService internals and outputs
  // -----------------------------
  runner.add("cleanDataPill converts label DP to {{Label}}", () => {
    const client = new FakeWorkatoClient({});
    const svc = new RecipeAnalyzerService(client);

    const raw = "#{_dp('{\\\"label\\\":\\\"Ticket ID\\\",\\\"path\\\":[\\\"ticket\\\",\\\"id\\\"]}')}";
    const cleaned = svc._cleanDataPill(raw);
    Assert.equal(cleaned, "{{Ticket ID}}");
  });

  runner.add("formatConditionSummary returns readable conditions", () => {
    const client = new FakeWorkatoClient({});
    const svc = new RecipeAnalyzerService(client);

    const step = {
      keyword: "if",
      input: {
        operand: "and",
        conditions: [
          {
            lhs: "#{_dp('{\\\"label\\\":\\\"Priority\\\",\\\"path\\\":[\\\"priority\\\"]}')}",
            operand: "=",
            rhs: "P1"
          }
        ]
      }
    };

    const s = svc._formatConditionSummary(step);
    Assert.contains(s, "{{Priority}} = P1");
  });

  runner.add("getCallEdges detects calls across flow_id, recipe_id, callable_recipe_id", () => {
    const codeObj = Fixtures.recipeCodeBlock_withBranchesAndCalls();
    const r = Fixtures.recipePayload("100", "Root", "p1", "f1", codeObj);

    const client = new FakeWorkatoClient({ "100": r });
    const svc = new RecipeAnalyzerService(client);

    const edges = svc.getCallEdges("100");
    const childIds = edges.map(e => String(e.child_recipe_id)).sort();

    Assert.deepEqual(childIds, ["200", "300", "400", "500"].sort());

    // ensure id_key captured
    const byChild = Object.fromEntries(edges.map(e => [String(e.child_recipe_id), e]));
    Assert.equal(byChild["200"].id_key, "flow_id");
    Assert.equal(byChild["300"].id_key, "recipe_id");
    Assert.equal(byChild["400"].id_key, "callable_recipe_id");
  });

  runner.add("getCallEdges records step_path and branch_context for if/else/error", () => {
    const codeObj = Fixtures.recipeCodeBlock_withBranchesAndCalls();
    const r = Fixtures.recipePayload("100", "Root", "p1", "f1", codeObj);

    const client = new FakeWorkatoClient({ "100": r });
    const svc = new RecipeAnalyzerService(client);

    const edges = svc.getCallEdges("100");
    const e200 = edges.find(e => String(e.child_recipe_id) === "200");
    const e300 = edges.find(e => String(e.child_recipe_id) === "300");
    const e400 = edges.find(e => String(e.child_recipe_id) === "400");

    Assert.ok(e200.step_path, "Expected step_path for 200");
    Assert.ok(e300.step_path, "Expected step_path for 300");
    Assert.ok(e400.step_path, "Expected step_path for 400");

    Assert.contains(e200.branch_context, "IF");
    Assert.contains(e300.branch_context, "ELSE");
    Assert.contains(e400.branch_context, "ON_ERROR");
  });

  // -----------------------------
  // Unit: Transitive graph building
  // -----------------------------
  runner.add("buildTransitiveCallGraph respects depth 0 (no recursion)", () => {
    const { r100, r200, r300 } = Fixtures.graphRecipes_cycle();
    const client = new FakeWorkatoClient({ "100": r100, "200": r200, "300": r300 });
    const svc = new RecipeAnalyzerService(client);

    const graph = svc.buildTransitiveCallGraph("100", 0);
    Assert.ok(graph.nodes.has("100"));
    // still includes edges from root
    Assert.equal(graph.edges.length, 1);
    Assert.equal(String(graph.edges[0].child_recipe_id), "200");
    // but should not expand child nodes at depth 0
    Assert.ok(!graph.nodes.has("200"));
  });

  runner.add("buildTransitiveCallGraph expands to depth 2 and detects cycles", () => {
    const { r100, r200, r300 } = Fixtures.graphRecipes_cycle();
    const client = new FakeWorkatoClient({ "100": r100, "200": r200, "300": r300 });
    const svc = new RecipeAnalyzerService(client);

    const graph = svc.buildTransitiveCallGraph("100", 3);
    Assert.ok(graph.nodes.has("100"));
    Assert.ok(graph.nodes.has("200"));
    Assert.ok(graph.nodes.has("300"));

    // edges: 100->200, 200->300, 300->200
    Assert.equal(graph.edges.length, 3);

    Assert.ok((graph.notes || []).some(n => String(n).includes("Cycle detected")));
  });

  // -----------------------------
  // Unit: Mermaid rendering
  // -----------------------------
  runner.add("renderMermaidCallGraph emits flowchart TD, nodes, and labeled edges", () => {
    const codeObj = Fixtures.recipeCodeBlock_withBranchesAndCalls();
    const r100 = Fixtures.recipePayload("100", 'Root "Recipe"\nName', "p1", "f1", codeObj);

    // provide child stubs so node labels resolve names in cache
    const r200 = Fixtures.recipePayload("200", "Escalate", "p1", "f1", { block: [] });
    const r300 = Fixtures.recipePayload("300", "Triage", "p1", "f1", { block: [] });
    const r400 = Fixtures.recipePayload("400", "Fallback", "p1", "f1", { block: [] });
    const r500 = Fixtures.recipePayload("500", "Billing", "p1", "f1", { block: [] });

    const client = new FakeWorkatoClient({ "100": r100, "200": r200, "300": r300, "400": r400, "500": r500 });
    const svc = new RecipeAnalyzerService(client);

    const graph = svc.buildTransitiveCallGraph("100", 1);
    const mermaid = svc.renderMermaidCallGraph("100", graph);

    Assert.contains(mermaid, "flowchart TD");
    // root label should be sanitized (no raw double quotes/newlines)
    Assert.ok(!mermaid.includes('\nName"'), "Expected sanitized label not to preserve raw newline+quote");
    // edges should include some label content
    Assert.ok(mermaid.includes("-->|"), "Expected at least one labeled edge");
  });

  runner.add("renderMermaidCallGraph dedupes identical edges", () => {
    const r100 = Fixtures.recipePayload("100", "Root", "p1", "f1", {
      block: [
        { provider: "workato_recipe_function", name: "Call 200", input: { flow_id: "200" } },
        { provider: "workato_recipe_function", name: "Call 200", input: { flow_id: "200" } }
      ]
    });
    const r200 = Fixtures.recipePayload("200", "Child", "p1", "f1", { block: [] });

    const client = new FakeWorkatoClient({ "100": r100, "200": r200 });
    const svc = new RecipeAnalyzerService(client);

    const graph = svc.buildTransitiveCallGraph("100", 0);
    const mermaid = svc.renderMermaidCallGraph("100", graph);

    // Only one edge should remain after dedupe
    const edgeCount = mermaid.split("\n").filter(l => l.includes("-->")).length;
    Assert.equal(edgeCount, 1);
  });

  // -----------------------------
  // Unit: DataMapper mapping for call edges
  // -----------------------------
  runner.add("mapCallEdgesToRows resolves project/folder and child recipe name", () => {
    const recipe = { id: "100", name: "Root", project_id: "p1", folder_id: "f1" };
    const edges = [{
      parent_recipe_id: "100",
      parent_recipe_name: "Root",
      child_recipe_id: "200",
      id_key: "flow_id",
      provider: "workato_recipe_function",
      step_name: "Call 200",
      step_path: "0",
      branch_context: "IF x"
    }];

    const projectMap = { "p1": "Project A" };
    const folderMap = { "f1": "Folder A" };
    const recipeNameMap = { "200": "Child Recipe" };

    const rows = DataMapper.mapCallEdgesToRows(recipe, edges, projectMap, folderMap, recipeNameMap);
    Assert.equal(rows.length, 1);
    Assert.equal(rows[0][0], "100"); // parent id
    Assert.equal(rows[0][2], "Project A");
    Assert.equal(rows[0][3], "Folder A");
    Assert.equal(rows[0][8], "200"); // child id
    Assert.equal(rows[0][9], "Child Recipe"); // child name
    Assert.equal(rows[0][10], "flow_id"); // id key
  });

  // -----------------------------
  // Integration-ish: WorkatoSyncApp.runProcessMaps (no real APIs)
  // -----------------------------
  runner.add("runProcessMaps writes PROCESS_MAPS with header + rows", () => {
    const testConfig = {
      API: {
        TOKEN: "x",
        BASE_URL: "https://example/api",
        PER_PAGE: 100,
        MAX_CALLS: 500,
        THROTTLE_MS: 0,
        RECIPE_LIMIT_DEBUG: 100,
        PROCESS_MAP_DEPTH: 2,
        MAX_RETRIES: 1
      },
      SHEETS: {
        PROCESS_MAPS: "process_maps",
        LOGIC_INPUT: "logic_requests"
      },
      HEADERS: {
        PROCESS_MAPS: ["Root recipe ID", "Root recipe name", "Depth", "Mermaid (flowchart)", "Notes", "Drive link", "Generated at"],
        LOGIC_INPUT: ["Enter recipe IDs below (one per row)"]
      },
      CONSTANTS: { CELL_CHAR_LIMIT: 48000, MERMAID_LABEL_MAX: 80, FLOW_ID_KEYS: ["flow_id", "recipe_id", "callable_recipe_id"], RECIPE_PROVIDERS: ["workato_recipe_function","workato_callable_recipe"] },
      DEBUG: { LOG_TO_DRIVE: true, LOG_TO_SHEET: false },
      VERTEX: {},
      VERBOSE: false
    };

    const r100 = Fixtures.recipePayload("100", "Root", "p1", "f1", { block: [{ provider:"workato_recipe_function", name:"Call 200", input:{ flow_id:"200" } }] });
    const r200 = Fixtures.recipePayload("200", "Child", "p1", "f1", { block: [] });

    const client = new FakeWorkatoClient({ "100": r100, "200": r200 });

    withTestConfig(testConfig, () => {
      // Build an app instance then replace services with fakes
      const app = new WorkatoSyncApp();
      app.analyzerService = new RecipeAnalyzerService(client);
      app.sheetService = new FakeSheetService(["100"]);
      app.driveService = new FakeDriveService();

      app.runProcessMaps();

      const written = app.sheetService.writes["PROCESS_MAPS"];
      Assert.ok(written, "Expected PROCESS_MAPS to be written");
      Assert.equal(written[0][0], "Root recipe ID"); // header row
      Assert.equal(written.length, 2); // header + 1 row
      Assert.equal(written[1][0], "100");
      Assert.equal(written[1][1], "Root");
      Assert.contains(written[1][3], "flowchart TD"); // mermaid content
    });
  });

  runner.add("runProcessMaps saves to Drive when Mermaid exceeds cell limit", () => {
    // Force tiny cell limit to trigger truncation + drive save
    const testConfig = {
      API: {
        TOKEN: "x",
        BASE_URL: "https://example/api",
        PER_PAGE: 100,
        MAX_CALLS: 500,
        THROTTLE_MS: 0,
        RECIPE_LIMIT_DEBUG: 100,
        PROCESS_MAP_DEPTH: 1,
        MAX_RETRIES: 1
      },
      SHEETS: {
        PROCESS_MAPS: "process_maps",
        LOGIC_INPUT: "logic_requests"
      },
      HEADERS: {
        PROCESS_MAPS: ["Root recipe ID", "Root recipe name", "Depth", "Mermaid (flowchart)", "Notes", "Drive link", "Generated at"],
        LOGIC_INPUT: ["Enter recipe IDs below (one per row)"]
      },
      CONSTANTS: { CELL_CHAR_LIMIT: 200, MERMAID_LABEL_MAX: 80, FLOW_ID_KEYS: ["flow_id", "recipe_id", "callable_recipe_id"], RECIPE_PROVIDERS: ["workato_recipe_function","workato_callable_recipe"] },
      DEBUG: { LOG_TO_DRIVE: true, LOG_TO_SHEET: false },
      VERTEX: {},
      VERBOSE: false
    };

    // Generate many edges to bloat mermaid text
    const bigBlock = { block: [] };
    for (let i = 0; i < 50; i++) {
      bigBlock.block.push({ provider:"workato_recipe_function", name:`Call ${200+i}`, input:{ flow_id:String(200+i) } });
    }
    const r100 = Fixtures.recipePayload("100", "Root", "p1", "f1", bigBlock);

    const recipes = { "100": r100 };
    for (let i = 0; i < 50; i++) {
      recipes[String(200+i)] = Fixtures.recipePayload(String(200+i), `Child ${200+i}`, "p1", "f1", { block: [] });
    }

    const client = new FakeWorkatoClient(recipes);

    withTestConfig(testConfig, () => {
      const app = new WorkatoSyncApp();
      app.analyzerService = new RecipeAnalyzerService(client);
      app.sheetService = new FakeSheetService(["100"]);
      const fakeDrive = new FakeDriveService();
      fakeDrive.nextUrl = "https://drive.fake/mermaid.mmd";
      app.driveService = fakeDrive;

      app.runProcessMaps();

      const row = app.sheetService.writes["PROCESS_MAPS"][1];
      const mermaidCell = row[3];
      const notes = row[4];
      const link = row[5];

      Assert.contains(mermaidCell, "TRUNCATED");
      Assert.contains(notes, "truncated");
      Assert.contains(link, "HYPERLINK");
      Assert.equal(fakeDrive.saved.length, 1);
      Assert.equal(fakeDrive.saved[0].ext, "mmd");
      Assert.contains(fakeDrive.saved[0].content, "flowchart TD");
    });
  });

  // Run + optionally write results to a sheet
  runner.run({ writeToSheet: true });
}
