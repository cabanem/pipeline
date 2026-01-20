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

/**
 * @class
 * @classdesc Main Application Controller.
 * * Orchestrates the fetching, transformation, and writing of Workato data.
 * * WorkatoSyncApp ID: 1sl2ZfkgwX57EIygRwEP7nkXTK8BEXaB60cnFKsqhg2DWic3V0SVAzrYS
 */
class WorkatoSyncApp {
  constructor(ctx = null) {
    // Backwards compatible: if no ctx provided, behave exactly as before.
    const context = ctx || new AppContext();
    this.ctx = context;

    this.config = context.config;
    this.client = context.client; // stable direct fetch handle
    this.inventoryService = context.inventoryService;
    this.analyzerService = context.analyzerService;
    this.sheetService = context.sheetService;
    this.driveService = context.driveService;
  }
  /**
   * The main execution method. 
   * Performs authentication check, fetches all resources, transforms data, 
   * resolves dependencies, and writes to Sheets.
   */
  runInventorySync() {
    return new InventorySyncRunner().run(this.ctx);
  }
  /**
   * Reads specific IDs from the input sheet and fetches step-by-step logic.
   */
  runLogicDebug(idsOverride = null) {
    return new LogicDebugRunner().run(this.ctx, idsOverride);
  }
  /**
   * Reads IDs from 'logic_requests', fetches them, sends to Gemini, and writes output.
   */
  runAiAnalysis(idsOverride = null) {
    return new AiAnalysisRunner().run(this.ctx, idsOverride);
  }
  /**
   * Reads recipe IDs from 'logic_requests' and generates process maps using the Library.
   * @param {{ mode?: string, callDepth?: number, maxNodes?: number }} [options]
   */
  runProcessMaps(options = {}, idsOverride = null) {
    return new ProcessMapsRunner().run(this.ctx, options, idsOverride);
  }
}
