# HR Email Automation System: Comprehensive Lessons Learned

## Document Overview

**Project**: HR Email Automation System using Google Cloud Vertex AI  
**Platform**: Workato Custom Connectors  
**Author**: Emily Cabaniss  
**Version**: 1.0  
**Last Updated**: December 2024

This document captures patterns, lessons learned, and best practices from building a production HR email automation system that uses Vertex AI RAG Engine, LLM generation, and Workato connectors. Each section describes the pattern, explains when and why to apply it, details how it works, and provides industry context.

---

## Table of Contents

1. [Architecture & Design Principles](#1-architecture--design-principles)
2. [RAG Engine Patterns](#2-rag-engine-patterns)
3. [Email Processing & Safety](#3-email-processing--safety)
4. [Google Cloud API Integration](#4-google-cloud-api-integration)
5. [LLM Schema Compliance](#5-llm-schema-compliance)
6. [Response Generation](#6-response-generation)
7. [Telemetry & Observability](#7-telemetry--observability)
8. [Authentication & Security](#8-authentication--security)
9. [Error Handling & Resilience](#9-error-handling--resilience)
10. [Configuration Management](#10-configuration-management)
11. [UI/UX Patterns (Workato-Specific)](#11-uiux-patterns-workato-specific)
12. [Performance Optimization](#12-performance-optimization)
13. [Testing & Debugging](#13-testing--debugging)

---

## 1. Architecture & Design Principles

### 1.1 Pipeline Architecture

#### Description
The system implements a multi-stage pipeline where each stage has clear inputs, outputs, and failure modes. Stages are loosely coupled and communicate through well-defined data contracts.

#### When to Apply
- When building systems with multiple processing stages
- When different stages have different failure characteristics
- When stages may need to be independently scaled or modified

#### Why It Matters
Pipeline architecture enables:
- **Graceful degradation**: One stage failing doesn't necessarily fail the entire pipeline
- **Independent testing**: Each stage can be tested in isolation
- **Flexibility**: Stages can be reordered, replaced, or enhanced independently
- **Observability**: Clear boundaries make debugging easier

#### How It Works

**Pipeline Stages (in order):**

| Stage | Action | Purpose | Failure Mode |
|-------|--------|---------|--------------|
| 1 | `deterministic_filter` | Rule-based pre-screening | Hard block or soft score |
| 2 | `ai_triage_filter` | LLM triage (IRRELEVANT/HUMAN/KEEP) | Falls back to HUMAN |
| 3 | `ai_intent_classifier` | Intent detection | Falls back to "unknown" |
| 4 | `embed_text_against_categories` | Semantic similarity | Requires 2+ categories |
| 5 | `rerank_shortlist` | LLM probability distribution | Can skip (mode=none) |
| 6 | `llm_referee_with_contexts` | Final category selection | Uses fallback category |
| 7 | `rag_retrieve_contexts_enhanced` | Context retrieval from corpus | Returns placeholder on empty |
| 8 | `rank_texts_with_ranking_api` | Context reranking | Partial success allowed |
| 9 | `gen_generate` | Response generation | Grounded-only responses |

#### Industry Context
This follows the **Pipes and Filters** architectural pattern, common in ETL systems and message processing. The key insight is that each filter should be idempotent and stateless where possible, enabling retry and parallel processing.

#### Common Pitfalls
- **Over-coupling stages**: Avoid having Stage 3 depend on implementation details of Stage 1
- **Missing failure boundaries**: Each stage should handle its own errors gracefully
- **Ignoring partial success**: Design for scenarios where some items succeed and others fail

---

### 1.2 Signal Threading Pattern

#### Description
Upstream stages produce "signals" (category, confidence, intent, triage decision) that downstream stages consume to adjust their behavior dynamically.

#### When to Apply
- When later stages benefit from earlier decisions
- When you want dynamic parameter adjustment without hardcoding
- When building adaptive systems that respond to context

#### Why It Matters
Signals enable the system to:
- Adjust LLM temperature based on upstream confidence
- Focus retrieval on relevant categories
- Modify response style based on detected intent
- Track decision provenance through the pipeline

#### How It Works

```ruby
# Each action outputs signals for downstream consumption
out['signals_category'] = chosen
out['signals_confidence'] = confidence
out['signals_intent'] = input['signals_intent']  # Pass-through
out['signals_triage'] = input['signals_triage']  # Pass-through

# Downstream actions consume and act on signals
if use_signal_enrichment != false
  # Category enhances domain focus
  if signals_category.present?
    sys_text += "\n\nDomain context: This is a #{signals_category} inquiry."
    applied_signals << 'category'
  end

  # Confidence adjusts temperature (higher confidence = lower temperature)
  if signals_confidence
    temp = case signals_confidence
           when 0.8..1.0 then 0.0
           when 0.6..0.8 then 0.3
           else 0.5
           end
    gen_cfg['temperature'] = temp
    applied_signals << 'confidence'
  end
end
```

**Signal Flow Diagram:**
```
deterministic_filter → signals_triage (decision)
        ↓
ai_triage_filter → signals_confidence, signals_domain
        ↓
ai_intent_classifier → signals_intent
        ↓
embed_text_against_categories → signals_category (initial)
        ↓
llm_referee_with_contexts → signals_category (refined), signals_confidence
        ↓
gen_generate ← consumes all signals to adjust behavior
```

#### Industry Context
This pattern is related to **Context Propagation** in distributed tracing (like OpenTelemetry) and **Feature Flags** in progressive delivery. The key principle is that context flows forward, enabling adaptive behavior without tight coupling.

#### Common Pitfalls
- **Signal explosion**: Don't create signals for everything; focus on actionable decisions
- **Implicit dependencies**: Document which signals each stage produces and consumes
- **Missing signal validation**: Downstream stages should handle missing/invalid signals gracefully

---

### 1.3 The Simplicity Principle

#### Description
Prefer simple, elegant solutions over complex abstractions. When in doubt, implement the straightforward approach first.

#### When to Apply
- When designing any new feature
- When refactoring existing code
- When choosing between multiple implementation approaches

#### Why It Matters
Complex systems have more failure modes, are harder to debug, and are more expensive to maintain. Simple solutions are often more robust than clever ones.

#### Key Examples from This Project

**Example 1: Template System Removal**
- **Initial approach**: Complex template system with category-specific response formats
- **Problem**: Templates were rigid, hard to maintain, and often produced unnatural responses
- **Solution**: Let the LLM work directly with retrieved context using clear instructions
- **Result**: More natural responses, easier debugging, simpler codebase

**Example 2: Metadata Immutability Acceptance**
- **Challenge**: Vertex AI RAG Engine doesn't support updating metadata on uploaded files
- **Over-engineered approach**: Build complex update mechanisms, external metadata stores
- **Simple approach**: Accept immutability as a constraint; use versioned corpora or re-import
- **Result**: Less code, fewer failure modes, cleaner architecture

**Example 3: Helper Method Caution**
- **Issue**: A helper method was silently swallowing errors, causing confusing failures
- **Solution**: Inline implementation for critical paths where error visibility matters
- **Lesson**: Abstraction has costs; sometimes duplication is clearer than indirection

#### Industry Context
This aligns with YAGNI (You Aren't Gonna Need It), KISS (Keep It Simple, Stupid), and the Unix philosophy of doing one thing well. Studies consistently show that simpler systems have fewer bugs and are easier to maintain.

#### Decision Framework
When evaluating complexity:
1. What failure modes does this add?
2. How will we debug this at 3 AM?
3. Can a new team member understand this in 15 minutes?
4. What's the simplest thing that could possibly work?

---

## 2. RAG Engine Patterns

### 2.1 PDF Text Extraction & Sanitization

#### Description
PDF-extracted text frequently contains artifacts (double-escaped characters, encoding issues, layout artifacts) that break JSON serialization and degrade LLM performance.

#### When to Apply
- When processing any PDF content through RAG
- When JSON serialization fails with encoding errors
- When LLM responses reference garbled text

#### Why It Matters
PDF extraction happens at import time, and the parsing mode significantly affects text quality. Residual artifacts can cause:
- JSON serialization failures
- LLM confusion from garbled text
- Poor retrieval relevance scores

#### How It Works

**PDF Parsing Modes (set at import time):**
- `Default`: Standard text extraction
- `Layout`: Preserves document structure
- `LLM`: Uses LLM for intelligent extraction (higher quality, higher cost)

**Multi-Phase Sanitization (at retrieval time):**

```ruby
sanitize_pdf_text: lambda do |raw_text|
  text = raw_text.to_s
  
  # Phase 1: Fix double-escaped sequences (most common PDF artifact)
  text = text
    .gsub(/\\\\n/, "\n")        # \\n → newline
    .gsub(/\\\\t/, " ")         # \\t → space
    .gsub(/\\\\r/, "")          # \\r → remove
    .gsub(/\\\\"/, '"')         # \\" → "
    .gsub(/\\\\/, "\\")         # \\\\ → \

  # Phase 2: Handle single-escaped sequences
  text = text
    .gsub(/\\n/, "\n")
    .gsub(/\\t/, " ")
    .gsub(/\\r/, "")

  # Phase 3: Remove control characters (keep tab, newline, carriage return)
  control_chars = (0..31).map { |i| i.chr }.join
  keep_chars = "\t\n\r"
  text = text.delete(control_chars.delete(keep_chars))

  # Phase 4: Fix PDF-specific layout artifacts
  text = text
    .gsub(/(\w)-\n(\w)/, '\1\2')    # Rejoin hyphenated words
    .gsub(/\r\n|\r/, "\n")          # Normalize line endings
    .gsub(/\n{3,}/, "\n\n")         # Collapse excessive newlines
    .gsub(/[ \t]+/, ' ')            # Collapse whitespace

  # Phase 5: Encoding cleanup with fallbacks
  text.encode('UTF-8', invalid: :replace, undef: :replace, replace: ' ').strip
end
```

**PDF Source Detection:**
```ruby
is_pdf_source?: lambda do |source_uri, metadata|
  # Check URI extension
  return true if source_uri.to_s.downcase.end_with?('.pdf')
  
  # Check metadata for file type indicators
  if metadata.is_a?(Hash)
    file_type = (metadata['mimeType'] || metadata['content_type'] || '').to_s.downcase
    return true if file_type.include?('pdf')
    
    # Page numbers are strong PDF indicators
    return true if metadata.key?('page') || metadata.key?('pageNumber')
  end
  
  false
end
```

#### Industry Context
PDF extraction is notoriously difficult. Tools like Apache PDFBox, PyMuPDF, and commercial solutions all produce artifacts. The industry best practice is defensive parsing with multiple fallback strategies.

#### Common Pitfalls
- **Assuming clean input**: Always sanitize PDF content, even from trusted sources
- **Single-pass sanitization**: Multiple phases catch different artifact types
- **Ignoring encoding**: UTF-8 encoding issues are common; always specify fallbacks

---

### 2.2 Context Extraction & Normalization

#### Description
Vertex AI RAG API returns contexts in a nested structure that can vary between API versions. Robust extraction handles all variations.

#### When to Apply
- When parsing RAG retrieval responses
- When integrating with multiple API versions
- When building resilient context processing

#### Why It Matters
The API returns `response['contexts']['contexts']` (nested), but this can vary. Failing to handle variations causes silent failures or crashes.

#### How It Works

```ruby
safe_extract_contexts: lambda do |response|
  return [] unless response.is_a?(Hash)
  
  # Primary path: response.contexts.contexts (Vertex AI RAG standard)
  if response.dig('contexts', 'contexts').is_a?(Array)
    return response.dig('contexts', 'contexts')
  end
  
  # Secondary: response.contexts as array
  if response['contexts'].is_a?(Array)
    return response['contexts']
  end
  
  # Tertiary: recursive search with depth limit
  find_contexts_array = lambda do |obj, depth = 0|
    return nil if depth > 3  # Prevent infinite recursion
    
    case obj
    when Hash
      if obj['contexts'].is_a?(Array) && !obj['contexts'].empty?
        first = obj['contexts'].first
        # Verify it looks like context objects
        if first.is_a?(Hash) && 
           (first.key?('text') || first.key?('chunkText') || first.key?('sourceUri'))
          return obj['contexts']
        end
      end
      obj.each_value { |v| result = find_contexts_array.call(v, depth + 1); return result if result }
    when Array
      # Check if this looks like a contexts array directly
      if !obj.empty? && obj.first.is_a?(Hash) && 
         (obj.first.key?('text') || obj.first.key?('chunkId'))
        return obj
      end
    end
    nil
  end
  
  find_contexts_array.call(response) || []
end
```

**Field Name Variations:**
| Concept | Possible Field Names |
|---------|---------------------|
| Text content | `text`, `chunkText`, `chunk.text` |
| Source URI | `sourceUri`, `uri`, `source_uri` |
| Score | `score`, `relevanceScore` |
| Chunk ID | `chunkId`, `id`, `chunk_id` |
| Metadata | `metadata`, `chunkMetadata` |

#### Industry Context
This is an application of **Robustness Principle** (Postel's Law): "Be conservative in what you send, liberal in what you accept." API responses change over time; defensive parsing ensures compatibility.

#### Common Pitfalls
- **Assuming fixed structure**: API responses evolve; hardcoded paths break
- **No depth limits**: Recursive search without limits can cause stack overflow
- **Ignoring empty results**: Handle empty arrays as a valid (if unfortunate) state

---

### 2.3 Metadata Strategy

#### Description
Vertex AI RAG Engine with RagManaged does NOT support updating metadata on uploaded files. Metadata is immutable after import.

#### When to Apply
- When designing document ingestion workflows
- When planning category-based filtering
- When metadata needs to change over time

#### Why It Matters
Understanding this limitation early prevents wasted effort building update mechanisms that won't work. Design decisions must account for immutability.

#### How It Works

**Metadata at Import Time:**
```ruby
# Metadata is set during file import and cannot be changed
import_config = {
  'ragFileParsingConfig' => {
    'useAdvancedPdfParsing' => true
  },
  'metadata' => {
    'document_type' => 'policy',
    'category' => 'PTO',
    'source_system' => 'sharepoint',
    'import_date' => Time.now.utc.iso8601
  }
}
```

**Workarounds for Metadata Updates:**

1. **Re-import with new metadata**: Delete and re-import the file
2. **Parallel corpus strategy**: Maintain versioned corpora (hr-corpus-v1, hr-corpus-v2)
3. **External metadata store**: Use Firestore for dynamic metadata, RAG for content
4. **Filename conventions**: Encode metadata in filenames (e.g., `PTO_policy_v2_2024.pdf`)

**Metadata Filtering During Retrieval:**
```ruby
# Metadata filters are applied at retrieval time
retrieval_config = {
  'topK' => 20,
  'filter' => {
    'vectorSimilarityThreshold' => 0.65,
    # Metadata filtering is supported for RagManaged
    'metadataFilter' => {
      'category' => 'PTO'
    }
  }
}
```

#### Industry Context
Many vector databases have similar limitations. Pinecone supports metadata updates; Weaviate supports partial updates. The trend is toward more flexible metadata, but immutability is common in managed services.

#### Design Recommendations
- **Keep inline metadata minimal**: document_type, category, source_system
- **Store dynamic data externally**: Feature flags, policies, thresholds in GCS/Firestore
- **Version your corpora**: Plan for corpus evolution from the start
- **Document the schema**: Metadata schemas should be versioned and documented

---

### 2.4 Document Categorization Automation

#### Description
Automate the classification of documents before import to ensure correct metadata assignment.

#### When to Apply
- When importing documents from diverse sources
- When manual categorization is error-prone or slow
- When categories need to be consistent across the corpus

#### Why It Matters
Incorrect categorization degrades retrieval quality. If a PTO document is categorized as "Benefits," category-filtered retrieval will miss it.

#### How It Works

**Multi-Tier Classification Approach:**

```
Tier 1: Title/Header Analysis (fast, cheap)
    ↓ confidence < 0.8
Tier 2: Content Clustering (embedding similarity)
    ↓ confidence < 0.7
Tier 3: LLM Classification (accurate, expensive)
    ↓ confidence < 0.6
Human Review Queue
```

**Using Existing Embedding Action:**
```ruby
# Leverage embed_text_against_categories for classification
classification_result = call(:embed_text_against_categories, {
  'email_text' => document_content[0..4000],  # First 4K chars
  'categories' => [
    { 'name' => 'PTO', 'description' => 'Paid time off, vacation, sick leave' },
    { 'name' => 'Benefits', 'description' => '401k, health insurance, HSA' },
    { 'name' => 'Payroll', 'description' => 'Compensation, direct deposit, taxes' },
    # ...
  ],
  'shortlist_k' => 3
})

top_category = classification_result['shortlist'].first
top_score = classification_result['scores'].first['score']

if top_score >= 0.8
  # Auto-import with category
  import_with_category(document, top_category)
elsif top_score >= 0.6
  # Flag for review but proceed
  import_with_review_flag(document, top_category, top_score)
else
  # Queue for human review
  queue_for_human_review(document, classification_result)
end
```

**Progressive Enhancement:**
- Store human corrections in Firestore
- Periodically retrain category definitions with examples from corrections
- Track classification accuracy over time

#### Industry Context
This is a **Human-in-the-Loop (HITL)** pattern common in ML systems. The key is establishing clear confidence thresholds and feedback loops for continuous improvement.

---

## 3. Email Processing & Safety

### 3.1 Chain & Forward Detection

#### Description
Distinguish direct employee requests from forwarded email chains and internal HR discussions to prevent inappropriate automated responses.

#### When to Apply
- Always, for any email automation system
- Especially critical for HR, legal, and customer-facing systems
- When the cost of an inappropriate response is high

#### Why It Matters
Auto-responding to forwarded chains can:
- Expose confidential discussions
- Send responses to the wrong person
- Create embarrassing situations (replying to HR discussing an employee)
- Violate privacy regulations

#### How It Works

**Hard Excludes (immediate block):**

| Pattern | Regex/Rule | Email Type |
|---------|-----------|------------|
| FWD: subjects | `^\s*(FW|Fw|Fwd|FWD):\s*` | `forwarded_chain` |
| RE: subjects | `^\s*(RE|Re):\s*` | `reply_chain` |
| Original Message | `---- Original Message ----` | `forwarded_chain` |
| Forwarded message | `---- Forwarded message ----` | `forwarded_chain` |
| Multiple From: headers | `From:.*From:` | `forwarded_chain` |
| Quote markers | `>>>` at line start | `forwarded_chain` |

**Soft Signals (negative weights):**

| Signal | Weight | Rationale |
|--------|--------|-----------|
| Third-person references ("this employee", "they need") | -3 | Indicates discussing someone, not from them |
| Internal routing ("please handle", "FYI") | -2 | HR-to-HR communication |
| Employee name in third person | -2 | "John's request" vs "my request" |
| Multiple email addresses in thread | -1 | Likely a forwarded discussion |

**Email Type Classification:**
```ruby
email_types = {
  'direct_request' => 'Employee directly emailing HR',
  'forwarded_chain' => 'Forwarded email thread',
  'internal_discussion' => 'HR-to-HR communication',
  'cc_consultation' => 'Employee CC\'d for visibility',
  'automated' => 'System-generated email',
  'newsletter' => 'Marketing/newsletter content'
}

# Only direct_request with information_request intent is eligible for auto-response
generation_eligible = (email_type == 'direct_request' && intent == 'information_request')
```

#### Industry Context
This is a **Safety Gate** pattern. Financial services, healthcare, and legal industries have similar requirements. The principle is that certain message types should never receive automated responses, regardless of content quality.

#### Implementation Pattern
```ruby
hr_eval_hard_enhanced?: lambda do |email, hard_pack|
  result = call(:hr_eval_hard?, email, hard_pack)
  
  if result[:hit] && result[:action]
    case result[:action]
    when 'forwarded_chain', 'ticket_update'
      result[:reason] = result[:action]  
      result[:category] = 'HUMAN'  # Requires human review
    when 'automated', 'newsletter', 'system'
      result[:reason] = result[:action]
      result[:category] = 'IRRELEVANT'  # Can be ignored
    end
  end
  
  result
end
```

---

### 3.2 Multi-Layer Safety Gates

#### Description
Implement multiple independent safety checks at different pipeline stages to prevent inappropriate automated responses.

#### When to Apply
- When building any system that sends automated communications
- When handling sensitive topics (HR, medical, legal, financial)
- When the cost of a mistake exceeds the cost of being conservative

#### Why It Matters
Defense in depth ensures that if one safety layer fails, others catch the issue. No single point of failure for safety-critical decisions.

#### How It Works

**Layer 1: Deterministic Filter (Rule-Based)**
- Pattern matching for known unsafe patterns
- No LLM involved; fast and deterministic
- Blocks: forwards, auto-replies, newsletters, bounces

**Layer 2: AI Triage Filter (LLM-Based)**
- Classifies as IRRELEVANT, HUMAN, or KEEP
- Detects semantic signals patterns miss
- Blocks: complaints, escalations, sensitive topics

**Layer 3: Intent Classification**
- Distinguishes information requests from action requests
- Only information_request intent eligible for auto-response
- Action requests require human processing

**Layer 4: Generator Gate (Pre-Response)**
```ruby
# Final check before response generation
generator_gate = {
  'pass_to_responder' => (
    email_type == 'direct_request' &&
    intent == 'information_request' &&
    confidence >= 0.60 &&
    !safety_blocked
  ),
  'reason' => compute_block_reason(...),
  'generator_hint' => confidence >= 0.60 ? 'proceed' : 'check_fallback'
}
```

**Safety-Blocked Topics:**

| Category | Examples | Action |
|----------|----------|--------|
| Legal language | "lawsuit", "attorney", "discrimination" | HUMAN |
| Crisis indicators | "suicidal", "self-harm", "harassment" | HUMAN + Alert |
| Medical privacy | "diagnosis", "medical records", "disability" | HUMAN |
| Emotional distress | Strong negative sentiment, crisis language | HUMAN |

#### Industry Context
This implements the **Swiss Cheese Model** of accident prevention—multiple barriers, each with holes, but aligned so that no single failure path exists through all layers.

#### Common Pitfalls
- **Assuming one layer is enough**: Always have multiple independent checks
- **Over-automating safety decisions**: When in doubt, route to human
- **Ignoring edge cases**: Test with adversarial examples

---

### 3.3 Channel Context (Anti-Loop Pattern)

#### Description
Inject context about the mailbox/channel identity to prevent the LLM from telling users to contact the same channel they're already using.

#### When to Apply
- When building email/chat automation
- When the system responds on behalf of a shared mailbox
- When users interact through a specific channel

#### Why It Matters
Without channel context, the LLM might generate responses like "Please contact hr@company.com for assistance"—when the user already emailed hr@company.com. This creates confusion and appears incompetent.

#### How It Works

```ruby
ch_ctx = input['channel_context']
if ch_ctx.is_a?(Hash)
  mailbox = ch_ctx['mailbox_address'].to_s.strip  # e.g., "hr@company.com"
  role = ch_ctx['mailbox_role'].to_s.strip        # e.g., "HR shared mailbox"
  channel = ch_ctx['channel_name'].to_s.strip     # e.g., "HR Helpdesk"

  if !mailbox.empty? || !role.empty?
    channel_text = <<~CH
      CHANNEL CONTEXT:
      - You are responding on behalf of #{role}#{mailbox.empty? ? '' : " (#{mailbox})"}.
      - The employee has already contacted this mailbox.
      - Never tell them to "email this mailbox" or "contact this email address" – they already did.
      - When referring to this channel, say "reply to this email" or "our HR/support team" instead of repeating the address.
    CH

    system_preamble = [system_preamble, channel_text].compact.join("\n\n")
  end
end
```

#### Industry Context
This is a specific instance of **Context Injection** for persona management. Customer service chatbots, support systems, and virtual assistants all need similar channel awareness to avoid breaking immersion.

---

## 4. Google Cloud API Integration

### 4.1 API Error Handling

#### Description
Extract structured error information from Google API responses to enable intelligent retry and debugging.

#### When to Apply
- For all Google Cloud API calls
- When implementing retry logic
- When building error reporting/alerting

#### Why It Matters
Google APIs return rich error information in a structured format. Extracting this information enables:
- Intelligent retry (only retry transient errors)
- Clear error messages for debugging
- Field-level validation error reporting

#### How It Works

```ruby
extract_google_error: lambda do |err|
  begin
    body = (err.respond_to?(:[]) && err.dig('response','body')).to_s
    json = (JSON.parse(body) rescue nil)
    
    if json && json['error']
      # google.rpc.Status shape
      details = json['error']['details'] || []
      
      # Extract field violations from BadRequest
      bad_request = details.find { |d| 
        (d['@type'] || '').end_with?('google.rpc.BadRequest') 
      } || {}
      
      violations = (bad_request['fieldViolations'] || []).map do |v|
        {
          'field' => v['field'] || v['fieldPath'],
          'reason' => v['description'] || v['message']
        }.compact
      end
      
      return {
        'code' => json['error']['code'],
        'message' => json['error']['message'],
        'details' => details,
        'violations' => violations,
        'raw' => json
      }
    end
  rescue
  end
  {}
end
```

**Retry Strategy:**
```ruby
# Retry only transient errors
RETRYABLE_CODES = [408, 429, 500, 502, 503, 504]

retry_on_response: RETRYABLE_CODES,
max_retries: 3
```

**Error Code Interpretation:**

| Code | Meaning | Retry? |
|------|---------|--------|
| 400 | Bad Request | No (fix input) |
| 401 | Unauthenticated | Yes (refresh token) |
| 403 | Permission Denied | No (check IAM) |
| 404 | Not Found | No (check resource) |
| 408 | Timeout | Yes |
| 429 | Rate Limited | Yes (with backoff) |
| 500 | Internal Error | Yes |
| 503 | Service Unavailable | Yes |

#### Industry Context
This follows Google's recommended error handling patterns. The structured error format (google.rpc.Status) is consistent across Google Cloud APIs, making generic extraction viable.

---

### 4.2 Vertex AI RAG API Specifics

#### Description
Key implementation details for the Vertex AI RAG Engine API.

#### Endpoint Structure
```
# Retrieve contexts
POST https://{location}-aiplatform.googleapis.com/v1/projects/{project}/locations/{location}:retrieveContexts

# Embedding
POST https://{location}-aiplatform.googleapis.com/v1/projects/{project}/locations/{location}/publishers/google/models/{model}:predict

# Generation
POST https://{location}-aiplatform.googleapis.com/v1/projects/{project}/locations/{location}/publishers/google/models/{model}:generateContent

# Count tokens
POST https://{location}-aiplatform.googleapis.com/v1/projects/{project}/locations/{location}/publishers/google/models/{model}:countTokens
```

#### Critical Header: x-goog-request-params
```ruby
# CORRECT format
headers['x-goog-request-params'] = "parent=projects/#{project}/locations/#{location}"

# INCORRECT format (common mistake)
headers['x-goog-request-params'] = "ranking_config=projects/#{project}/..."
```

#### Response Structure
```ruby
# RAG retrieval response - note the double nesting
response = {
  'contexts' => {
    'contexts' => [
      {
        'chunkId' => 'chunk-123',
        'text' => 'Document content...',
        'score' => 0.87,
        'sourceUri' => 'gs://bucket/file.pdf',
        'metadata' => { ... }
      }
    ]
  }
}

# Access: response['contexts']['contexts']
```

#### Batch Size Limits
| API | Limit | Notes |
|-----|-------|-------|
| Embedding (text-embedding-005) | 250 instances | Batch for efficiency |
| Embedding (gemini-embedding-001) | 1 instance | No batching |
| Discovery Engine Ranking | 100-200 records | Varies by config |
| RAG retrieval topK | 1-200 | Default 10 |

---

### 4.3 Multi-Region URL Building

#### Description
Construct correct API URLs based on regional and multi-regional location requirements.

#### When to Apply
- For all Google Cloud API calls
- When supporting multiple regions
- When certain APIs require specific location types

#### How It Works

**Regional vs Multi-Regional:**
```ruby
# Vertex AI uses regional locations
# us-central1, europe-west1, asia-northeast1

# Discovery Engine / AI Applications uses multi-regions
# global, us, eu

# Map regional to multi-regional
region_to_aiapps_loc: lambda do |raw|
  v = raw.to_s.strip.downcase
  return 'global' if v.empty? || v == 'global'
  return 'us' if v.start_with?('us-')
  return 'eu' if v.start_with?('eu-') || v.start_with?('europe-')
  'global'  # Safe fallback
end
```

**Dynamic Host Selection:**
```ruby
aipl_service_host: lambda do |connection, loc=nil|
  l = (loc || connection['location']).to_s.downcase
  (l.blank? || l == 'global') ? 
    'aiplatform.googleapis.com' : 
    "#{l}-aiplatform.googleapis.com"
end

# Results:
# global    → aiplatform.googleapis.com
# us-central1 → us-central1-aiplatform.googleapis.com
```

**Model Path Construction:**
```ruby
build_model_path_with_global_preview: lambda do |connection, model|
  loc = (connection['location'].presence || 'global').to_s.downcase
  
  # Handle various input formats
  m = model.to_s.strip
  m = m.split('/', 2).last if m.start_with?('models/')
  
  if m.start_with?('publishers/')
    "projects/#{connection['project_id']}/locations/#{loc}/#{m}"
  else
    "projects/#{connection['project_id']}/locations/#{loc}/publishers/google/models/#{m}"
  end
end
```

---

## 5. LLM Schema Compliance

### 5.1 The Schema Compliance Problem

#### Description
LLMs frequently violate JSON schema constraints, making schema compliance the #1 failure mode in production LLM systems.

#### Common Violations
- Adding markdown formatting (```json blocks)
- Including explanatory text before/after JSON
- Omitting required fields
- Using incorrect types (string instead of number)
- Adding fields not in schema
- Incorrect enum values

#### Why It Matters
Every schema violation requires error handling, retries, or fallbacks. High violation rates mean:
- Increased latency from retries
- Higher costs from additional API calls
- More complex error handling code
- Degraded user experience

#### Mitigation Strategies
The solution requires a multi-pronged approach: schema design, prompt engineering, and code-side validation.

---

### 5.2 Schema-Side Enhancements

#### Description
Design schemas that guide the LLM toward compliance.

#### When to Apply
- When defining any JSON response schema
- When experiencing high violation rates
- When adding new LLM-based actions

#### How It Works

**Enhanced Schema Example:**
```ruby
response_schema = {
  'type' => 'object',
  'additionalProperties' => false,  # CRITICAL: Reject extra fields
  'properties' => {
    'decision' => {
      'type' => 'string',
      'enum' => ['IRRELEVANT', 'HUMAN', 'KEEP'],  # Explicit allowed values
      'description' => 'Classification decision'
    },
    'confidence' => {
      'type' => 'number',
      'minimum' => 0,
      'maximum' => 1,  # Bounded range
      'description' => 'Confidence score between 0 and 1'
    },
    'reasons' => {
      'type' => 'array',
      'items' => { 'type' => 'string' },
      'maxItems' => 3,  # Prevent verbose outputs
      'description' => 'Up to 3 brief reasons'
    }
  },
  'required' => ['decision', 'confidence']  # Explicit requirements
}
```

**Key Schema Techniques:**
| Technique | Purpose | Example |
|-----------|---------|---------|
| `additionalProperties: false` | Prevent extra fields | Catches hallucinated fields |
| `enum` | Constrain allowed values | `['KEEP', 'HUMAN', 'IRRELEVANT']` |
| `minimum/maximum` | Bound numeric ranges | `confidence: 0-1` |
| `maxItems/maxLength` | Limit output size | Prevent verbose arrays |
| `required` | Ensure critical fields | `['decision', 'confidence']` |
| `description` | Guide LLM behavior | Inline documentation |

---

### 5.3 Prompt Engineering for Compliance

#### Description
Structure prompts to maximize schema compliance through repetition, emphasis, and explicit instruction.

#### Key Techniques

**1. Repeat Critical Instructions (3+ times):**
```ruby
system_prompt = <<~PROMPT
  You are a classifier. Output MUST be valid JSON only.
  
  CRITICAL: Your entire response must be a single JSON object.
  Do NOT include any text before or after the JSON.
  Do NOT use markdown code fences.
  
  ... (task instructions) ...
  
  REMINDER: Output valid JSON only. No prose, no markdown, no explanation.
  The first character of your response MUST be '{'.
  The last character of your response MUST be '}'.
PROMPT
```

**2. Use Capital Letters for Emphasis:**
```
Output MUST be valid JSON only.
Do NOT include markdown.
NEVER add explanatory text.
```

**3. Specify Character-Level Rules:**
```
The first character must be `{`
The last character must be `}`
Do not include ``` anywhere in your response
```

**4. Provide Negative Examples:**
```
WRONG (includes markdown):
```json
{"decision": "KEEP"}
```

WRONG (includes explanation):
Based on my analysis, the answer is {"decision": "KEEP"}

CORRECT:
{"decision": "KEEP", "confidence": 0.85}
```

**5. Self-Verification Instruction:**
```
Before outputting, verify:
1. Response starts with '{'
2. Response ends with '}'
3. All required fields are present
4. Values match schema types
```

---

### 5.4 Code-Side Validation

#### Description
Implement defensive parsing and validation to handle LLM non-compliance gracefully.

#### When to Apply
- For all LLM response parsing
- When schema compliance is critical
- When fallbacks are acceptable

#### How It Works

**JSON Parsing with Cleanup:**
```ruby
json_parse_safe: lambda do |raw, type: nil, required: false, allow_wrapper: false|
  # Handle pre-parsed objects
  return raw if raw.is_a?(Hash) || raw.is_a?(Array)
  
  s = raw.to_s.strip
  
  # Strip markdown fences (common LLM artifact)
  s = s.gsub(/```json\s*/i, '').gsub(/```\s*$/, '').strip
  
  # Strip leading prose (another common artifact)
  if s !~ /\A[\[{]/
    # Try to find JSON start
    json_start = s.index(/[\[{]/)
    s = s[json_start..-1] if json_start
  end
  
  begin
    parsed = JSON.parse(s)
  rescue JSON::ParserError => e
    error("Invalid JSON: #{e.message.split(':').first}")
  end
  
  # Type validation
  case type
  when :hash, :object
    error("Expected object, got #{parsed.class}") unless parsed.is_a?(Hash)
  when :array
    # Handle wrapper objects like {categories: [...]}
    if allow_wrapper && parsed.is_a?(Hash)
      wrapped = parsed['categories'] || parsed['items'] || parsed['data']
      return wrapped if wrapped.is_a?(Array)
    end
    error("Expected array, got #{parsed.class}") unless parsed.is_a?(Array)
  end
  
  parsed
end
```

**Schema Validation Helper:**
```ruby
validate_against_schema: lambda do |parsed, schema|
  errors = []
  
  # Check required fields
  (schema['required'] || []).each do |field|
    errors << "Missing required field: #{field}" unless parsed.key?(field)
  end
  
  # Validate types and constraints
  (schema['properties'] || {}).each do |field, spec|
    value = parsed[field]
    next if value.nil? && !schema['required']&.include?(field)
    
    case spec['type']
    when 'string'
      unless value.is_a?(String)
        # Attempt coercion
        parsed[field] = value.to_s
      end
      if spec['enum'] && !spec['enum'].include?(parsed[field])
        # Use first enum value as default
        parsed[field] = spec['enum'].first
        errors << "Invalid enum value for #{field}, using default"
      end
    when 'number'
      unless value.is_a?(Numeric)
        parsed[field] = value.to_f rescue 0.0
      end
      if spec['minimum'] && parsed[field] < spec['minimum']
        parsed[field] = spec['minimum']
      end
      if spec['maximum'] && parsed[field] > spec['maximum']
        parsed[field] = spec['maximum']
      end
    when 'boolean'
      parsed[field] = !!value unless [true, false].include?(value)
    when 'array'
      parsed[field] = [value].compact unless value.is_a?(Array)
      if spec['maxItems'] && parsed[field].length > spec['maxItems']
        parsed[field] = parsed[field].first(spec['maxItems'])
      end
    end
  end
  
  { 'validated' => parsed, 'errors' => errors }
end
```

**Retry with Error Feedback:**
```ruby
def llm_call_with_retry(prompt, schema, max_retries: 2)
  retries = 0
  last_error = nil
  
  while retries <= max_retries
    response = call_llm(prompt)
    
    begin
      parsed = json_parse_safe(response, type: :hash)
      validated = validate_against_schema(parsed, schema)
      return validated['validated'] if validated['errors'].empty?
      
      # Retry with error feedback
      prompt = "#{prompt}\n\nYour previous response had errors: #{validated['errors'].join(', ')}. Please fix and try again."
      last_error = validated['errors']
    rescue => e
      last_error = e.message
    end
    
    retries += 1
  end
  
  # Return safe fallback
  build_fallback_response(schema, last_error)
end
```

---

### 5.5 Generation Config for Determinism

#### Description
Configure generation parameters to maximize consistency and schema compliance.

#### Recommended Settings for Structured Output

```ruby
generation_config = {
  'temperature' => 0,           # Most deterministic
  'topK' => 1,                  # Single best token
  'topP' => 0.1,                # Narrow probability mass
  'candidateCount' => 1,        # Single response
  'responseMimeType' => 'application/json',
  'responseSchema' => schema    # Enforced schema
}
```

**Parameter Effects:**

| Parameter | Low Value Effect | High Value Effect |
|-----------|------------------|-------------------|
| temperature | Deterministic, focused | Creative, varied |
| topK | Fewer token choices | More token choices |
| topP | Narrow distribution | Broader distribution |
| candidateCount | Single response | Multiple options |

**When to Adjust:**
- **Classification/extraction**: temperature=0, topK=1
- **Creative writing**: temperature=0.7-1.0
- **Summarization**: temperature=0.3-0.5
- **Structured output**: Always use responseMimeType + responseSchema

---

## 6. Response Generation

### 6.1 Grounding Requirements

#### Description
Responses must be based ONLY on provided context—no general knowledge, no assumptions, no fabrication.

#### When to Apply
- For all RAG-based response generation
- When accuracy is more important than completeness
- When responses have legal or policy implications

#### Why It Matters
Ungrounded responses:
- May contain inaccurate information
- Cannot be traced to sources
- May conflict with actual policies
- Create liability concerns

#### How It Works

**Grounding Preamble:**
```ruby
system_prompt = <<~PROMPT
  You are a helpful HR assistant. Your responses must follow these rules:
  
  GROUNDING REQUIREMENTS:
  1. Use ONLY information from the provided context chunks
  2. If the context doesn't contain the answer, say so explicitly
  3. NEVER use general knowledge about HR policies
  4. NEVER assume or extrapolate beyond the context
  5. NEVER promise to take actions (submit, process, update)
  
  VALIDATION CHECKLIST:
  - Is every claim supported by a specific context chunk?
  - Have I cited the source for each piece of information?
  - Am I avoiding assumptions about missing information?
  
  If context is insufficient, output:
  {
    "answer": "I don't have enough information to fully answer this question...",
    "insufficient_context": true,
    "missing_info": ["what specific information is missing"]
  }
PROMPT
```

**Citation Tracking:**
```ruby
response_schema = {
  'type' => 'object',
  'properties' => {
    'answer' => { 'type' => 'string' },
    'citations' => {
      'type' => 'array',
      'items' => {
        'type' => 'object',
        'properties' => {
          'chunk_id' => { 'type' => 'string' },
          'source' => { 'type' => 'string' },
          'uri' => { 'type' => 'string' },
          'score' => { 'type' => 'number' }
        }
      }
    },
    'insufficient_context' => { 'type' => 'boolean' },
    'missing_info' => { 
      'type' => 'array', 
      'items' => { 'type' => 'string' } 
    }
  },
  'required' => ['answer']
}
```

---

### 6.2 Acknowledgment Without Commitment

#### Description
Responses should acknowledge receipt and provide information, never promise to perform actions.

#### When to Apply
- For all automated HR/support responses
- When the system cannot actually perform the requested action
- When human review/approval is required

#### Why It Matters
If the system says "I'll process your PTO request" but cannot actually do so, trust is broken. Responses are drafts for human approval, not executed actions.

#### Forbidden Phrases

| Don't Say | Do Say |
|-----------|--------|
| "I'll process your request" | "Your request has been received" |
| "I've submitted the form" | "The next step would be to submit via [system]" |
| "I'll update your records" | "To update your records, you would need to..." |
| "I've created a ticket" | "This may need a support ticket" |
| "Your changes have been saved" | "Here's how to save these changes..." |

#### Implementation
```ruby
acknowledgment_instructions = <<~INST
  CRITICAL - ACTION LANGUAGE RULES:
  
  NEVER use these action verbs:
  - "I'll process", "I'll submit", "I'll update", "I'll create"
  - "I've submitted", "I've processed", "I've updated"
  - "Your request has been processed/submitted/approved"
  
  ALWAYS use these acknowledgment phrases:
  - "Your question has been received"
  - "Based on the information provided..."
  - "The policy states that..."
  - "The next step would be to..."
  - "You can [action] by [method]"
  
  Remember: You provide information and guidance, not action execution.
INST
```

---

### 6.3 Context Sufficiency Validation

#### Description
Validate that retrieved context is sufficient to answer the specific query type.

#### When to Apply
- Before generating any response
- When certain query types require specific information
- When incomplete answers would be misleading

#### How It Works

**Category-Specific Requirements:**

| Category | Required Context |
|----------|------------------|
| PTO | Accrual rate, balance, approval process |
| W-2 | Tax year, delivery method, timeline |
| Benefits | Plan type, coverage details, enrollment dates |
| Payroll | Pay schedule, direct deposit info |

**Validation Logic:**
```ruby
def validate_context_sufficiency(category, contexts, query)
  requirements = CATEGORY_REQUIREMENTS[category] || []
  
  missing = requirements.select do |req|
    # Check if any context mentions this requirement
    contexts.none? { |c| context_covers_requirement?(c, req) }
  end
  
  {
    'sufficient' => missing.empty?,
    'missing' => missing,
    'confidence_adjustment' => missing.length * -0.1  # Reduce confidence
  }
end
```

---

## 7. Telemetry & Observability

### 7.1 Standardized Telemetry Pattern

#### Description
Every action follows the same telemetry lifecycle: step_begin!, step_ok!/step_err!, with consistent output structure.

#### When to Apply
- For all action implementations
- For any operation that needs observability
- When building debugging/monitoring capabilities

#### Why It Matters
Consistent telemetry enables:
- Unified log analysis
- Cross-action correlation
- Automated alerting
- Performance monitoring

#### How It Works

**Lifecycle Methods:**

```ruby
# 1. BEGIN: Capture start state
step_begin!: lambda do |action_id, input|
  { 
    'action' => action_id.to_s, 
    'started_at' => Time.now.utc.iso8601,
    't0' => Time.now,  # For duration calculation
    'cid' => call(:ensure_correlation_id!, input)
  }
end

# 2. SUCCESS: Merge results with telemetry
step_ok!: lambda do |ctx, result, code=200, msg='OK', extras=nil|
  env = call(:telemetry_envelope, ctx['t0'], ctx['cid'], true, code, msg)
  out = (result || {}).merge(env)
  
  # Bridge to standard outputs
  out['op_correlation_id'] ||= out.dig('telemetry', 'correlation_id')
  out['op_telemetry'] ||= out['telemetry']
  
  # Build complete output with facets
  out = call(:build_complete_output, out, ctx['action'], (extras || {}))
  
  # Attach local log entry
  call(:local_log_attach!, out,
    call(:local_log_entry, ctx['action'], ctx['started_at'], ctx['t0'], out, nil, extras))
end

# 3. ERROR: Structured error with Google-specific extraction
step_err!: lambda do |ctx, err|
  g = call(:extract_google_error, err)
  msg = [err.to_s, g['message']].compact.join(' | ')
  code = call(:telemetry_parse_error_code, err)
  env = call(:telemetry_envelope, ctx['t0'], ctx['cid'], false, code, msg)
  
  call(:local_log_attach!, env,
    call(:local_log_entry, ctx['action'], ctx['started_at'], ctx['t0'], nil, err, { 'google_error' => g }))
  
  error(env)
end
```

**Telemetry Envelope Structure:**
```ruby
{
  'ok' => true,
  'telemetry' => {
    'http_status' => 200,
    'message' => 'OK',
    'duration_ms' => 142,
    'correlation_id' => 'a1b2c3d4-e5f6-7890-abcd-ef1234567890',
    'facets' => { ... }  # Action-specific metrics
  }
}
```

---

### 7.2 Correlation IDs

#### Description
Thread a unique identifier through the entire pipeline for log stitching and request tracing.

#### When to Apply
- For all multi-step pipelines
- When logs need to be correlated across services
- When debugging production issues

#### How It Works

```ruby
# Generate at pipeline start
build_correlation_id: lambda do
  SecureRandom.uuid
end

# Ensure exists (use provided or generate)
ensure_correlation_id!: lambda do |input|
  cid = input['correlation_id']
  (cid.is_a?(String) && cid.strip != '') ? cid.strip : SecureRandom.uuid
end

# Pass to every action
execute: lambda do |connection, input|
  ctx = call(:step_begin!, :action_name, input)
  # ctx['cid'] contains the correlation ID
  
  # Include in all API calls
  headers = call(:request_headers_auth, connection, ctx['cid'], ...)
  
  # Include in all outputs
  call(:step_ok!, ctx, result, 200, 'OK', extras)
end
```

**Usage Pattern in Recipes:**
```
# Pipeline start: generate correlation ID
correlation_id = SecureRandom.uuid

# Pass to all actions
deterministic_filter(email: email, correlation_id: correlation_id)
  → ai_triage_filter(email_text: text, correlation_id: correlation_id)
    → embed_text_against_categories(..., correlation_id: correlation_id)
      → ...

# All logs can be queried by correlation_id
```

---

### 7.3 Facets for Analytics

#### Description
Compute analytics-ready metrics at each pipeline stage for monitoring and alerting.

#### When to Apply
- When building dashboards
- When setting up alerting
- When analyzing pipeline performance

#### Why It Matters
Facets provide pre-computed, consistent metrics without parsing full payloads. They enable:
- Real-time monitoring dashboards
- Anomaly detection
- Performance trending
- Business metrics tracking

#### How It Works

**Action-Specific Facets:**

```ruby
compute_facets_for!: lambda do |action_id, out, extras = {}|
  case action_id.to_s
  when 'deterministic_filter'
    {
      'hard_blocked' => extras['hard_blocked'],
      'email_type' => extras['email_type'],
      'soft_score' => extras['soft_score'],
      'rules_evaluated' => extras['rules_evaluated'],
      'threshold_preset' => extras['threshold_preset']
    }
    
  when 'rag_retrieve_contexts_enhanced'
    {
      'top_k' => extras.dig('retrieval', 'top_k'),
      'contexts_count' => extras.dig('retrieval', 'contexts_count'),
      'success_count' => extras.dig('retrieval', 'success_count'),
      'error_count' => extras.dig('retrieval', 'error_count'),
      'partial_failure' => extras.dig('retrieval', 'partial_failure'),
      'network_error' => extras['network_error'].present?
    }
    
  when 'gen_generate'
    {
      'mode' => extras['mode'],
      'model' => extras['model'],
      'finish_reason' => call(:_facet_finish_reason, out),
      'confidence' => out['confidence'],
      'has_citations' => out.dig('parsed', 'citations')&.any?,
      'signals_used' => extras['applied_signals']&.any?,
      'tokens_prompt' => out.dig('usageMetadata', 'promptTokenCount'),
      'tokens_total' => out.dig('usageMetadata', 'totalTokenCount')
    }
  end
end
```

**Key Facets by Action:**

| Action | Key Facets | Alerting Use |
|--------|-----------|--------------|
| deterministic_filter | hard_blocked, email_type, soft_score | High block rate |
| ai_triage_filter | decision, confidence, should_continue | Low confidence trend |
| rag_retrieve_contexts | contexts_count, error_count, partial_failure | Retrieval failures |
| gen_generate | confidence, tokens_total, safety_blocked | Cost monitoring, safety events |

---

### 7.4 Local Logging

#### Description
Attach structured log entries to action outputs for debugging and audit trails.

#### How It Works

```ruby
local_log_entry: lambda do |action_id, started_at, t0, result=nil, err=nil, extras=nil|
  now = Time.now
  {
    'ts' => now.utc.iso8601,
    'action' => action_id.to_s,
    'started_at' => started_at,
    'ended_at' => now.utc.iso8601,
    'latency_ms' => ((now.to_f - t0.to_f) * 1000).round,
    'status' => err ? 'error' : 'ok',
    'correlation' => result&.dig('telemetry', 'correlation_id'),
    'http_status' => result&.dig('telemetry', 'http_status'),
    'message' => result&.dig('telemetry', 'message'),
    'error_class' => err&.class&.to_s,
    'error_msg' => err&.message&.to_s&.[](0, 512),
    'extras' => extras
  }.compact
end

local_log_attach!: lambda do |container, entry|
  begin
    tel = (container['telemetry'] ||= {})
    arr = (tel['local_logs'] ||= [])
    arr << entry if entry.is_a?(Hash) && !entry.empty?
  rescue
    # Never fail from logging
  end
  container
end
```

---

## 8. Authentication & Security

### 8.1 JWT-Based OAuth with Caching

#### Description
Use service account JWT to obtain OAuth tokens, with intelligent caching to minimize token refreshes.

#### When to Apply
- For all Google Cloud API authentication
- When using service accounts
- When API call frequency is high

#### Why It Matters
Token generation is an API call with latency and rate limits. Caching reduces:
- API latency (skip token exchange)
- Rate limit consumption
- Authentication failures from concurrent requests

#### How It Works

**Token Cache Structure:**
```ruby
# Cache keyed by scope
connection['__token_cache'] = {
  'scope_key_1' => {
    'access_token' => '...',
    'expires_at' => '2024-12-08T12:00:00Z',
    'expires_in' => 3600
  },
  'scope_key_2' => { ... }
}
```

**Cache Management:**
```ruby
auth_token_cache_get: lambda do |connection, scope_key|
  cache = (connection['__token_cache'] ||= {})
  tok = cache[scope_key]
  
  return nil unless tok.is_a?(Hash) && 
                    tok['access_token'].present? && 
                    tok['expires_at'].present?
  
  exp = Time.parse(tok['expires_at']) rescue nil
  
  # Return nil if expired or within 60-second buffer
  return nil unless exp && Time.now < (exp - 60)
  
  tok
end

auth_build_access_token!: lambda do |connection, scopes: nil|
  set = call(:auth_normalize_scopes, scopes)
  scope_key = set.join(' ')
  
  # Check cache first
  if (cached = call(:auth_token_cache_get, connection, scope_key))
    return cached['access_token']
  end
  
  # Mint new token
  fresh = call(:auth_issue_token!, connection, set)
  call(:auth_token_cache_put, connection, scope_key, fresh)['access_token']
end
```

**JWT Construction:**
```ruby
auth_issue_token!: lambda do |connection, scopes|
  key = JSON.parse(connection['service_account_key_json'])
  pk = key['private_key'].gsub(/\\n/, "\n")  # Normalize newlines
  
  now = Time.now.to_i
  payload = {
    iss: key['client_email'],
    scope: scopes.join(' '),
    aud: 'https://oauth2.googleapis.com/token',
    iat: now,
    exp: now + 3600
  }
  
  assertion = call(:jwt_sign_rs256, payload, pk)
  
  res = post('https://oauth2.googleapis.com/token')
          .payload(
            grant_type: 'urn:ietf:params:oauth:grant-type:jwt-bearer',
            assertion: assertion
          )
          .request_format_www_form_urlencoded
  
  {
    'access_token' => res['access_token'],
    'expires_at' => (Time.now + res['expires_in'].to_i).utc.iso8601,
    'scope_key' => scopes.join(' ')
  }
end
```

---

### 8.2 Header Construction

#### Description
Build consistent, correct headers for all API calls including authentication, correlation, and routing.

#### How It Works

```ruby
request_headers_auth: lambda do |connection, correlation_id=nil, user_project=nil, request_params=nil|
  token = call(:auth_build_access_token!, connection)
  
  h = {
    'Authorization' => "Bearer #{token}",
    'Content-Type' => 'application/json',
    'Accept' => 'application/json',
    'X-Correlation-Id' => correlation_id.to_s
  }
  
  # Optional: User project for billing/quota
  up = user_project.to_s.strip
  h['x-goog-user-project'] = up unless up.empty?
  
  # Optional: Request routing params
  rp = request_params.to_s.strip
  h['x-goog-request-params'] = rp unless rp.empty?
  
  h
end
```

**Header Usage by Context:**

| Context | Required Headers |
|---------|-----------------|
| All calls | Authorization, Content-Type, X-Correlation-Id |
| Quota billing | + x-goog-user-project |
| RAG retrieval | + x-goog-request-params (parent=...) |
| Model calls | + x-goog-request-params (model=...) |

---

## 9. Error Handling & Resilience

### 9.1 Graceful Failure Patterns

#### Description
Allow partial success rather than complete pipeline failures when possible.

#### When to Apply
- When processing batches of items
- When some failures are acceptable
- When total failure is worse than partial results

#### How It Works

**Partial Success Tracking:**
```ruby
# Processing contexts with partial failure handling
contexts = []
success_count = 0
error_count = 0

raw_contexts.each_with_index do |ctx_item, idx|
  begin
    processed = process_context(ctx_item)
    contexts << processed
    success_count += 1
  rescue => e
    error_count += 1
    
    case on_error_behavior
    when 'fail'
      raise e  # Fail entire operation
    when 'include'
      # Include error placeholder
      contexts << {
        'id' => "ctx-#{idx}-error",
        'text' => "[Error: #{e.message[0..200]}]",
        'processing_error' => true
      }
    when 'skip'
      next  # Skip this item
    end
  end
end

# Report partial failure in facets
facets = {
  'contexts_count' => contexts.length,
  'success_count' => success_count,
  'error_count' => error_count,
  'partial_failure' => error_count > 0
}
```

**Empty Response Handling:**
```ruby
if raw_contexts.empty?
  case empty_response_behavior
  when 'error'
    error('No contexts retrieved')
  when 'placeholder'
    contexts << {
      'id' => 'no-results',
      'text' => 'No contexts were returned for this query.',
      'score' => 0.0
    }
  when 'empty'
    # Return empty array (caller handles)
  end
end
```

---

### 9.2 Retry Strategies

#### Description
Implement intelligent retry for transient failures.

#### How It Works

**Workato Retry Configuration:**
```ruby
{
  retry_on_request: ['GET', 'HEAD'],  # Idempotent methods
  retry_on_response: [408, 429, 500, 502, 503, 504],  # Transient errors
  max_retries: 3
}
```

**Custom Retry Logic:**
```ruby
def call_with_retry(operation, max_attempts: 3)
  attempts = 0
  last_error = nil
  
  while attempts < max_attempts
    begin
      return operation.call
    rescue => e
      last_error = e
      code = extract_error_code(e)
      
      # Don't retry client errors (except 429)
      break if code >= 400 && code < 500 && code != 429
      
      attempts += 1
      
      if attempts < max_attempts
        # Exponential backoff
        sleep_time = (2 ** attempts) + rand(0.0..1.0)
        sleep(sleep_time)
      end
    end
  end
  
  raise last_error
end
```

---

### 9.3 Safe Fallback Responses

#### Description
When validation fails, provide safe default responses rather than errors.

#### How It Works

```ruby
build_fallback_response: lambda do |schema, error_context|
  fallback = {}
  
  (schema['properties'] || {}).each do |field, spec|
    fallback[field] = case spec['type']
    when 'string'
      spec['enum']&.first || ''
    when 'number'
      spec['default'] || 0.0
    when 'boolean'
      spec['default'] || false
    when 'array'
      []
    when 'object'
      {}
    end
  end
  
  # Mark as fallback for debugging
  fallback['_fallback'] = true
  fallback['_fallback_reason'] = error_context
  
  fallback
end
```

---

## 10. Configuration Management

### 10.1 Configuration Sources

#### Description
Use the right storage mechanism for different types of configuration.

#### Configuration Types and Storage

| Config Type | Storage | Update Frequency | Example |
|-------------|---------|------------------|---------|
| Static schemas | Inline JSON | Never | Response schemas |
| Safety rules | GCS with caching | Monthly | Block patterns |
| Category definitions | GCS with caching | Weekly | Category list |
| Feature flags | Firestore | Real-time | A/B tests |
| Thresholds | GCS + inline override | Per-test | Confidence thresholds |

#### GCS with Caching Pattern

```ruby
CONFIG_CACHE = {}
CACHE_TTL = 300  # 5 minutes

def load_config(bucket, path)
  cache_key = "#{bucket}/#{path}"
  cached = CONFIG_CACHE[cache_key]
  
  if cached && Time.now < cached[:expires_at]
    return cached[:data]
  end
  
  # Fetch from GCS
  data = gcs_client.get_object(bucket, path)
  parsed = JSON.parse(data)
  
  CONFIG_CACHE[cache_key] = {
    data: parsed,
    expires_at: Time.now + CACHE_TTL
  }
  
  parsed
end
```

---

### 10.2 Preset Architecture

#### Description
Use preset configurations for common use cases, with custom override capability.

#### When to Apply
- When users have predictable configuration needs
- When simplicity matters more than flexibility
- When you want sensible defaults

#### How It Works

**Threshold Presets:**
```ruby
config_threshold_presets: lambda do
  {
    'conservative' => {
      'label' => 'Conservative (Higher confidence required)',
      'values' => {
        'min_confidence_for_keep' => 0.75,
        'confidence_short_circuit' => 0.90,
        'min_confidence' => 0.40
      }
    },
    'balanced' => {
      'label' => 'Balanced (Default settings)',
      'values' => {
        'min_confidence_for_keep' => 0.60,
        'confidence_short_circuit' => 0.85,
        'min_confidence' => 0.25
      }
    },
    'aggressive' => {
      'label' => 'Aggressive (Lower confidence accepted)',
      'values' => {
        'min_confidence_for_keep' => 0.45,
        'confidence_short_circuit' => 0.75,
        'min_confidence' => 0.15
      }
    }
  }
end

# Resolution logic
resolve_thresholds: lambda do |input|
  preset = input['threshold_preset'] || 'balanced'
  
  if preset == 'custom' && input['custom_thresholds']
    input['custom_thresholds']
  else
    presets = call(:config_threshold_presets)
    presets[preset]['values']
  end
end
```

**Intent Presets by Domain:**
```ruby
get_intent_preset: lambda do |preset_type|
  presets = {
    'hr_intents' => [
      { 'intent' => 'information_request', 'actionable' => true },
      { 'intent' => 'action_request', 'actionable' => true },
      { 'intent' => 'complaint', 'actionable' => false },
      # ...
    ],
    'it_intents' => [...],
    'customer_intents' => [...],
    'sales_intents' => [...]
  }
  
  presets[preset_type] || presets['hr_intents']
end
```

---

## 11. UI/UX Patterns (Workato-Specific)

### 11.1 Conditional Field Visibility

#### Description
Show/hide fields based on other field values for cleaner UIs.

#### How It Works

**extends_schema for Dynamic Fields:**
```ruby
# Toggle triggers schema re-evaluation
{ name: 'show_advanced', 
  label: 'Show advanced options',
  type: 'boolean', 
  control_type: 'checkbox',
  extends_schema: true }  # <-- Key property

# Fields that appear when toggle is true
extended_fields: lambda do |connection|
  if connection['show_advanced'] == 'true'
    [
      { name: 'advanced_field_1', ... },
      { name: 'advanced_field_2', ... }
    ]
  else
    []
  end
end
```

**ngIf for Conditional Display:**
```ruby
# Field visible only when condition is true
{ name: 'custom_retrieval', 
  type: 'object',
  ngIf: 'input.retrieval_preset == "custom"',  # <-- Condition
  properties: [
    { name: 'top_k', type: 'integer' },
    { name: 'threshold', type: 'number' }
  ]
}

# Nested conditions
{ name: 'semantic_model',
  ngIf: 'input.ranking_config.use_ranking && input.ranking_config.ranking_type == "semantic"' }
```

---

### 11.2 Toggle Fields

#### Description
Allow switching between selection (pick list) and free-text input.

#### How It Works

```ruby
{ name: 'exclude_patterns', 
  label: 'Quick exclusions',
  control_type: 'multiselect',
  pick_list: 'common_email_patterns',
  toggle_hint: 'Select from list',
  toggle_field: {
    name: 'exclude_patterns',
    label: 'Pattern codes',
    type: 'string',
    control_type: 'text',
    toggle_hint: 'Use custom value',
    hint: 'Comma-separated pattern codes'
  }
}
```

---

### 11.3 Input Normalization

#### Description
Handle various input formats (strings, hashes, datapills) consistently.

#### Why It Matters
Workato datapills can produce different data types depending on source. Robust normalization prevents mysterious failures.

#### How It Works

```ruby
normalize_model_identifier: lambda do |raw|
  return '' if raw.nil? || raw == true || raw == false
  
  if raw.is_a?(Hash)
    # Try common keys from various datapill sources
    v = raw['value'] || raw[:value] || 
        raw['id'] || raw[:id] || 
        raw['path'] || raw[:path] || 
        raw['name'] || raw[:name] || 
        raw.to_s
    return v.to_s.strip
  end
  
  raw.to_s.strip
end

# Boolean normalization
normalize_boolean: lambda do |v|
  %w[true 1 yes on].include?(v.to_s.strip.downcase)
end

# Hash normalization (handle JSON strings)
sanitize_hash: lambda do |v|
  v.is_a?(Hash) ? v : (v.is_a?(String) ? (JSON.parse(v) rescue nil) : nil)
end
```

---

## 12. Performance Optimization

### 12.1 Token Budget Management

#### Description
Optimize context selection to maximize information within token limits.

#### How It Works

**Binary Search for Optimal Context Count:**
```ruby
select_prefix_by_budget: lambda do |connection, ordered_items, question, sys, budget, model|
  # Exponential ramp to find upper bound
  lo = 0
  hi = [1, ordered_items.length].min
  
  while hi <= ordered_items.length && tokens_fit?(hi)
    lo = hi
    hi = [hi * 2, ordered_items.length].min
    break if hi == lo
  end
  
  # Binary search within bounds: O(log n) countTokens calls
  while lo < hi
    mid = (lo + hi + 1) / 2
    if tokens_fit?(mid)
      lo = mid
    else
      hi = mid - 1
    end
  end
  
  ordered_items.first(lo)
end
```

---

### 12.2 Diversity Ordering (MMR)

#### Description
Balance relevance with diversity to avoid redundant context.

#### How It Works

```ruby
mmr_diverse_order: lambda do |items, alpha: 0.7, per_source_cap: 3|
  pool = items.map { |c| c.merge('_tokens' => tokenize(c['text'])) }
  kept = []
  kept_by_source = Hash.new(0)
  
  while pool.any?
    best = nil
    best_score = -Float::INFINITY
    
    pool.each do |cand|
      # Enforce source diversity cap
      src = cand['source'].to_s
      next if per_source_cap && kept_by_source[src] >= per_source_cap
      
      # Calculate max overlap with kept items
      max_overlap = kept.map { |k| 
        jaccard_similarity(cand['_tokens'], k['_tokens']) 
      }.max || 0.0
      
      # MMR score: relevance - redundancy
      score = alpha * cand['score'].to_f - (1.0 - alpha) * max_overlap
      
      if score > best_score
        best = cand
        best_score = score
      end
    end
    
    break unless best
    
    kept << best
    kept_by_source[best['source'].to_s] += 1
    pool.delete(best)
  end
  
  kept.each { |c| c.delete('_tokens') }
  kept
end
```

---

### 12.3 Near-Duplicate Removal

#### Description
Remove contexts that are too similar to already-selected ones.

#### How It Works

```ruby
drop_near_duplicates: lambda do |items, jaccard_threshold=0.9|
  kept = []
  
  items.each do |c|
    tokens = tokenize(c['text'])
    
    # Check similarity against kept items
    is_duplicate = kept.any? { |k| 
      jaccard_similarity(tokens, k['_tokens']) >= jaccard_threshold 
    }
    
    unless is_duplicate
      c = c.dup
      c['_tokens'] = tokens
      kept << c
    end
  end
  
  kept.each { |k| k.delete('_tokens') }
  kept
end
```

---

## 13. Testing & Debugging

### 13.1 Debugging Methodology

#### Description
Systematic approach to isolating and resolving issues.

#### Process

1. **Isolate the Component**
   - Identify which pipeline stage is failing
   - Use correlation_id to find all related logs
   - Check facets for anomalies

2. **Check Inputs**
   - Validate input format and types
   - Check for missing required fields
   - Verify normalization is working

3. **Examine API Responses**
   - Enable debug mode for request/response logging
   - Check for Google-specific error details
   - Verify response structure matches expectations

4. **Trace Dependencies**
   - Map helper method call chains
   - Check for silent error swallowing
   - Verify data transformations

5. **Test in Isolation**
   - Create minimal reproduction case
   - Test with known-good inputs
   - Compare against working examples

---

### 13.2 Common Debug Patterns

**Enable Debug Output:**
```ruby
{ name: 'debug', 
  type: 'boolean', 
  control_type: 'checkbox',
  hint: 'Adds request/response preview to output' }

# In execute:
if input['debug'] && !connection['prod_mode']
  out['debug'] = {
    'request_url' => url,
    'request_body' => redact_secrets(payload),
    'response_preview' => response.to_s[0..1000]
  }
end
```

**Facet Monitoring:**
```ruby
# Key facets to monitor for issues
alerts = []

if facets['error_count'] > 0
  alerts << "Partial failure: #{facets['error_count']} contexts failed"
end

if facets['confidence'] < 0.5
  alerts << "Low confidence: #{facets['confidence']}"
end

if facets['contexts_count'] == 0
  alerts << "No contexts retrieved"
end
```

---

### 13.3 Testing Strategies

**Unit Testing Actions:**
- Test each action in isolation
- Use known inputs with expected outputs
- Test error paths explicitly

**Integration Testing:**
- Test full pipeline with real API calls
- Use dedicated test corpus
- Verify correlation_id threading

**Edge Case Testing:**
- Empty inputs
- Maximum size inputs
- Malformed inputs
- API timeout simulation

**Regression Testing:**
- Track LLM output changes over time
- Version control test cases
- Alert on significant changes

---

## Appendix A: Key File Paths

| Purpose | Path |
|---------|------|
| Configuration storage | `gs://your-bucket/hr-configs/` |
| Category definitions | `gs://your-bucket/hr-configs/categories/v1/definitions.json` |
| Safety rules | `gs://your-bucket/hr-configs/safety_rules/v1/rules.json` |
| Document staging | `gs://hr-docs-staging/` |
| Processed documents | `gs://hr-docs-processed/{category}/` |

---

## Appendix B: Quick Reference

### Action Pipeline Order
1. `deterministic_filter` - Rule-based filtering
2. `ai_triage_filter` - LLM triage
3. `ai_intent_classifier` - Intent detection
4. `embed_text_against_categories` - Semantic similarity
5. `rerank_shortlist` - Probability distribution
6. `llm_referee_with_contexts` - Final category selection
7. `rag_retrieve_contexts_enhanced` - Context retrieval
8. `rank_texts_with_ranking_api` - Context reranking
9. `gen_generate` - Response generation

### Standard Output Fields
- `ok` (boolean) - Success indicator
- `telemetry` (object) - http_status, message, duration_ms, correlation_id, facets
- `complete_output` (object) - Business data snapshot
- `facets` (object) - Analytics-ready metrics
- `op_correlation_id` - Tracking ID
- `op_telemetry` - Telemetry bridge

### Signal Fields
- `signals_category` - Detected/chosen category
- `signals_confidence` - Confidence score (0-1)
- `signals_intent` - User intent classification
- `signals_triage` - Triage decision (IRRELEVANT/HUMAN/KEEP)
- `signals_domain` - Detected topic/domain

---

## Document History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | Dec 2024 | Emily Cabaniss | Initial comprehensive document |

---

*This document represents collective learnings from building a production HR email automation system. Patterns and approaches should be adapted to specific use cases and organizational requirements.*
