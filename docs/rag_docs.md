# Vertex AI RAG Engine Connector Documentation
**Version:** 1.0.3  
**Author:** Emily Cabaniss

## Overview
This Workato custom connector integrates with Google Cloud's Vertex AI platform to provide RAG (Retrieval-Augmented Generation) capabilities for automated email processing and intelligent workflow automation. The connector implements a sophisticated 8-stage pipeline designed for enterprise email triage, particularly focused on HR and IT help desk automation.

## Connection Configuration

### Required Fields
- **Service Account Key**: GCP service account JSON key with necessary Vertex AI permissions
- **GCP Project ID**: Target Google Cloud project identifier
- **GCP Region**: Deployment region (e.g., us-central1, us-east4)
- **Discovery API Version**: v1alpha (default), v1beta, or v1

### Advanced Options (Optional)
- **Production Mode**: Suppresses debug output and enforces strict retry rules
- **User Project**: Override project for quota/billing attribution
- **Enable Facets Logging**: Adds telemetry metrics to logs for monitoring

## Actions

---

## 1. Filter: Heuristics (with intent)
**Action Name:** `deterministic_filter`  
**Pipeline Position:** Step 1 of 8  
**Display Priority:** 510

### Business Purpose
Pre-screens incoming emails using deterministic rules to quickly eliminate non-actionable messages before expensive AI processing. This first-line defense filters out automated responses, newsletters, email chains, and other irrelevant content.

### Inputs
| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| email | object | Yes | - | Email envelope containing subject, body, from address |
| exclude_patterns | array | No | - | Quick exclusion patterns for filtering |
| use_advanced_rules | boolean | No | false | Enable advanced rule engine |
| rules_mode | select | No | - | 'json' or 'rows' for advanced rules |
| rules_json | text | No | - | JSON rulepack definition |
| rules_rows | array | No | - | Tabular rule definitions |
| op_correlation_id | string | No | - | Pipeline tracking ID |

### Outputs
| Field | Type | Description |
|-------|------|-------------|
| passed | boolean | True if email passes all hard rules |
| hard_block | boolean | True if email is definitively blocked |
| hard_reason | string | Reason for blocking (e.g., 'forwarded_chain', 'safety_block') |
| email_type | string | Classification: 'direct_request', 'forwarded_chain', 'automated', 'newsletter' |
| email_text | string | Normalized email content |
| soft_score | integer | Soft signals score (0-10) |
| gate | object | Pipeline control object for conditional routing |
| excluded_pattern | string | Specific pattern that caused exclusion |

### Error Conditions
- Invalid JSON in rules_json
- Malformed email envelope
- Missing required fields in email object

### Mechanism of Action
1. Normalizes email envelope extracting subject, body, and metadata
2. Applies rule precedence: advanced rules > quick patterns > defaults
3. Evaluates hard rules (blocking conditions)
4. If not blocked, calculates soft signals score
5. Determines preliminary routing decision (KEEP/HUMAN/IRRELEVANT)
6. Returns gate object for pipeline control

---

## 2. Filter: AI Triage
**Action Name:** `ai_triage_filter`  
**Pipeline Position:** Step 2 of 8  
**Display Priority:** 501

### Business Purpose
Uses LLM-based classification to intelligently triage emails into three categories: IRRELEVANT (spam/newsletters), HUMAN (needs manual review for sensitive matters), or KEEP (can be automated). This provides nuanced filtering that rules alone cannot achieve.

### Inputs
| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| email_text | string | Yes | - | Normalized email content |
| email_type | string | No | 'direct_request' | Type from deterministic filter |
| system_preamble | text | No | - | Override system instructions |
| min_confidence_for_keep | number | No | 0.60 | Minimum confidence threshold |
| confidence_short_circuit | number | No | 0.85 | High-confidence bypass threshold |
| model | string | No | 'gemini-2.0-flash' | AI model selection |
| temperature | number | No | 0 | Model temperature |

### Outputs
| Field | Type | Description |
|-------|------|-------------|
| decision | string | IRRELEVANT, HUMAN, or KEEP |
| confidence | number | Confidence score (0-1) |
| reasons | array | List of decision reasons |
| matched_signals | array | Detected signal patterns |
| should_continue | boolean | Pipeline continuation flag |
| short_circuit | boolean | High-confidence early exit |
| signals_triage | string | Decision copy for downstream |
| signals_confidence | number | Confidence copy for downstream |

### Error Conditions
- Model endpoint unavailable
- Invalid response format from LLM
- Timeout during inference
- Safety blocking by model

### Mechanism of Action
1. Constructs specialized triage prompt with system instructions
2. Sends email content to selected Gemini model
3. Parses structured JSON response for decision and confidence
4. Applies confidence thresholds for routing decisions
5. Sets pipeline control flags based on decision type
6. Propagates signals for downstream enhancement

---

## 3. AI Intent Classifier
**Action Name:** `ai_intent_classifier`  
**Pipeline Position:** Step 3 of 8  
**Display Priority:** 500

### Business Purpose
Performs detailed intent classification for emails that passed triage, identifying specific request types (e.g., PTO request, benefits inquiry, password reset) with confidence scoring and hierarchical categorization.

### Inputs
| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| email_text | string | Yes | - | Email content to classify |
| categories | array | Yes | - | Category definitions with examples |
| strict_mode | boolean | No | false | Require exact category matching |
| model | string | No | 'gemini-2.0-flash' | Classification model |
| include_examples | boolean | No | false | Include examples in prompt |
| primary_only | boolean | No | false | Return only top category |

### Outputs
| Field | Type | Description |
|-------|------|-------------|
| primary | object | Top-scoring intent category |
| secondary | object | Second-best match |
| all_scores | array | All category confidence scores |
| matched | boolean | True if confident match found |
| confidence | number | Primary category confidence |
| signals_category | string | Category for downstream |
| signals_intent | string | Intent classification |

### Error Conditions
- No categories provided
- Invalid category format
- Model classification failure
- Below threshold confidence

### Mechanism of Action
1. Builds prompt with category definitions and examples
2. Uses LLM to analyze email against all categories
3. Extracts confidence scores for each category
4. Applies strict mode validation if enabled
5. Ranks categories by confidence
6. Returns hierarchical classification results

---

## 4. Embed Text Against Categories
**Action Name:** `embed_text_against_categories`  
**Pipeline Position:** Step 4 of 8 (optional)  
**Display Priority:** 499

### Business Purpose
Creates semantic embeddings of email content and compares against predefined category embeddings to identify best matches using cosine similarity. Provides fast, scalable classification without LLM calls.

### Inputs
| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| email_text | string | Yes | - | Text to embed |
| categories | array | Yes | - | Categories with descriptions |
| embedding_model | string | No | 'text-embedding-004' | Embedding model |
| top_k | integer | No | 5 | Number of results |
| similarity_threshold | number | No | 0.75 | Min similarity score |

### Outputs
| Field | Type | Description |
|-------|------|-------------|
| embedding | array | Email text embedding vector |
| category_embeddings | array | Category embedding vectors |
| similarities | array | Ranked similarity scores |
| best_match | object | Top matching category |
| above_threshold | array | Matches above threshold |

### Error Conditions
- Embedding API failure
- Invalid text input
- Category format errors
- Model unavailable

### Mechanism of Action
1. Generates embeddings for email text
2. Retrieves or generates category embeddings
3. Calculates cosine similarity scores
4. Ranks categories by similarity
5. Applies threshold filtering
6. Returns semantic matching results

---

## 5. Rerank Shortlist
**Action Name:** `rerank_shortlist`  
**Pipeline Position:** Step 5 of 8  
**Display Priority:** 498

### Business Purpose
Reranks a shortlist of candidate responses or documents using advanced ranking models to ensure the most relevant content appears first, critical for accurate RAG response generation.

### Inputs
| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| query | string | Yes | - | Query text for ranking |
| items | array | Yes | - | Items to rank (id, title, content) |
| model | string | No | 'semantic-ranker-512' | Ranking model |
| top_n | integer | No | 10 | Results to return |
| boost_recent | boolean | No | false | Boost recent items |

### Outputs
| Field | Type | Description |
|-------|------|-------------|
| ranked_items | array | Reranked items with scores |
| ranking_scores | array | Detailed ranking scores |
| model_used | string | Actual model used |
| items_reranked | integer | Number of items processed |

### Error Conditions
- Empty items array
- Ranking API unavailable
- Invalid item format
- Model not found

### Mechanism of Action
1. Prepares items for ranking API
2. Calls Discovery Engine ranking endpoint
3. Processes ranking scores
4. Applies boost factors if enabled
5. Sorts items by final scores
6. Returns reordered results

---

## 6. LLM Referee with Contexts
**Action Name:** `llm_referee_with_contexts`  
**Pipeline Position:** Step 6 of 8  
**Display Priority:** 497

### Business Purpose
Makes intelligent decisions about email routing by analyzing retrieved contexts and applying business rules. Acts as a referee between automated responses and human escalation.

### Inputs
| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| email_text | string | Yes | - | Original email |
| contexts | array | Yes | - | Retrieved RAG contexts |
| policy_rules | object | No | - | Business rules to apply |
| model | string | No | 'gemini-2.0-flash' | Decision model |
| confidence_threshold | number | No | 0.75 | Min confidence |

### Outputs
| Field | Type | Description |
|-------|------|-------------|
| decision | string | AUTOMATE, HUMAN, or DEFER |
| confidence | number | Decision confidence (0-1) |
| explanation | string | Decision reasoning |
| applicable_contexts | array | Relevant contexts used |
| policy_violations | array | Any rule violations |
| recommended_action | string | Suggested next step |

### Error Conditions
- No contexts provided
- Policy rule conflicts
- Model decision failure
- Below threshold confidence

### Mechanism of Action
1. Analyzes email against retrieved contexts
2. Evaluates policy rules and constraints
3. Uses LLM to make routing decision
4. Validates decision confidence
5. Generates explanation with citations
6. Returns structured decision output

---

## 7. RAG Retrieve Contexts Enhanced
**Action Name:** `rag_retrieve_contexts_enhanced`  
**Pipeline Position:** Core RAG operation  
**Display Priority:** 496

### Business Purpose
Retrieves relevant document chunks from Vertex AI RAG corpus using semantic search. This is the core RAG operation that finds knowledge base content to answer queries.

### Inputs
| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| corpus_id | string | Yes | - | RAG corpus identifier |
| query | string | Yes | - | Search query |
| similarity_top_k | integer | No | 10 | Results to retrieve |
| vector_distance_threshold | number | No | 0.5 | Min similarity |
| use_hybrid | boolean | No | true | Combine vector + keyword |
| metadata_filters | array | No | - | Filtering conditions |

### Outputs
| Field | Type | Description |
|-------|------|-------------|
| contexts | array | Retrieved document chunks |
| chunk_ids | array | Chunk identifiers |
| scores | array | Relevance scores |
| total_retrieved | integer | Number of chunks found |
| metadata | object | Retrieval metadata |

### Error Conditions
- Corpus not found
- Invalid query format
- RAG API unavailable
- No results above threshold

### Mechanism of Action
1. Constructs retrieval request with filters
2. Calls Vertex AI RAG retrieval endpoint
3. Processes returned contexts
4. Extracts chunk metadata and scores
5. Applies post-retrieval filtering
6. Returns ranked contexts

---

## 8. Rank Texts with Ranking API
**Action Name:** `rank_texts_with_ranking_api`  
**Pipeline Position:** Advanced ranking  
**Display Priority:** 495

### Business Purpose
Uses Google's Discovery Engine Ranking API for sophisticated relevance ranking of text passages, providing superior ranking compared to basic similarity scores.

### Inputs
| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| project_id | string | Yes | - | GCP project ID |
| ranking_config | string | Yes | - | Ranking configuration |
| query | string | Yes | - | Query for ranking |
| records | array | Yes | - | Records to rank |
| boost_spec | object | No | - | Boost configurations |

### Outputs
| Field | Type | Description |
|-------|------|-------------|
| ranked_records | array | Records ordered by relevance |
| scores | array | Detailed ranking scores |
| ranking_metadata | object | API response metadata |

### Error Conditions
- Invalid ranking config
- Discovery Engine API error
- Malformed records
- Project permissions

### Mechanism of Action
1. Formats records for Discovery Engine
2. Applies boost specifications
3. Calls ranking API endpoint
4. Processes ranking scores
5. Reorders records by relevance
6. Returns ranked results

---

## 9. Generate Content
**Action Name:** `gen_generate`  
**Pipeline Position:** Final generation  
**Display Priority:** 494

### Business Purpose
Generates final responses using LLMs with optional RAG context integration. Supports multiple modes including plain generation, RAG-enhanced, and structured output.

### Inputs
| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| mode | select | Yes | 'plain' | Generation mode |
| model | string | Yes | - | Model identifier |
| contents | array | Conditional | - | Conversation history |
| system_preamble | text | No | - | System instructions |
| signals_category | string | No | - | Upstream category |
| signals_confidence | number | No | - | Upstream confidence |
| signals_intent | string | No | - | Upstream intent |
| use_signal_enrichment | boolean | No | true | Apply upstream signals |
| generation_config | object | No | - | Generation parameters |
| safetySettings | array | No | - | Safety configurations |

#### Mode-Specific Inputs
**RAG with Context Mode:**
| Field | Type | Required | Description |
|-------|------|----------|-------------|
| email_text | string | Yes | Original query |
| retrieved_contexts | array | Yes | RAG contexts |
| context_limit | integer | No | Max contexts to use |

### Outputs
| Field | Type | Description |
|-------|------|-------------|
| text | string | Generated text response |
| usage | object | Token usage statistics |
| safety_ratings | array | Content safety scores |
| grounding_metadata | object | Citation information |
| finish_reason | string | Generation stop reason |
| model_version | string | Model version used |

### Error Conditions
- Model endpoint unavailable
- Safety blocking
- Token limit exceeded
- Invalid generation config
- Context processing failure

### Mechanism of Action
1. Selects generation strategy based on mode
2. Enriches prompt with upstream signals if enabled
3. Formats contexts for RAG mode
4. Constructs model request with configurations
5. Handles streaming or batch response
6. Extracts and validates generated content
7. Returns formatted response with metadata

---

## Pipeline Architecture

### Typical Email Processing Flow
```
1. Deterministic Filter → 
2. AI Triage Filter → 
3. AI Intent Classifier → 
4. RAG Retrieve Contexts → 
5. Rank Texts → 
6. LLM Referee → 
7. Generate Response
```

### Conditional Branching
The pipeline supports conditional execution based on confidence scores and decision gates:
- High-confidence IRRELEVANT emails exit early
- HUMAN decisions route to manual queues
- KEEP decisions continue through automation

### Signal Propagation
Upstream actions pass signals downstream through special fields:
- `signals_category`: Category classification
- `signals_confidence`: Confidence scores
- `signals_intent`: Intent determination

These signals enhance downstream processing quality by providing context.

## Best Practices

### Performance Optimization
1. Use deterministic filtering to reduce AI processing costs
2. Set appropriate confidence thresholds for your use case
3. Enable production mode to suppress debug output
4. Use correlation IDs to track requests across pipeline stages

### Error Handling
1. All actions include comprehensive error catching and reporting
2. Use `op_telemetry` fields for monitoring and debugging
3. Check `facets` output for detailed metrics
4. Implement retry logic for transient failures

### Cost Management
1. Monitor token usage through usage metadata
2. Adjust `context_limit` to control RAG costs
3. Use embedding-based classification for high-volume scenarios
4. Implement caching for frequently accessed contexts

### Security Considerations
1. Service account requires minimal Vertex AI permissions
2. Use `user_project` for proper billing attribution
3. Enable safety settings appropriate for your domain
4. Implement rate limiting for production deployments

## Monitoring and Observability

### Key Metrics to Track
- Pipeline stage latencies (`latency_ms`)
- Decision distribution (KEEP/HUMAN/IRRELEVANT)
- Confidence score distributions
- Token usage by model
- Error rates by stage

### Logging
Enable facets logging for detailed telemetry:
```json
{
  "correlation_id": "request-123",
  "stage": "ai_triage",
  "latency_ms": 1250,
  "decision": "KEEP",
  "confidence": 0.82
}
```

## Troubleshooting Guide

### Common Issues and Solutions

**Issue:** High latency in generation
**Solution:** Reduce context_limit or switch to faster model (gemini-2.0-flash)

**Issue:** Low confidence scores
**Solution:** Improve category definitions with more examples

**Issue:** Frequent safety blocks
**Solution:** Adjust safety settings or preprocess content

**Issue:** RAG retrieval returning irrelevant contexts
**Solution:** Tune vector_distance_threshold and similarity_top_k

## Version History
- v1.0.3: Current version with enhanced error handling and signal propagation
- v1.0.2: Added advanced rule engine and embedding support
- v1.0.1: Initial release with core pipeline functionality
