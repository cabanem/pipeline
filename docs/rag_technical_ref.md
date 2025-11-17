# Vertex AI Connector - Technical Implementation Reference

## Architecture Overview

### Core Design Patterns
The connector implements several sophisticated patterns:

1. **Token-Aware Context Truncation**: Dynamically adjusts context based on model token limits
2. **Signal Propagation**: Passes classification signals through pipeline stages  
3. **Faceted Telemetry**: Embeds metrics in responses without affecting business outputs
4. **Cached Authentication**: Implements JWT token caching to minimize OAuth exchanges
5. **Progressive Disclosure UI**: Shows advanced options only when needed

## Key Helper Methods

### Authentication & Connection
```ruby
# JWT Bearer Token Generation
auth_build_access_token!(connection, scopes:)
- Generates and caches OAuth tokens
- Implements 1-hour cache with 60-second safety margin
- Handles service account key parsing and JWT signing

# Service Host Resolution  
aipl_service_host(connection, location)
- Maps regions to correct Vertex AI endpoints
- Handles special cases (global, EU regions)
```

### Token Management
```ruby
# Token Counting
count_tokens(connection, model, parts)
- Estimates token usage before API calls
- Supports both text and structured content

# Token-Aware Truncation
tokens_truncate_contexts(connection, model, question, contexts, budget)
- Binary search algorithm to fit maximum contexts
- Preserves complete contexts (no partial chunks)
```

### Email Processing
```ruby
# Email Normalization
norm_email_envelope!(input)
- Extracts subject, body, from, headers
- Handles multipart MIME structures
- Preserves metadata for filtering

# Rule Evaluation
hr_eval_hard_enhanced?(email_data, rules)
- Pattern matching with regex support
- Header inspection capabilities
- Attachment detection
```

### RAG Operations
```ruby
# Context Retrieval
rag_retrieve_contexts_api(connection, input)
- Constructs proper Vertex AI RAG API calls
- Handles pagination and filtering
- Extracts relevance scores

# Context Formatting
rag_format_contexts(contexts, style)
- Multiple formatting styles (xml, markdown, compact)
- Citation preservation
- Token-efficient representations
```

## Error Handling Strategies

### Structured Error Responses
All actions implement consistent error handling:
```ruby
{
  'error': true,
  'error_type': 'API_ERROR|VALIDATION_ERROR|AUTH_ERROR',
  'error_message': 'Human readable description',
  'error_details': { /* structured details */ },
  'op_telemetry': { /* timing and correlation */ }
}
```

### Retry Logic
- Auth errors (401): Automatic token refresh
- Rate limits (429): Exponential backoff
- Transient errors (503): Configurable retry

### Safety Mechanisms
- Input validation before API calls
- Response format verification
- Graceful degradation on partial failures

## Performance Optimizations

### Caching Strategies
1. **Token Cache**: 1-hour OAuth tokens with early refresh
2. **Embedding Cache**: Category embeddings reused across calls
3. **Model Path Cache**: Computed paths stored in connection

### Batching Capabilities
- Embedding requests support batch processing
- Context retrieval can fetch multiple queries
- Generation supports conversation history

### Resource Management
- Automatic context truncation to fit token budgets
- Streaming response support for large generations
- Connection pooling for HTTP requests

## Schema Management

### Dynamic Schema Extension
```ruby
extended_fields: lambda do |connection|
  connection['show_advanced'] ? advanced_fields : []
end
```
Allows progressive disclosure without schema conflicts

### Object Definition Reuse
Shared definitions for:
- content_part
- generation_config  
- safety_setting
- pipeline_gate

### Input Field Validation
- Type coercion (string to number/boolean)
- Default value injection
- Required field enforcement

## Pipeline Control Flow

### Gate Objects
Each filter stage produces a gate for routing:
```ruby
gate = {
  'prelim_pass': boolean,      # Continue pipeline?
  'hard_block': boolean,        # Immediate stop?
  'decision': 'KEEP|HUMAN|IRRELEVANT',
  'generator_hint': string,     # Downstream guidance
  'soft_score': integer        # Confidence/quality
}
```

### Signal Enhancement
Downstream actions can access upstream signals:
```ruby
if input['use_signal_enrichment']
  prompt = enhance_with_signals(
    prompt,
    category: input['signals_category'],
    confidence: input['signals_confidence'],
    intent: input['signals_intent']
  )
end
```

### Correlation Tracking
```ruby
ctx = step_begin!(action_name, input)
# Sets correlation_id, start_time, action context
result = step_ok!(ctx, output, http_status, message, facets)
# Adds telemetry, duration, correlation
```

## Advanced Features

### Custom Policy Integration
Actions support JSON policy injection:
```ruby
policy = safe_json(input['custom_policy_json'])
decision = evaluate_against_policy(email, policy)
```

### Multi-Model Support
```ruby
build_model_path_with_global_preview(connection, model_name)
- Handles gemini-* models
- Supports custom endpoints
- Preview model access
```

### Faceted Logging
Non-invasive metrics collection:
```ruby
facets = {
  'retrieval_count': contexts.length,
  'avg_score': scores.mean,
  'latency_ms': duration
}
# Logged separately from business outputs
```

## Testing Helpers

### Sample Data Generators
```ruby
sample_deterministic_filter_simplified
sample_embedding_output  
sample_rag_contexts
```
Provide realistic test data for development

### Mock Responses
Built-in mocks for:
- Token counting
- Embedding generation
- Context retrieval

## Security Considerations

### Input Sanitization
- JSON parsing with error handling
- SQL-like injection prevention in filters
- Path traversal protection in corpus IDs

### Credential Management
- Service account key never logged
- Token refresh without key exposure
- Scoped permissions per operation

### Rate Limiting
- Built-in throttling for API calls
- Configurable delays between requests
- Circuit breaker pattern for failures

## Extension Points

### Custom Actions
```ruby
custom_action: true
# Allows users to add their own Vertex AI calls
```

### Pick Lists
Extensible enumerations for:
- Models
- Regions  
- Safety levels
- Generation modes

### Webhook Support
Framework ready for:
- Async processing callbacks
- Stream processing integration
- Event-driven triggers

## Code Organization

### Method Naming Conventions
- `auth_*`: Authentication related
- `hr_*`: Heuristic rules (deterministic filter)
- `rag_*`: RAG operations
- `gen_*`: Generation utilities
- `ui_*`: UI/UX helpers
- `step_*`: Pipeline telemetry
- `norm_*`: Normalization functions

### Method Suffixes
- `!`: Mutating/important operation
- `?`: Returns boolean
- `_api`: Direct API call
- `_safe`: Error-handled version

## Debugging Aids

### Debug Mode Features
When not in production mode:
- Verbose logging of API calls
- Request/response dumps
- Timing breakdowns by operation
- Token usage reports

### Correlation ID Tracking
Every operation tagged with correlation_id for:
- Cross-stage tracing
- Error investigation
- Performance analysis
- Usage attribution

## Integration Patterns

### Workato-Specific Patterns
1. **Lazy Schema Loading**: Fields computed at runtime
2. **Extended Fields**: Progressive UI disclosure
3. **Object Definitions**: Reusable type definitions
4. **Pick Lists**: Dynamic enumeration values

### Error Recovery
1. **Graceful Degradation**: Fallback to simpler operations
2. **Partial Success**: Return what succeeded with error flags
3. **Retry with Backoff**: Automatic retry for transient failures
4. **Circuit Breaking**: Stop cascading failures

## Maintenance Guidelines

### Adding New Actions
1. Define in `actions:` hash
2. Implement input/output fields lambdas
3. Add execute lambda with error handling
4. Include sample_output for testing
5. Update help documentation

### Modifying Helper Methods
1. Check all callers for compatibility
2. Maintain backward compatibility
3. Update related tests
4. Document behavior changes

### Version Management
- Semantic versioning (major.minor.patch)
- Changelog maintenance
- Deprecation warnings for breaking changes
- Migration guides for upgrades
