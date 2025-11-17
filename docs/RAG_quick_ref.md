# Vertex AI Connector - Quick Reference Guide

## Pipeline Flow Diagram
```
Email Input
    ↓
[1] Deterministic Filter (Rule-based screening)
    ↓ (if passed = true)
[2] AI Triage Filter (IRRELEVANT/HUMAN/KEEP decision)
    ↓ (if decision = KEEP)
[3] AI Intent Classifier (Categorization)
    ↓
[4] RAG Retrieve Contexts (Knowledge base search)
    ↓
[5] Rank Texts (Relevance scoring)
    ↓
[6] LLM Referee (Automation decision)
    ↓
[7] Generate Response (Final output)
```

## Quick Decision Matrix

| Stage | Decision | Next Action | Use Case |
|-------|----------|-------------|----------|
| Deterministic Filter | hard_block = true | Stop pipeline | Newsletters, chains, auto-replies |
| AI Triage | IRRELEVANT | Stop/Archive | Spam, marketing |
| AI Triage | HUMAN | Route to agent | Complaints, sensitive issues |
| AI Triage | KEEP | Continue pipeline | Standard requests |
| LLM Referee | AUTOMATE | Generate response | Clear policy match |
| LLM Referee | HUMAN | Escalate | Complex/ambiguous |

## Common Configuration Patterns

### Basic Email Processing
```json
{
  "email": {
    "subject": "PTO Request",
    "body": "I need to take time off next week",
    "from": "employee@company.com"
  },
  "op_correlation_id": "req-123"
}
```

### With Advanced Filtering
```json
{
  "email": { /* email content */ },
  "use_advanced_rules": true,
  "rules_mode": "rows",
  "rules_rows": [
    {
      "field": "subject",
      "operator": "contains",
      "value": "URGENT",
      "action": "hard_block"
    }
  ]
}
```

### RAG-Enhanced Generation
```json
{
  "mode": "rag_with_context",
  "model": "gemini-2.0-flash",
  "email_text": "What is the PTO policy?",
  "retrieved_contexts": [ /* from RAG retrieve */ ],
  "context_limit": 5,
  "use_signal_enrichment": true
}
```

## Model Selection Guide

| Model | Speed | Quality | Cost | Best For |
|-------|-------|---------|------|----------|
| gemini-2.0-flash | Fast | Good | Low | Triage, classification |
| gemini-1.5-flash | Fast | Good | Low | General tasks |
| gemini-1.5-pro | Moderate | Excellent | Medium | Complex reasoning |
| gemini-2.0-pro | Slow | Best | High | Critical decisions |

## Confidence Thresholds

### Recommended Settings
- **Triage Keep Threshold**: 0.60 (default)
- **Short Circuit**: 0.85 (high confidence IRRELEVANT)
- **Intent Match**: 0.70 (category classification)
- **RAG Relevance**: 0.50 (vector similarity)
- **Automation Decision**: 0.75 (referee threshold)

## Signal Propagation Cheat Sheet

| Upstream Action | Signal Field | Downstream Usage |
|-----------------|--------------|------------------|
| AI Triage | signals_triage | Affects generation tone |
| AI Triage | signals_confidence | Adjusts response certainty |
| Intent Classifier | signals_category | Focuses RAG retrieval |
| Intent Classifier | signals_intent | Customizes response style |

## Common Error Patterns & Solutions

| Error | Cause | Solution |
|-------|-------|----------|
| `Token limit exceeded` | Too many contexts | Reduce context_limit |
| `Invalid corpus ID` | Wrong corpus format | Check corpus_id format |
| `Safety blocking` | Sensitive content | Adjust safety settings |
| `Low confidence scores` | Poor category match | Improve category examples |
| `Timeout errors` | Model overload | Switch to faster model |

## Performance Tips

### Speed Optimization
1. **Use gemini-2.0-flash** for time-sensitive operations
2. **Limit contexts** to 5-10 for generation
3. **Enable production mode** to skip debug logs
4. **Set temperature to 0** for deterministic results

### Cost Optimization
1. **Filter early** with deterministic rules
2. **Use embeddings** for high-volume classification
3. **Cache frequent queries** externally
4. **Batch similar requests** when possible

### Quality Optimization
1. **Provide examples** in category definitions
2. **Use signal enrichment** for context
3. **Set appropriate thresholds** per use case
4. **Include system preambles** for guidance

## Monitoring Checklist

### Key Metrics
- [ ] Average latency per stage
- [ ] Decision distribution (KEEP/HUMAN/IRRELEVANT)
- [ ] Token usage by model
- [ ] Error rate by stage
- [ ] Confidence score distribution

### Health Indicators
- [ ] API response times < 2s
- [ ] Success rate > 95%
- [ ] No repeated auth failures
- [ ] Correlation IDs properly tracked

## Testing Patterns

### Unit Test: Single Action
```ruby
# Test deterministic filter
input = {
  "email": {
    "subject": "Test",
    "body": "Test content"
  }
}
output = actions.deterministic_filter.execute(connection, input)
assert output["passed"] == true
```

### Integration Test: Pipeline
```ruby
# Test full pipeline
correlation_id = "test-#{Time.now.to_i}"
result = run_pipeline(email, correlation_id)
assert result["final_response"].present?
```

## Troubleshooting Flowchart

```
Problem occurs
    ↓
Check correlation_id in logs
    ↓
Identify failing stage
    ↓
Is it auth error? → Refresh token
    ↓ No
Is it timeout? → Switch to faster model
    ↓ No
Is it safety block? → Review content/settings
    ↓ No
Is it low confidence? → Improve prompts/examples
    ↓ No
Check error_details in response
```

## Quick Command Reference

### Get Connection Status
```ruby
test_connection = connection.test
```

### Force Token Refresh
```ruby
connection["__token_cache"] = {}
```

### Enable Debug Mode
```ruby
connection["prod_mode"] = false
```

### Check Token Usage
```ruby
output["usageMetadata"]["totalTokenCount"]
```

## Field Naming Patterns

### Input Conventions
- `op_*`: Operational/telemetry fields
- `signals_*`: Upstream context fields
- `use_*`: Feature toggle booleans
- `*_config`: Configuration objects

### Output Conventions
- `*_metadata`: API response metadata
- `facets`: Telemetry/metrics object
- `complete_output`: Full API response
- `op_telemetry`: Timing/correlation data

## Support Resources

- **API Documentation**: https://cloud.google.com/vertex-ai/docs
- **Workato Community**: https://support.workato.com
- **Vertex AI Console**: https://console.cloud.google.com/vertex-ai
- **Connector Version**: 1.0.3
- **Author**: Emily Cabaniss
