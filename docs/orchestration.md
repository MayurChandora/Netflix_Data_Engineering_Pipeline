# Orchestration

The project uses **Lakeflow Jobs** to orchestrate the Bronze, Silver, and Gold pipelines.

## Workflow

```text
Bronze Ingestion
       ↓
Silver Transformations
       ↓
Gold Analytics
```

## Tasks

| Task | Pipeline | Dependency |
|---|---|---|
| `bronze_ingestion` | Bronze pipeline | — |
| `silver_transformations` | Silver pipeline | Bronze |
| `gold_analytics_pipeline` | Gold pipeline | Silver |

## Execution

The workflow ensures that:

1. Bronze runs first.
2. Silver runs after Bronze completes.
3. Gold runs after Silver completes.

The workflow was tested with multiple source batches and successfully processed the data through all three layers.
