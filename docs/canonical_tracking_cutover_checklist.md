# Canonical MCP Tracking Cutover Checklist

## Pre-deploy
- Run `python -m py_compile` on modified modules.
- Run `python tests/test_canonical_contract.py`.
- Validate at least one data tool and one workspace tool response against canonical schema.

## Deploy
- Deploy server with canonical response contract enabled by default.
- Confirm no legacy top-level `results`/`tsv`/`fasta` sibling fields in tool responses.

## Post-deploy
- Check stderr logs for normalization boundary entries:
  - `requestId`
  - `requestCorrelationId`
  - `schema_valid`
  - `nextCursorId`
- Verify counters remain near zero:
  - `canonical_schema_validation_failures`
  - `correlation_missing_fields`
  - `pagination_contract_violations`

## Regression checks
- Global search response remains replayable via canonical `replay`.
- Paginated queries maintain stable `requestCorrelationId`.
- Workspace browse/download/metadata return canonical top-level shape.
