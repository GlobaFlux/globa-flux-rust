# LLM Settings on Hobby Plan (DB-Only)

## Context
Vercel Hobby deployments are limited to 12 Serverless Functions. The Rust API tree currently sits at 12 functions; adding `api/tenants/llm_settings.rs` pushed the count to 13 and caused deploy failures. We still need tenant-specific Gemini model selection to be dynamic, but we cannot add another function without upgrading or splitting projects.

## Goals
- Keep tenant-level model selection working.
- Avoid adding new serverless functions.
- Preserve server-side enforcement of model choice for `api/chat/risk_check`.

## Decision
Remove the LLM settings API endpoint and manage model selection directly in TiDB. Keep the `tenant_llm_settings` table and the runtime lookup in `api/chat/risk_check` so the model choice remains dynamic without a redeploy.

## Data Flow
1. Internal ops updates `tenant_llm_settings.gemini_model` for a given `tenant_id`.
2. `api/chat/risk_check` reads the row and overrides the Gemini model before calling the provider.

## Constraints / Risks
- No admin UI or API for updates; changes are manual in DB.
- No audit trail beyond `updated_by` unless ops tooling captures it.
- Model allowlist is operational policy (enforced by ops, not by API).

## SQL (for ops)
```sql
INSERT INTO tenant_llm_settings (tenant_id, gemini_model, updated_by)
VALUES ('t1', 'gemini-1.5-flash', 'ops')
ON DUPLICATE KEY UPDATE
  gemini_model = VALUES(gemini_model),
  updated_by = VALUES(updated_by),
  updated_at = CURRENT_TIMESTAMP(3);
```

