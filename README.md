# globa-flux-rust (LLM Gateway)

Internal backend for YouTube Revenue Agent:
- Calls OpenAI/Anthropic (provider keys never reach the browser)
- Streams responses via SSE
- Persists LLM usage + `cost_usd` into TiDB with idempotency

Implementation note: OpenAI integration uses the `async-openai` SDK.

## Endpoints

- `POST /api/chat/risk_check?stream=1` (SSE)
- `POST /api/chat/risk_check` (JSON)

## Vercel (Serverless Functions)

This repo is intended to deploy as **Vercel Serverless Functions** (not a long-running Rust server):
- Each `api/**/*.rs` file is a separate Function; the route matches the file path (e.g. `api/chat/risk_check.rs` → `/api/chat/risk_check`).
- `vercel.json` configures the Rust builder (`@vercel/rust`).
- `.vercelignore` excludes `target/` to keep deploys small.

Deploy:
- Connect the repo in Vercel, or run `vercel deploy`.
- Set the env vars below in the Vercel project settings.

### Vercel build notes (rustup / cargo)

If the Vercel build logs show `spawn cargo ENOENT`, it usually means `rustup` installed Cargo under `/root/.cargo/bin` but the build process used `HOME=/vercel` and couldn't find it.

This repo sets build-time env vars in `vercel.json` to align them:
- `HOME=/root`
- `CARGO_HOME=/root/.cargo`
- `RUSTUP_HOME=/root/.rustup`

If you still see the error, check that the Vercel project does not override these vars, then “Clear cache & redeploy”.

## Env Vars

- `RUST_INTERNAL_TOKEN` (shared secret; required)
- `TIDB_DATABASE_URL` (required for TiDB writes)
- `OPENAI_API_KEY` (required for OpenAI)
- `OPENAI_MODEL` (default: `gpt-4o-mini`)
- `OPENAI_MAX_COMPLETION_TOKENS` (default: `600`)
- `OPENAI_PROMPT_TOKEN_RESERVE` (default: `2000`, used for budget reserve precheck)
- `OPENAI_PRICE_PROMPT_USD_PER_M_TOKEN` (optional override)
- `OPENAI_PRICE_COMPLETION_USD_PER_M_TOKEN` (optional override)
- `ANTHROPIC_API_KEY` (later)
- `YOUTUBE_CLIENT_ID` (required for YouTube OAuth)
- `YOUTUBE_CLIENT_SECRET` (required for YouTube OAuth)
- `YOUTUBE_REDIRECT_URI` (required for YouTube OAuth; must match Hydrogen authorize redirect)

## Local build

Run: `cargo test`

## Notes

This repo currently contains a stub handler that matches the contract and returns SSE/JSON. Provider streaming + TiDB persistence are the next steps.
