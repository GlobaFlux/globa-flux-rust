<INSTRUCTIONS>
## Preflight (Required)
Before making ANY product/UX/engineering changes in this repo, read these docs to align with current commercial goals:
- `../docs/PLAN.md`
- `../docs/business-panorama.md`
- `../../bussiness/README.md`
- `../../bussiness/00-overview/one-page-plan.md`

If any of the above paths do not exist, stop and ask the user for the updated location.

## YouTube Metrics Workflow (Hard Standard)
Before ANY work that pulls/defines YouTube metrics (Analytics/Reporting queries, schema fields, dashboards, guardrails):
- First read: `https://developers.google.com/youtube/analytics/metrics`
- Then verify queryability for the chosen API/report type (some “metrics” exist but are not supported for a given report/dimension).

## Working Rules
- Optimize for Q1 MVP P0 release gates in `../docs/PLAN.md` (data trust → revenue guardrails → report export → sponsor quote → UX/perf).
- Prefer small, testable changes (unit tests for pure logic; avoid breaking API route count constraints).
</INSTRUCTIONS>
