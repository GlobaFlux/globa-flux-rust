use bytes::Bytes;
use http_body_util::BodyExt;
use hyper::{HeaderMap, Method, StatusCode};
use serde::Deserialize;
use vercel_runtime::{run, service_fn, Error, Request, Response, ResponseBody};

use globa_flux_rust::db::{
  create_geo_monitor_project, enqueue_geo_monitor_prompt_tasks, fetch_geo_monitor_project,
  fetch_geo_monitor_run_results, fetch_geo_monitor_run_summary, fetch_latest_geo_monitor_run,
  fetch_tenant_gemini_model, get_pool, list_geo_monitor_projects, list_geo_monitor_prompts,
  replace_geo_monitor_prompts, ensure_geo_monitor_run,
};
use globa_flux_rust::geo_monitor::parse_string_list_json;

const GLOBAL_TENANT_ID: &str = "global";

fn bearer_token(header_value: Option<&str>) -> Option<&str> {
  let value = header_value?;
  value.strip_prefix("Bearer ").or_else(|| value.strip_prefix("bearer "))
}

fn json_response(status: StatusCode, value: serde_json::Value) -> Result<Response<ResponseBody>, Error> {
  Ok(
    Response::builder()
      .status(status)
      .header("content-type", "application/json; charset=utf-8")
      .body(ResponseBody::from(value))?,
  )
}

fn has_tidb_url() -> bool {
  std::env::var("TIDB_DATABASE_URL")
    .or_else(|_| std::env::var("DATABASE_URL"))
    .map(|v| !v.is_empty())
    .unwrap_or(false)
}

#[derive(Deserialize)]
struct PromptInput {
  #[serde(default)]
  theme: Option<String>,
  text: String,
}

#[derive(Deserialize)]
struct GeoMonitorRpcRequest {
  op: String,
  #[serde(default)]
  tenant_id: Option<String>,
  #[serde(default)]
  project_id: Option<i64>,
  #[serde(default)]
  name: Option<String>,
  #[serde(default)]
  website: Option<String>,
  #[serde(default)]
  brand_aliases: Option<Vec<String>>,
  #[serde(default)]
  competitors: Option<Vec<String>>,
  #[serde(default)]
  schedule: Option<String>,
  #[serde(default)]
  prompts: Option<Vec<PromptInput>>,
}

fn required_string(input: Option<String>, field: &str) -> Result<String, Error> {
  let value = input.unwrap_or_default().trim().to_string();
  if value.is_empty() {
    return Err(Box::new(std::io::Error::other(format!(
      "{field} is required"
    ))));
  }
  Ok(value)
}

async fn handle_geo_monitor(
  method: &Method,
  headers: &HeaderMap,
  body: Bytes,
) -> Result<Response<ResponseBody>, Error> {
  let expected = std::env::var("RUST_INTERNAL_TOKEN").unwrap_or_default();
  let provided = bearer_token(headers.get("authorization").and_then(|v| v.to_str().ok())).unwrap_or("");

  if expected.is_empty() || provided != expected {
    return json_response(
      StatusCode::UNAUTHORIZED,
      serde_json::json!({"ok": false, "error": "unauthorized"}),
    );
  }

  if method != Method::POST {
    return json_response(
      StatusCode::METHOD_NOT_ALLOWED,
      serde_json::json!({"ok": false, "error": "method_not_allowed"}),
    );
  }

  if !has_tidb_url() {
    return json_response(
      StatusCode::NOT_IMPLEMENTED,
      serde_json::json!({"ok": false, "error": "not_configured", "message": "Missing TIDB_DATABASE_URL (or DATABASE_URL)"}),
    );
  }

  let parsed: GeoMonitorRpcRequest = serde_json::from_slice(&body).map_err(|e| -> Error {
    Box::new(std::io::Error::other(format!("invalid json body: {e}")))
  })?;

  let tenant_id = match required_string(parsed.tenant_id, "tenant_id") {
    Ok(v) => v,
    Err(_) => {
      return json_response(
        StatusCode::BAD_REQUEST,
        serde_json::json!({"ok": false, "error": "bad_request", "message": "tenant_id is required"}),
      )
    }
  };

  let pool = get_pool().await?;

  match parsed.op.as_str() {
    "list_projects" => {
      let projects = list_geo_monitor_projects(pool, &tenant_id).await?;
      let payload = projects
        .into_iter()
        .map(|p| {
          serde_json::json!({
            "id": p.id,
            "name": p.name,
            "website": p.website,
            "schedule": p.schedule,
            "enabled": p.enabled,
            "brand_aliases": parse_string_list_json(p.brand_aliases_json.as_deref()),
            "competitors": parse_string_list_json(p.competitor_names_json.as_deref()),
          })
        })
        .collect::<Vec<_>>();

      json_response(StatusCode::OK, serde_json::json!({"ok": true, "projects": payload}))
    }

    "create_project" => {
      let name = match required_string(parsed.name, "name") {
        Ok(v) => v,
        Err(_) => {
          return json_response(
            StatusCode::BAD_REQUEST,
            serde_json::json!({"ok": false, "error": "bad_request", "message": "name is required"}),
          )
        }
      };

      let website = parsed.website.as_deref().map(str::trim).filter(|v| !v.is_empty());
      let schedule = parsed.schedule.unwrap_or_else(|| "weekly".to_string());

      let brand_aliases_json = serde_json::to_string(&parsed.brand_aliases.unwrap_or_default())
        .ok()
        .filter(|s| s != "[]");
      let competitors_json = serde_json::to_string(&parsed.competitors.unwrap_or_default())
        .ok()
        .filter(|s| s != "[]");

      let id = create_geo_monitor_project(
        pool,
        &tenant_id,
        &name,
        website,
        brand_aliases_json.as_deref(),
        competitors_json.as_deref(),
        &schedule,
      )
      .await?;

      json_response(StatusCode::OK, serde_json::json!({"ok": true, "project_id": id}))
    }

    "get_project" => {
      let project_id = parsed.project_id.unwrap_or(0);
      if project_id <= 0 {
        return json_response(
          StatusCode::BAD_REQUEST,
          serde_json::json!({"ok": false, "error": "bad_request", "message": "project_id is required"}),
        );
      }

      let project = fetch_geo_monitor_project(pool, &tenant_id, project_id).await?;
      let project = match project {
        Some(v) => v,
        None => {
          return json_response(
            StatusCode::NOT_FOUND,
            serde_json::json!({"ok": false, "error": "not_found"}),
          )
        }
      };

      let prompts = list_geo_monitor_prompts(pool, &tenant_id, project_id).await?;
      let prompts_json = prompts
        .iter()
        .map(|p| serde_json::json!({"id": p.id, "theme": p.theme, "text": p.prompt_text, "enabled": p.enabled, "sort_order": p.sort_order}))
        .collect::<Vec<_>>();

      let latest_run = fetch_latest_geo_monitor_run(pool, &tenant_id, project_id).await?;
      let run_json = if let Some(run) = latest_run {
        let summary = fetch_geo_monitor_run_summary(pool, run.id).await?;
        let results = fetch_geo_monitor_run_results(pool, run.id, 200).await?;
        serde_json::json!({
          "id": run.id,
          "run_for_dt": run.run_for_dt.to_string(),
          "status": run.status,
          "provider": run.provider,
          "model": run.model,
          "prompt_total": run.prompt_total,
          "started_at": run.started_at.to_rfc3339(),
          "finished_at": run.finished_at.map(|t| t.to_rfc3339()),
          "summary": {
            "results_total": summary.results_total,
            "presence_count": summary.presence_count,
            "top3_count": summary.top3_count,
            "top5_count": summary.top5_count,
            "error_count": summary.error_count,
            "cost_usd": summary.cost_usd
          },
          "results": results.into_iter().map(|(prompt_id, id, prompt_text, output_text, presence, rank_int, cost_usd, error)| {
            serde_json::json!({
              "id": id,
              "prompt_id": prompt_id,
              "prompt_text": prompt_text,
              "output_text": output_text,
              "presence": presence,
              "rank_int": rank_int,
              "cost_usd": cost_usd,
              "error": error
            })
          }).collect::<Vec<_>>()
        })
      } else {
        serde_json::Value::Null
      };

      json_response(
        StatusCode::OK,
        serde_json::json!({
          "ok": true,
          "project": {
            "id": project.id,
            "name": project.name,
            "website": project.website,
            "schedule": project.schedule,
            "enabled": project.enabled,
            "brand_aliases": parse_string_list_json(project.brand_aliases_json.as_deref()),
            "competitors": parse_string_list_json(project.competitor_names_json.as_deref()),
          },
          "prompts": prompts_json,
          "latest_run": run_json
        }),
      )
    }

    "set_prompts" => {
      let project_id = parsed.project_id.unwrap_or(0);
      if project_id <= 0 {
        return json_response(
          StatusCode::BAD_REQUEST,
          serde_json::json!({"ok": false, "error": "bad_request", "message": "project_id is required"}),
        );
      }

      let latest_run = fetch_latest_geo_monitor_run(pool, &tenant_id, project_id).await?;
      if let Some(run) = latest_run {
        if run.finished_at.is_none() && run.status == "running" {
          return json_response(
            StatusCode::CONFLICT,
            serde_json::json!({"ok": false, "error": "conflict", "message": "cannot modify prompts while a run is in progress"}),
          );
        }
      }

      let prompts = parsed.prompts.unwrap_or_default();
      let mut cleaned: Vec<(Option<String>, String)> = Vec::new();
      for p in prompts.into_iter() {
        let text = p.text.trim().to_string();
        if text.is_empty() {
          continue;
        }
        cleaned.push((
          p.theme.and_then(|t| {
            let t = t.trim().to_string();
            if t.is_empty() {
              None
            } else {
              Some(t)
            }
          }),
          text,
        ));
      }

      replace_geo_monitor_prompts(pool, &tenant_id, project_id, cleaned.as_slice()).await?;
      json_response(StatusCode::OK, serde_json::json!({"ok": true}))
    }

    "start_run" => {
      let project_id = parsed.project_id.unwrap_or(0);
      if project_id <= 0 {
        return json_response(
          StatusCode::BAD_REQUEST,
          serde_json::json!({"ok": false, "error": "bad_request", "message": "project_id is required"}),
        );
      }

      let project = fetch_geo_monitor_project(pool, &tenant_id, project_id).await?;
      if project.is_none() {
        return json_response(
          StatusCode::NOT_FOUND,
          serde_json::json!({"ok": false, "error": "not_found"}),
        );
      }

      let prompts = list_geo_monitor_prompts(pool, &tenant_id, project_id).await?;
      let prompt_ids: Vec<i64> = prompts.iter().filter(|p| p.enabled).map(|p| p.id).collect();
      let prompt_total = prompt_ids.len() as i32;
      if prompt_total <= 0 {
        return json_response(
          StatusCode::BAD_REQUEST,
          serde_json::json!({"ok": false, "error": "bad_request", "message": "no prompts configured"}),
        );
      }

      let db_model = fetch_tenant_gemini_model(pool, GLOBAL_TENANT_ID).await?;
      let model = match db_model {
        Some(v) if !v.trim().is_empty() => v,
        _ => {
          return json_response(
            StatusCode::NOT_IMPLEMENTED,
            serde_json::json!({"ok": false, "error": "not_configured", "message": "Missing gemini_model for tenant_id=global"}),
          )
        }
      };

      let now = chrono::Utc::now();
      let run_for_dt = now.date_naive();

      let run = ensure_geo_monitor_run(
        pool,
        &tenant_id,
        project_id,
        run_for_dt,
        "gemini",
        model.trim(),
        prompt_total,
      )
      .await?;

      let enqueued = enqueue_geo_monitor_prompt_tasks(pool, &tenant_id, project_id, run_for_dt, &prompt_ids).await?;

      json_response(
        StatusCode::OK,
        serde_json::json!({
          "ok": true,
          "run": {
            "id": run.id,
            "run_for_dt": run.run_for_dt.to_string(),
            "status": run.status,
            "provider": run.provider,
            "model": run.model,
            "prompt_total": run.prompt_total,
            "started_at": run.started_at.to_rfc3339(),
            "finished_at": run.finished_at.map(|t| t.to_rfc3339())
          },
          "enqueued_rows": enqueued
        }),
      )
    }

    other => json_response(
      StatusCode::BAD_REQUEST,
      serde_json::json!({"ok": false, "error": "bad_request", "message": format!("unknown op: {other}")}),
    ),
  }
}

async fn handler(req: Request) -> Result<Response<ResponseBody>, Error> {
  let method = req.method().clone();
  let headers = req.headers().clone();
  let bytes = req.into_body().collect().await?.to_bytes();
  handle_geo_monitor(&method, &headers, bytes).await
}

#[tokio::main]
async fn main() -> Result<(), Error> {
  run(service_fn(handler)).await
}

