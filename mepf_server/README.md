# MEPF Central Server + Orchestrator

FastAPI service implementing the Central Server + Orchestrator from your
architecture diagram, built to match your existing pyRevit extension
(MEPF.extension / FamilyExtract.extension) exactly — no extension code
changes needed. Endpoint paths/shapes match endpoint_config.py's defaults.

## What it implements

| Diagram block          | File                              |
|-------------------------|-----------------------------------|
| System Registry          | app/routers/agents.py (register/heartbeat) |
| Document Registry         | app/routers/agents.py + app/models/db.py (DocumentRecord) |
| Central Server / DB      | app/models/db.py |
| Central Server ingest    | app/routers/project.py (extraction data) |
| Processing Engine trigger| app/routers/commands.py (POST /create) |
| Routing Engine            | app/orchestrator/routing_engine.py |
| Command Manager           | app/routers/commands.py (/next, /result) |

## Endpoints (match endpoint_config.py defaults exactly)

- `POST /api/agents/register`   — called every 30s by agent_registry.py
- `POST /api/agents/heartbeat`  — called every 30s by agent_registry.py
- `POST /api/project/ingest-auto` — called by shared_sender.send_combined_payload()
- `GET  /api/commands/next?machine_id=&revit_process_id=&session_id=` — polled by fetch_worker.py
- `POST /api/commands/{command_id}/result` — posted by fetch_worker._send_command_result()

Plus operator/debug endpoints:
- `POST /api/commands/create` — create + auto-route a command (e.g. your VAV/duct placement JSON) to the correct online agent by document_id / project_uid / machine_id
- `GET  /api/agents/list` — view System Registry
- `GET  /api/commands/list` — view recent commands + status

## Run locally

```bash
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8000
```

Uses SQLite by default (`mepf.db`). Set `DATABASE_URL` env var to a
Postgres URL (e.g. from Render) to use Postgres instead — no code changes
needed, both are supported via SQLAlchemy.

## Deploy to Render

1. Push this folder to a GitHub repo.
2. On Render: New -> Blueprint -> point at the repo (render.yaml is included,
   creates the web service + a free Postgres database and wires
   DATABASE_URL automatically).
3. Once deployed, your existing extension's `endpoint_config.py`
   (`central_server_url`) should already point at
   `https://multiple-system-payload-send.onrender.com` — just make sure
   the Render service name matches, or update that URL to your actual
   deployed URL.

## Example: create + route a command with your VAV/Duct placement JSON

```bash
curl -X POST https://<your-render-url>/api/commands/create \
  -H "Content-Type: application/json" \
  -d '{
    "action": "place_mep_elements",
    "items": [ ...contents of VAV-Duct_placement.json... ],
    "target_selector": { "document_id": "..." }
  }'
```

The server finds the single matching online document from the Document
Registry, builds the routing block, stores the command as "pending" for
that machine_id. The agent's fetch_worker.py picks it up on its next poll,
smart_target.py validates every routing field, and the handlers
(handler_point_family.py / handler_curve_mep.py) place the elements.

---

## Payload folder router (added)

Drop `*.json` payloads into `payloads/inbox/` (or `POST /api/payloads/upload`). A background watcher
validates each file, resolves it to **exactly one online Revit document**, queues a command for that
document's agent, tracks it through `QUEUED -> CLAIMED -> EXECUTING -> SUCCEEDED`, and moves the file to
`payloads/done/` or `payloads/failed/`. See `payloads/routing_map.example.json` and
`payloads/inbox/payload_example.json.sample`.

| Endpoint | Purpose |
|---|---|
| `GET /api/payloads` | list payloads and their state |
| `GET /api/payloads/{id}` | one payload + event history + command |
| `POST /api/payloads/preview` | dry-run: validation + which document would be chosen |
| `POST /api/payloads/upload` | multipart file upload; validates, persists, and immediately routes one pass |
| `POST /api/payloads/scan` | run one watcher pass now |
| `POST /api/payloads/{id}/retry?force=` | re-queue (force needed if elements may already exist) |
| `POST /api/payloads/{id}/cancel` | cancel if not yet claimed |

Env: `MEPF_PAYLOAD_DIR`, `MEPF_PAYLOAD_SCAN_SEC`, `MEPF_PAYLOAD_WATCHER=0` (disable),
`MEPF_PAYLOAD_QUEUED_TTL_SEC`, `MEPF_PAYLOAD_NO_TARGET_TTL_SEC`, `COMMAND_PENDING_TTL_SEC`.
**Render note:** the web-service filesystem is ephemeral - attach a persistent disk and point
`MEPF_PAYLOAD_DIR` at it, or use `/api/payloads/upload` (payload bodies are also stored in the DB).
Tests: `python tests/test_payload_router.py`.

### Automatic routing policy

If the uploaded JSON has no `target` block and no filename/routing-map rule, the server uses the live Document Registry as the candidate set. It selects deterministically: (1) an eligible document that is not already busy when one is available, (2) lowest active workload (payload-router work plus queued/claimed/executing commands for that document), then (3) `machine_id`, `revit_process_id`, `session_id`, `revit_instance_id`, and `document_id` as stable tie-breakers. No random selection and no database row-order selection are used.

If a selector such as `project_uid` matches multiple live machines, the same deterministic policy resolves the tie. To target an exact Revit instance, prefer `document_id`; for the same project open on several machines, `agent_id + project_uid` or `machine_id + project_uid` distinguishes the physical target.

### File upload API

```bash
curl -X POST "https://<your-render-url>/api/payloads/upload" \
  -H "X-Api-Key: <MEPF_API_KEY>" \
  -F "file=@payload.json;type=application/json"
```

The upload is capped by `MEPF_PAYLOAD_MAX_UPLOAD_BYTES` (default 5,000,000 bytes). The JSON is parsed before acceptance, written atomically to `payloads/inbox/`, and then one router pass runs immediately. The normal background watcher continues the asynchronous Revit lifecycle.

The response is not a false synchronous Revit-success response: `QUEUED`, `CLAIMED`, and `EXECUTING` mean work is in progress; poll `GET /api/payloads/{payload_id}` for `SUCCEEDED`, `PARTIAL`, `EXECUTION_FAILED`, `TIMEOUT`, etc. A request with no eligible live document returns HTTP 409 with `NO_TARGET`; the payload remains persisted and can be routed when a live agent appears.
