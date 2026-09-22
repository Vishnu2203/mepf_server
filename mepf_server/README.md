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
