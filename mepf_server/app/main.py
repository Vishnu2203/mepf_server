# -*- coding: utf-8 -*-
"""
main.py
=============================================================================
Central Server + Orchestrator, combined into one FastAPI service.

Routers map exactly onto your architecture diagram:
  agents.py   -> System Registry + Document Registry (dynamic auto-registration)
  project.py  -> Central Server ingest (extraction data with identity)
  commands.py -> Processing Engine -> Orchestrator Routing Engine -> Command Manager
                 + the poll/result loop the pyRevit extension already speaks.

Matches endpoint_config.py's defaults exactly:
  POST /api/agents/register
  POST /api/agents/heartbeat
  POST /api/project/ingest-auto
  GET  /api/commands/next
  POST /api/commands/{command_id}/result
=============================================================================
"""
from fastapi import FastAPI

from app.models.db import init_db
from app.routers import agents, project, commands

app = FastAPI(title="MEPF Central Server + Orchestrator")

app.include_router(agents.router)
app.include_router(project.router)
app.include_router(commands.router)


@app.on_event("startup")
def on_startup():
    init_db()


@app.get("/")
def root():
    return {"status": "ok", "service": "MEPF Central Server + Orchestrator"}


@app.get("/health")
def health():
    return {"status": "healthy"}
