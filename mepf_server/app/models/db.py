# -*- coding: utf-8 -*-
"""
db.py
=============================================================================
Database layer for the Central Server + Orchestrator.

Tables map 1:1 onto your architecture diagram:

  systems       -> "System Registry"      (SYS-001 -> PC-01)
  documents     -> "Document Registry"    (DOC-001 -> Revit version -> PC -> Model)
  extractions   -> extracted payloads received from agents
  jobs          -> work created from an extraction (processing engine output)
  commands      -> routed commands sent down to a specific agent
  command_results -> results reported back by agents

Field names deliberately match what routing_identity.py / smart_target.py /
agent_registry.py / fetch_worker.py already send and expect:
  machine_id, revit_process_id, session_id, project_uid, document_id,
  document_path, document_title, revit_version, command_id
=============================================================================
"""
import os
import datetime as dt

from sqlalchemy import (
    create_engine, Column, String, Integer, Boolean, DateTime, Text, ForeignKey, JSON
)
from sqlalchemy.orm import declarative_base, sessionmaker, relationship

DATABASE_URL = os.environ.get("DATABASE_URL", "sqlite:///./mepf.db")

# Render's managed Postgres URLs start with postgres:// — SQLAlchemy needs postgresql://
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

connect_args = {"check_same_thread": False} if DATABASE_URL.startswith("sqlite") else {}
engine = create_engine(DATABASE_URL, connect_args=connect_args, pool_pre_ping=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


def now():
    return dt.datetime.utcnow()


# ============================================================
# SYSTEM REGISTRY  (SYS-001 -> PC-01)
# ============================================================
class SystemRecord(Base):
    __tablename__ = "systems"

    machine_id = Column(String, primary_key=True)   # workstation_id from agent's endpoint_config.json
    machine_name = Column(String, nullable=True)
    status = Column(String, default="online")        # online | offline
    last_seen = Column(DateTime, default=now, onupdate=now)
    first_registered = Column(DateTime, default=now)

    documents = relationship("DocumentRecord", back_populates="system")


# ============================================================
# DOCUMENT REGISTRY  (DOC-001 -> Revit 2023 -> PC-01 -> Model A)
# ============================================================
class DocumentRecord(Base):
    __tablename__ = "documents"

    document_id = Column(String, primary_key=True)   # deterministic hash from routing_identity.document_id()
    machine_id = Column(String, ForeignKey("systems.machine_id"), index=True)
    revit_instance_id = Column(String, index=True)
    revit_process_id = Column(String, nullable=True)
    session_id = Column(String, nullable=True)
    project_uid = Column(String, index=True, nullable=True)
    document_title = Column(String, nullable=True)
    document_path = Column(String, nullable=True)
    revit_version = Column(String, nullable=True)
    is_online = Column(Boolean, default=True)
    last_seen = Column(DateTime, default=now, onupdate=now)

    system = relationship("SystemRecord", back_populates="documents")


# ============================================================
# EXTRACTIONS  (data coming UP from an agent)
# ============================================================
class ExtractionRecord(Base):
    __tablename__ = "extractions"

    extraction_id = Column(String, primary_key=True)
    machine_id = Column(String, index=True, nullable=True)
    document_id = Column(String, index=True, nullable=True)
    project_uid = Column(String, index=True, nullable=True)
    payload = Column(JSON)
    received_at = Column(DateTime, default=now)


# ============================================================
# JOBS  (work created by the Processing Engine from an extraction,
#         or directly requested by an operator/UI)
# ============================================================
class JobRecord(Base):
    __tablename__ = "jobs"

    job_id = Column(String, primary_key=True)
    extraction_id = Column(String, ForeignKey("extractions.extraction_id"), nullable=True)
    job_type = Column(String, default="place_elements")
    status = Column(String, default="pending")   # pending | routed | completed | failed
    target_document_id = Column(String, nullable=True)
    target_project_uid = Column(String, nullable=True)
    created_at = Column(DateTime, default=now)


# ============================================================
# COMMANDS  (routed to one specific agent, matches receiver_verified.py's
#            expected envelope: command_id, routing{}, payload{items:[...]})
# ============================================================
class CommandRecord(Base):
    __tablename__ = "commands"

    command_id = Column(String, primary_key=True)
    job_id = Column(String, ForeignKey("jobs.job_id"), nullable=True)
    machine_id = Column(String, index=True)         # which agent should pick this up
    routing = Column(JSON)                          # the exact {routing} block smart_target.py validates
    action = Column(String, default="place_mep_elements")
    items = Column(JSON)                            # the payload.items list (VAV/duct placement data etc.)
    status = Column(String, default="pending")       # pending | delivered | committed | failed | timeout
    created_at = Column(DateTime, default=now)
    delivered_at = Column(DateTime, nullable=True)


class CommandResultRecord(Base):
    __tablename__ = "command_results"

    id = Column(Integer, primary_key=True, autoincrement=True)
    command_id = Column(String, ForeignKey("commands.command_id"), index=True)
    status = Column(String)
    routing = Column(JSON)
    result = Column(JSON)
    received_at = Column(DateTime, default=now)


def init_db():
    Base.metadata.create_all(bind=engine)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
