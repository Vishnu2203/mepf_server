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
    create_engine, Column, String, Integer, Boolean, DateTime, Text, ForeignKey, JSON, Index, inspect, text as sql_text
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
    status = Column(String, default="PENDING", index=True)  # PENDING|CLAIMED|EXECUTING|SUCCEEDED|PARTIAL|FAILED|DEAD_LETTER|CANCELLED
    created_at = Column(DateTime, default=now, index=True)
    delivered_at = Column(DateTime, nullable=True)
    claimed_at = Column(DateTime, nullable=True)
    execution_started_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)
    lease_expires_at = Column(DateTime, nullable=True, index=True)
    lease_token = Column(String, nullable=True, index=True)
    lease_owner = Column(String, nullable=True)
    attempts = Column(Integer, default=0, nullable=False)
    max_attempts = Column(Integer, default=3, nullable=False)
    last_error = Column(Text, nullable=True)
    updated_at = Column(DateTime, default=now, onupdate=now)


class CommandResultRecord(Base):
    __tablename__ = "command_results"

    id = Column(Integer, primary_key=True, autoincrement=True)
    command_id = Column(String, ForeignKey("commands.command_id"), index=True)
    status = Column(String)
    routing = Column(JSON)
    result = Column(JSON)
    received_at = Column(DateTime, default=now)


class AuditEventRecord(Base):
    __tablename__ = "audit_events"

    id = Column(Integer, primary_key=True, autoincrement=True)
    command_id = Column(String, index=True, nullable=True)
    event_type = Column(String, index=True, nullable=False)
    actor = Column(String, nullable=True)
    details = Column(JSON, nullable=True)
    created_at = Column(DateTime, default=now, index=True)


class IdempotencyRecord(Base):
    __tablename__ = "idempotency_keys"

    key = Column(String, primary_key=True)
    command_id = Column(String, ForeignKey("commands.command_id"), nullable=False, index=True)
    request_hash = Column(String, nullable=True, index=True)
    created_at = Column(DateTime, default=now)


Index("ix_commands_claimable", CommandRecord.machine_id, CommandRecord.status, CommandRecord.created_at)
Index("ix_commands_lease", CommandRecord.status, CommandRecord.lease_expires_at)


def _add_missing_columns():
    """Small additive migration for installations created by older builds."""
    inspector = inspect(engine)
    existing = set(c["name"] for c in inspector.get_columns("commands")) if "commands" in inspector.get_table_names() else set()
    existing_idempotency = set(c["name"] for c in inspector.get_columns("idempotency_keys")) if "idempotency_keys" in inspector.get_table_names() else set()
    additions = {
        "status": "VARCHAR", "claimed_at": "TIMESTAMP", "execution_started_at": "TIMESTAMP",
        "completed_at": "TIMESTAMP", "lease_expires_at": "TIMESTAMP", "lease_token": "VARCHAR",
        "lease_owner": "VARCHAR", "attempts": "INTEGER DEFAULT 0", "max_attempts": "INTEGER DEFAULT 3",
        "last_error": "TEXT", "updated_at": "TIMESTAMP",
    }
    if not existing:
        return
    with engine.begin() as conn:
        for name, typ in additions.items():
            if name not in existing:
                conn.execute(sql_text("ALTER TABLE commands ADD COLUMN {} {}".format(name, typ)))
        if "request_hash" not in existing_idempotency and existing_idempotency:
            conn.execute(sql_text("ALTER TABLE idempotency_keys ADD COLUMN request_hash VARCHAR"))
        # Normalize legacy states from the pre-lease build. A previously
        # delivered command had not been durably acknowledged by Revit, so it
        # is deliberately returned to PENDING instead of being marked done.
        conn.execute(sql_text("UPDATE commands SET status='PENDING' WHERE status='pending'"))
        conn.execute(sql_text("UPDATE commands SET status='PENDING', lease_token=NULL, lease_owner=NULL, lease_expires_at=NULL WHERE status='delivered'"))
        conn.execute(sql_text("UPDATE commands SET status='SUCCEEDED' WHERE status='committed'"))
        conn.execute(sql_text("UPDATE commands SET status='FAILED' WHERE status='failed'"))
        conn.execute(sql_text("UPDATE commands SET attempts=0 WHERE attempts IS NULL"))
        conn.execute(sql_text("UPDATE commands SET max_attempts=3 WHERE max_attempts IS NULL"))


def init_db():
    Base.metadata.create_all(bind=engine)
    _add_missing_columns()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
