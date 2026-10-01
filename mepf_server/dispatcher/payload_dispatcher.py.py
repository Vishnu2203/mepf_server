# -*- coding: utf-8 -*-
"""
Automatic server-folder -> Revit command dispatcher.

Folder naming:
    inbox/
        project_1_2022.json
        project_2_2024.json

Meaning:
    project_1_2022.json -> project_1 opened in Revit 2022
    project_2_2024.json -> project_2 opened in Revit 2024

The dispatcher:
    1. Watches the server payload inbox.
    2. Reads project + Revit version from the filename.
    3. Gets live agents/documents from the existing backend.
    4. Finds EXACTLY ONE matching online Revit document.
    5. Creates the existing /api/commands/create command.
    6. Waits for the existing command lifecycle to finish.
    7. Moves the JSON to succeeded / failed / ambiguous / invalid.

Important:
    This program should run ON THE SERVER (or on a worker with access to
    the server's payload folder), not on each Revit machine.

Recommended filename:
    project_1_2022.json

If two machines can simultaneously have the same project and same Revit
version, use an extended filename:
    project_1_2022_MACHINE_A.json
or, preferably:
    project_1_2022_<agent_id>.json

The existing backend APIs are intentionally kept:
    GET  /api/agents/list
    POST /api/commands/create
    GET  /api/commands/{command_id}
"""

import json
import os
import re
import shutil
import time
from pathlib import Path

import requests


# ============================================================
# CONFIGURATION
# ============================================================

SERVER_URL = os.getenv(
    "MEPF_SERVER_URL",
    "https://mepf-server.onrender.com",
).rstrip("/")

API_KEY = os.getenv("MEPF_API_KEY", "")

# Example on a Windows server:
INBOX_DIR = Path(os.getenv("MEPF_PAYLOAD_INBOX", r"C:\MEPF\payloads\inbox"))

# The dispatcher creates these automatically.
PROCESSING_DIR = INBOX_DIR.parent / "processing"
SUCCEEDED_DIR = INBOX_DIR.parent / "succeeded"
FAILED_DIR = INBOX_DIR.parent / "failed"
AMBIGUOUS_DIR = INBOX_DIR.parent / "ambiguous"
INVALID_DIR = INBOX_DIR.parent / "invalid"

SCAN_SECONDS = 3
STATUS_TIMEOUT_SECONDS = 600
STATUS_POLL_SECONDS = 3
MAX_ATTEMPTS = 3

TERMINAL_OK = {"SUCCEEDED"}
TERMINAL_PARTIAL = {"PARTIAL"}
TERMINAL_FAIL = {"FAILED", "DEAD_LETTER", "CANCELLED"}

# Strict filename contract:
#   project_1_2022.json
#
# Everything before the final _YEAR is treated as the project key.
FILENAME_RE = re.compile(
    r"^(?P<project_key>.+)_(?P<revit_year>20\d{2})\.json$",
    re.IGNORECASE,
)


# ============================================================
# GENERAL HELPERS
# ============================================================

def log(message):
    print(time.strftime("[%Y-%m-%d %H:%M:%S]"), message, flush=True)


def ensure_dirs():
    for directory in (
        INBOX_DIR,
        PROCESSING_DIR,
        SUCCEEDED_DIR,
        FAILED_DIR,
        AMBIGUOUS_DIR,
        INVALID_DIR,
    ):
        directory.mkdir(parents=True, exist_ok=True)


def headers():
    if not API_KEY:
        raise RuntimeError(
            "MEPF_API_KEY environment variable is not configured."
        )

    return {
        "X-API-Key": API_KEY,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


def normalized(value):
    """
    Normalization used only for matching names.

    Examples:
        Project_1.rvt       -> project1
        Project 1          -> project1
        project_1          -> project1
    """
    if value is None:
        return ""
    value = str(value).strip().lower()
    value = os.path.basename(value)

    # Remove .rvt if present.
    if value.endswith(".rvt"):
        value = value[:-4]

    # Ignore spaces, underscores, hyphens and punctuation.
    return re.sub(r"[^a-z0-9]+", "", value)


# ============================================================
# FILENAME -> TARGET
# ============================================================

def parse_filename(path):
    match = FILENAME_RE.match(path.name)
    if not match:
        raise ValueError(
            "Invalid filename '{}'. Expected project_1_2022.json".format(
                path.name
            )
        )

    return {
        "project_key": match.group("project_key"),
        "revit_year": match.group("revit_year"),
    }


# ============================================================
# BACKEND
# ============================================================

def get_agents():
    response = requests.get(
        SERVER_URL + "/api/agents/list",
        headers=headers(),
        timeout=60,
    )

    if not response.ok:
        raise RuntimeError(
            "GET /api/agents/list failed: {} {}".format(
                response.status_code,
                response.text[:1000],
            )
        )

    data = response.json()

    if isinstance(data, list):
        return data

    if isinstance(data, dict) and isinstance(data.get("agents"), list):
        return data["agents"]

    raise RuntimeError("Unexpected /api/agents/list response.")


def extract_revit_year(document):
    """
    Read the Revit version from the live document record.

    The existing agent/backend may expose one of several common field names.
    Prefer the explicit Revit version field.

    Examples:
        revit_version = "2022"
        revit_year    = 2022
    """
    candidates = (
        document.get("revit_version"),
        document.get("revit_year"),
        document.get("version"),
    )

    for value in candidates:
        if value is None:
            continue

        match = re.search(r"20\d{2}", str(value))
        if match:
            return match.group(0)

    return None


def document_project_key(document):
    """
    Determine the project/document name used for filename matching.

    The existing agent data normally contains document_title and/or
    document_path.
    """
    title = document.get("document_title")
    if title:
        return normalized(title)

    path = document.get("document_path")
    if path:
        return normalized(path)

    return ""


def find_matching_documents(agents, project_key, revit_year):
    """
    Return all ONLINE documents matching:
        project name + Revit version

    We intentionally do NOT choose between multiple matches.
    Ambiguity is moved to the ambiguous folder instead of sending to
    the wrong Revit model.
    """
    wanted_project = normalized(project_key)
    wanted_year = str(revit_year)

    matches = []

    for agent in agents:
        if str(agent.get("status", "")).lower() != "online":
            continue

        for document in agent.get("documents") or []:
            if not document.get("document_id"):
                continue

            document_year = extract_revit_year(document)

            if document_year != wanted_year:
                continue

            if document_project_key(document) != wanted_project:
                continue

            matches.append({
                "agent": agent,
                "document": document,
            })

    return matches


# ============================================================
# PAYLOAD
# ============================================================

def load_payload(path):
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    if isinstance(data, dict) and "network_elements" in data:
        items = data["network_elements"]
        meta = {
            "project": data.get("project") or {},
            "schema": data.get("schema_version"),
        }

        # Keep the same behavior as your current script:
        # existing_vavs, connections, rooms, validation, etc. are metadata
        # and are not sent as create-items.
        log(
            "HVAC_NETWORK detected; sending network_elements only: {}".format(
                len(items)
            )
        )

    elif isinstance(data, dict) and "items" in data:
        items = data["items"]
        meta = {}

    elif isinstance(data, list):
        items = data
        meta = {}

    else:
        items = [data]
        meta = {}

    if not isinstance(items, list) or not items:
        raise ValueError("Payload must contain a non-empty items list.")

    return items, meta


# ============================================================
# COMMAND CREATION
# ============================================================

def create_command(agent, document, items):
    target_selector = {
        "agent_id": agent.get("agent_id"),
        "machine_id": agent.get("machine_id"),
        "document_id": document.get("document_id"),
        "revit_process_id": document.get("revit_process_id"),
        "session_id": document.get("session_id"),
    }

    # Do not send empty selector fields.
    target_selector = {
        key: value
        for key, value in target_selector.items()
        if value not in (None, "")
    }

    body = {
        "action": "place_mep_elements",
        "items": items,
        "target_selector": target_selector,
        "max_attempts": MAX_ATTEMPTS,
    }

    response = requests.post(
        SERVER_URL + "/api/commands/create",
        headers=headers(),
        json=body,
        timeout=120,
    )

    if not response.ok:
        raise RuntimeError(
            "POST /api/commands/create failed: {} {}".format(
                response.status_code,
                response.text[:2000],
            )
        )

    result = response.json()
    command_id = result.get("command_id")

    if not command_id:
        raise RuntimeError(
            "Backend did not return command_id: {}".format(
                json.dumps(result, indent=2)
            )
        )

    return result


# ============================================================
# COMMAND MONITORING
# ============================================================

def get_command_detail(command_id):
    response = requests.get(
        SERVER_URL + "/api/commands/{}".format(command_id),
        headers=headers(),
        timeout=30,
    )

    if not response.ok:
        return None

    return response.json()


def wait_for_completion(command_id):
    started = time.time()
    last_status = None

    while time.time() - started < STATUS_TIMEOUT_SECONDS:
        detail = get_command_detail(command_id)

        if detail:
            status = str(
                detail.get("command_status")
                or detail.get("status")
                or ""
            ).upper()

            if status != last_status:
                log(
                    "command {} status -> {}".format(
                        command_id,
                        status,
                    )
                )
                last_status = status

            if status in TERMINAL_OK | TERMINAL_PARTIAL | TERMINAL_FAIL:
                return detail

        time.sleep(STATUS_POLL_SECONDS)

    raise TimeoutError(
        "Command {} did not reach a terminal state within {} seconds."
        .format(command_id, STATUS_TIMEOUT_SECONDS)
    )


# ============================================================
# FILE MOVING
# ============================================================

def move_file(source, destination_dir):
    destination_dir.mkdir(parents=True, exist_ok=True)
    destination = destination_dir / source.name

    # If the same filename already exists, add a timestamp.
    if destination.exists():
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        destination = destination_dir / (
            "{}_{}.json".format(source.stem, timestamp)
        )

    shutil.move(str(source), str(destination))
    return destination


# ============================================================
# ONE PAYLOAD
# ============================================================

def process_payload(path):
    log("=" * 70)
    log("Processing {}".format(path.name))

    try:
        target = parse_filename(path)
        log(
            "Filename target: project={} | Revit={}".format(
                target["project_key"],
                target["revit_year"],
            )
        )

        items, meta = load_payload(path)
        log("Payload items: {}".format(len(items)))

    except Exception as exc:
        log("INVALID PAYLOAD: {}".format(exc))
        move_file(path, INVALID_DIR)
        return

    try:
        agents = get_agents()
        matches = find_matching_documents(
            agents,
            target["project_key"],
            target["revit_year"],
        )
    except Exception as exc:
        log("Backend lookup failed: {}".format(exc))
        # Leave it in processing so the next run can retry safely.
        return

    if not matches:
        log(
            "No online Revit document matches project={} Revit={}. "
            "Leaving payload pending for the next scan.".format(
                target["project_key"],
                target["revit_year"],
            )
        )

        # Put it back into inbox if it was moved to processing.
        if path.parent == PROCESSING_DIR:
            move_file(path, INBOX_DIR)

        return

    if len(matches) > 1:
        log(
            "AMBIGUOUS: {} Revit documents match project={} Revit={}."
            .format(
                len(matches),
                target["project_key"],
                target["revit_year"],
            )
        )

        for index, match in enumerate(matches, start=1):
            agent = match["agent"]
            document = match["document"]
            log(
                "  [{}] machine={} agent={} document={} path={}".format(
                    index,
                    agent.get("machine_name"),
                    agent.get("agent_id"),
                    document.get("document_title"),
                    document.get("document_path"),
                )
            )

        move_file(path, AMBIGUOUS_DIR)
        return

    match = matches[0]
    agent = match["agent"]
    document = match["document"]

    log(
        "TARGET FOUND: machine={} | agent={} | document={} | document_id={}"
        .format(
            agent.get("machine_name"),
            agent.get("agent_id"),
            document.get("document_title"),
            document.get("document_id"),
        )
    )

    try:
        command = create_command(agent, document, items)
        command_id = command["command_id"]

        log("COMMAND CREATED: {}".format(command_id))

        detail = wait_for_completion(command_id)
        status = str(
            detail.get("command_status")
            or detail.get("status")
            or ""
        ).upper()

        if status == "SUCCEEDED":
            log(
                "SUCCESS: {} was executed in {}."
                .format(
                    path.name,
                    document.get("document_title"),
                )
            )
            move_file(path, SUCCEEDED_DIR)

        elif status == "PARTIAL":
            log(
                "PARTIAL: {} completed with partial results.".format(
                    path.name
                )
            )
            move_file(path, FAILED_DIR)

        else:
            log(
                "FAILED: {} ended with status {}.".format(
                    path.name,
                    status,
                )
            )
            move_file(path, FAILED_DIR)

    except Exception as exc:
        log("DISPATCH/EXECUTION ERROR: {}".format(exc))
        move_file(path, FAILED_DIR)


# ============================================================
# WATCHER
# ============================================================

def claim_inbox_file(path):
    """
    Atomically move a JSON from inbox -> processing before doing work.

    This prevents the same worker loop from processing the same file twice.
    """
    destination = PROCESSING_DIR / path.name

    if destination.exists():
        return None

    try:
        path.rename(destination)
        return destination
    except FileNotFoundError:
        return None
    except PermissionError:
        return None


def scan_once():
    files = sorted(
        p for p in INBOX_DIR.glob("*.json")
        if p.is_file()
    )

    for path in files:
        claimed = claim_inbox_file(path)

        if claimed is not None:
            process_payload(claimed)


def main():
    ensure_dirs()

    log("=" * 70)
    log("MEPF SERVER FOLDER DISPATCHER")
    log("=" * 70)
    log("Server : {}".format(SERVER_URL))
    log("Inbox  : {}".format(INBOX_DIR))
    log("Mode   : automatic / no human selection")
    log("=" * 70)

    while True:
        try:
            scan_once()
        except KeyboardInterrupt:
            log("Dispatcher stopped.")
            break
        except Exception as exc:
            log("Dispatcher loop error: {}".format(exc))

        time.sleep(SCAN_SECONDS)


if __name__ == "__main__":
    main()
