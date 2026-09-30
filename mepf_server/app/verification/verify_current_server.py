"""Empirical checks of the CURRENT server code (unmodified) with simulated agents."""
import os, sys, tempfile, time, datetime as dt
tmp = tempfile.mkdtemp()
os.environ["DATABASE_URL"] = "sqlite:///%s/t.db" % tmp
os.environ["MEPF_API_KEY"] = "k"
sys.path.insert(0, os.getcwd())
from fastapi.testclient import TestClient
from app.main import app
from app.models import db as dbm
H = {"X-Api-Key": "k"}

def doc(machine, pid, sess, did, uid, title, path):
    return {"machine_id": machine, "machine_name": machine, "revit_process_id": pid, "session_id": sess,
            "revit_instance_id": "INST-%s-%s" % (machine, pid), "document_id": did, "project_uid": uid,
            "document_title": title, "document_path": path, "revit_version": "2025"}
def beat(c, machine, pid, sess, docs):
    body = {"agent_id": "AGENT-" + machine, "machine_id": machine, "revit_process_id": pid,
            "session_id": sess, "documents": docs}
    r = c.post("/api/agents/heartbeat", json=body, headers=dict(H, **{"X-Workstation-Id": machine}))
    assert r.status_code == 200, r.text

with TestClient(app) as c:
    print("1) Is /api/ai mounted?  ->", c.post("/api/ai/generate-and-place", json={}, headers=H).status_code, "(404 = not mounted)")

    # --- 2) head-of-line blocking in /next -------------------------------
    # One machine, TWO Revit processes (P1, P2). 25 commands for P1, then 1 for P2.
    beat(c, "PC1", "100", "S100", [doc("PC1", "100", "S100", "DOC-A", "UID-A", "A", r"C:\a.rvt")])
    beat(c, "PC1", "200", "S200", [doc("PC1", "200", "S200", "DOC-B", "UID-B", "B", r"C:\b.rvt")])
    for i in range(25):
        r = c.post("/api/commands/create", headers=H, json={"items": [{"placement": "point_family", "i": i}],
                   "target_selector": {"document_id": "DOC-A"}})
        assert r.status_code == 200, r.text
    r = c.post("/api/commands/create", headers=H, json={"items": [{"placement": "point_family"}],
               "target_selector": {"document_id": "DOC-B"}})
    cmd_b = r.json()["command_id"]
    r = c.get("/api/commands/next", params={"machine_id": "PC1", "revit_process_id": "200", "session_id": "S200"},
              headers=dict(H, **{"X-Workstation-Id": "PC1"}))
    print("2) P2's own command visible while 25 P1 commands are queued ahead? ->", bool(r.json()["commands"]),
          "(False = head-of-line blocking; command", cmd_b[:12], "stays PENDING)")

    # --- 3) same file open in two Revit processes on one PC -> same document_id (identity algo has no PID) ---
    beat(c, "PC2", "300", "S300", [doc("PC2", "300", "S300", "DOC-SAME", "UID-X", "X", r"C:\x.rvt")])
    beat(c, "PC2", "400", "S400", [doc("PC2", "400", "S400", "DOC-SAME", "UID-X", "X", r"C:\x.rvt")])
    rows = [d for d in c.get("/api/agents/list", headers=H).json() if d["machine_id"] == "PC2"][0]["documents"]
    print("3) Same file in 2 processes on PC2 -> registry rows:", len(rows), "| process owning DOC-SAME now:", rows[0]["revit_process_id"], "(P300 was silently displaced)")

    # --- 4) command frozen to a session that no longer exists ---------------
    r = c.post("/api/commands/create", headers=H, json={"items": [{"placement": "point_family"}], "target_selector": {"document_id": "DOC-SAME"}})
    cid = r.json()["command_id"]
    # Revit restarts: new pid/session, same file -> same document_id
    beat(c, "PC2", "500", "S500", [doc("PC2", "500", "S500", "DOC-SAME", "UID-X", "X", r"C:\x.rvt")])
    r = c.get("/api/commands/next", params={"machine_id": "PC2", "revit_process_id": "500", "session_id": "S500"}, headers=dict(H, **{"X-Workstation-Id": "PC2"}))
    st = c.get("/api/commands/%s" % cid, headers=H).json()["status"]
    print("4) After Revit restart, does the new process receive the queued command? ->", bool(r.json()["commands"]), "| status:", st, "(stranded forever: no TTL/rebind)")

    # --- 5) FAILED / PARTIAL are terminal -> no retry ---------------------------------
    beat(c, "PC3", "600", "S600", [doc("PC3", "600", "S600", "DOC-C", "UID-C", "C", r"C:\c.rvt")])
    r = c.post("/api/commands/create", headers=H, json={"items": [{"placement": "x"}], "target_selector": {"document_id": "DOC-C"}})
    cid = r.json()["command_id"]
    h3 = dict(H, **{"X-Workstation-Id": "PC3"})
    n = c.get("/api/commands/next", params={"machine_id": "PC3", "revit_process_id": "600", "session_id": "S600"}, headers=h3).json()["commands"][0]
    c.post("/api/commands/%s/start" % cid, json={"lease_token": n["lease_token"]}, headers=H)
    c.post("/api/commands/%s/result" % cid, headers=H, json={"command_id": cid, "status": "FAILED", "result": {"fatal_error": "boom"},
           "lease_token": n["lease_token"], "routing": n["routing"]})
    s = c.get("/api/commands/%s" % cid, headers=H).json()
    print("5) After agent-reported FAILED -> status:", s["status"], "| attempts:", s["attempts"], "of", s["max_attempts"], "(max_attempts is only used by lease expiry)")

    # --- 6) machine 'online' although the Revit process that owned the doc died ------
    print("6) systems.status is per machine_id; /list shows PC1 online if ANY process on it beats:")
    beat(c, "PC1", "200", "S200", [doc("PC1", "200", "S200", "DOC-B", "UID-B", "B", r"C:\b.rvt")])
    with dbm.SessionLocal() as s2:
        old = dbm.now() - dt.timedelta(seconds=200)
        d = s2.get(dbm.DocumentRecord, "DOC-A"); print("   DOC-A (process 100 stopped beating) last_seen age(s) before sweep:", 200, "-> is_online after next sweep:", end=" ")
        d.last_seen = old; s2.commit()
    c.get("/api/agents/list", headers=H)
    with dbm.SessionLocal() as s2:
        print(s2.get(dbm.DocumentRecord, "DOC-A").is_online, "| PC1 system status:", s2.get(dbm.SystemRecord, "PC1").status)
    print("   (sweep flips the doc offline OK, but the machine stays 'online' because P200 keeps beating)")
