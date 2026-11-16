"""WorkGraph core: a shared task graph for agent teams, built on Relate by Neo4j.

One single-file Relate graph holds the whole team state: agents, tasks,
dependencies, claims and a full event log. Every agent reads and writes the
same graph, so a crashed agent's work is visible to everyone else and can be
recovered instead of lost.
"""

from __future__ import annotations

import threading
import time
import uuid

import relate

OPEN, CLAIMED, DONE, FAILED = "open", "claimed", "done", "failed"


class WorkGraph:
    """Shared task graph. One instance per agent process; all point at the
    same Relate database file, so every agent sees the same truth."""

    def __init__(self, path: str = "workgraph.rlf"):
        self.db = relate.connect(path)
        self.db.autocommit = True
        self._lock = threading.Lock()

    # -- tasks -----------------------------------------------------------

    def add_task(self, title: str, task_id: str | None = None,
                 blocked_by: list[str] | None = None) -> str:
        task_id = task_id or "t-" + uuid.uuid4().hex[:8]
        now = time.time()
        with self._lock:
            self.db.execute(
                "CREATE (t:Task {id: $id, title: $title, status: $status,"
                " claimed_by: '', result: '', created_at: $now, updated_at: $now})",
                {"id": task_id, "title": title, "status": OPEN, "now": now},
            )
            for dep in blocked_by or []:
                self.db.execute(
                    "MATCH (a:Task {id: $id}), (b:Task {id: $dep})"
                    " CREATE (a)-[:BLOCKED_BY]->(b)",
                    {"id": task_id, "dep": dep},
                )
        self._event("task_added", None, task_id, title)
        return task_id

    def claimable(self) -> list[dict]:
        """Open tasks whose every BLOCKED_BY dependency is done."""
        rows = self.db.execute(
            "MATCH (t:Task {status: 'open'})"
            " WHERE NOT EXISTS {"
            "   MATCH (t)-[:BLOCKED_BY]->(d:Task) WHERE d.status <> 'done'"
            " } RETURN t"
        ).fetchall()
        return [r.to_dict()["t"]["properties"] | {"id": r.to_dict()["t"]["properties"]["id"]} for r in rows]

    def claim(self, agent: str, task_id: str) -> bool:
        """Atomically claim an open task. Returns False if it was already taken."""
        with self._lock:
            row = self.db.execute(
                "MATCH (t:Task {id: $id}) RETURN t", {"id": task_id}
            ).fetchone()
            if not row or row.to_dict()["t"]["properties"]["status"] != OPEN:
                return False
            self.db.execute(
                "MATCH (t:Task {id: $id})"
                " SET t.status = $claimed, t.claimed_by = $agent, t.updated_at = $now",
                {"id": task_id, "claimed": CLAIMED, "agent": agent, "now": time.time()},
            )
            self.db.execute(
                "MATCH (a:Agent {name: $agent}), (t:Task {id: $id})"
                " CREATE (a)-[:CLAIMED]->(t)",
                {"agent": agent, "id": task_id},
            )
        self._event("claimed", agent, task_id, "")
        return True

    def complete(self, agent: str, task_id: str, result: str = "") -> None:
        self.db.execute(
            "MATCH (t:Task {id: $id, claimed_by: $agent})"
            " SET t.status = $done, t.result = $result, t.updated_at = $now",
            {"id": task_id, "agent": agent, "done": DONE,
             "result": result, "now": time.time()},
        )
        self._event("completed", agent, task_id, result)

    def fail(self, agent: str, task_id: str, reason: str) -> None:
        """Give up a claim; the task goes back to open for another agent."""
        self.db.execute(
            "MATCH (t:Task {id: $id, claimed_by: $agent})"
            " SET t.status = $open, t.claimed_by = '', t.updated_at = $now",
            {"id": task_id, "agent": agent, "open": OPEN, "now": time.time()},
        )
        self._event("released", agent, task_id, reason)

    # -- agents ----------------------------------------------------------

    def register_agent(self, name: str) -> None:
        self.db.execute(
            "MERGE (a:Agent {name: $name})"
            " SET a.state = 'alive', a.last_seen = $now",
            {"name": name, "now": time.time()},
        )
        self._event("agent_registered", name, "", "")

    def heartbeat(self, name: str) -> None:
        self.db.execute(
            "MATCH (a:Agent {name: $name}) SET a.last_seen = $now",
            {"name": name, "now": time.time()},
        )

    def mark_dead(self, name: str) -> None:
        self.db.execute(
            "MATCH (a:Agent {name: $name}) SET a.state = 'dead'",
            {"name": name},
        )
        self._event("agent_died", name, "", "")

    # -- recovery ----------------------------------------------------------

    def recover_orphans(self, stale_after: float = 30.0) -> list[str]:
        """Requeue tasks claimed by agents that stopped heartbeating.

        This is the payoff of keeping work state in a shared graph: nothing
        lives in a dead process's memory, so nothing is lost with it.
        """
        cutoff = time.time() - stale_after
        rows = self.db.execute(
            "MATCH (t:Task {status: 'claimed'})<-[:CLAIMED]-(a:Agent)"
            " WHERE t.claimed_by = a.name"
            " AND (a.last_seen < $cutoff OR a.state = 'dead')"
            " RETURN t, a",
            {"cutoff": cutoff},
        ).fetchall()
        recovered = []
        with self._lock:
            for r in rows:
                d = r.to_dict()
                tid = d["t"]["properties"]["id"]
                dead = d["a"]["properties"]["name"]
                self.db.execute(
                    "MATCH (:Agent)-[r:CLAIMED]->(t:Task {id: $id}) DELETE r",
                    {"id": tid},
                )
                self.db.execute(
                    "MATCH (t:Task {id: $id})"
                    " SET t.status = 'open', t.claimed_by = '', t.updated_at = $now",
                    {"id": tid, "now": time.time()},
                )
                self._event("recovered", dead, tid, "requeued from dead agent")
                recovered.append(tid)
        return recovered

    # -- read models -------------------------------------------------------

    def snapshot(self) -> dict:
        """Everything the UI or a curious agent needs, in one local query."""
        tasks = [r.to_dict()["t"]["properties"] for r in
                 self.db.execute("MATCH (t:Task) RETURN t").fetchall()]
        agents = [r.to_dict()["a"]["properties"] for r in
                  self.db.execute("MATCH (a:Agent) RETURN a").fetchall()]
        deps = [{"from": r.to_dict()["a"]["properties"]["id"],
                 "to": r.to_dict()["b"]["properties"]["id"]} for r in
                self.db.execute(
                    "MATCH (a:Task)-[:BLOCKED_BY]->(b:Task) RETURN a, b"
                ).fetchall()]
        events = [r.to_dict()["e"]["properties"] for r in
                  self.db.execute(
                      "MATCH (e:Event) RETURN e ORDER BY e.ts DESC LIMIT 50"
                  ).fetchall()]
        return {"tasks": tasks, "agents": agents, "dependencies": deps,
                "events": events}

    # -- internals ---------------------------------------------------------

    def _event(self, kind: str, agent: str | None, task_id: str, detail: str) -> None:
        eid = "e-" + uuid.uuid4().hex[:10]
        self.db.execute(
            "CREATE (e:Event {id: $eid, ts: $ts, kind: $kind,"
            " agent: $agent, task_id: $task, detail: $detail})",
            {"eid": eid, "ts": time.time(), "kind": kind,
             "agent": agent or "", "task": task_id, "detail": detail},
        )
        if task_id:
            self.db.execute(
                "MATCH (e:Event {id: $eid}), (t:Task {id: $tid})"
                " CREATE (e)-[:ON]->(t)",
                {"eid": eid, "tid": task_id},
            )

    def close(self) -> None:
        self.db.close()
