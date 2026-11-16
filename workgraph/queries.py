"""Cypher traversals over a WorkGraph's existing task and event graph.

Paths are returned prerequisite-first. Waiting agents are agents with CLAIMED
or WAITING_ON relationships to affected downstream tasks; idle workers have no
known task affinity and are not guessed to be waiting on a particular task.
"""
from __future__ import annotations

from .core import WorkGraph


class Queries:
    def __init__(self, graph: WorkGraph):
        self.graph = graph

    def critical_path(self) -> list[dict]:
        row = self.graph.db.execute(
            "MATCH p=(t:Task)-[:BLOCKED_BY*0..]->(d:Task)"
            " WHERE all(n IN nodes(p) WHERE n.status <> 'done')"
            " RETURN nodes(p) AS tasks, length(p) AS depth"
            " ORDER BY depth DESC, t.id, d.id LIMIT 1").fetchone()
        return [n["properties"] for n in reversed(row.to_dict()["tasks"])] if row else []

    def blocked_by(self, task_id: str) -> list[dict]:
        rows = self.graph.db.execute(
            "MATCH p=(t:Task {id: $id})-[:BLOCKED_BY*1..]->(d:Task)"
            " WHERE all(n IN nodes(p) WHERE n.status <> 'done')"
            " RETURN d, min(length(p)) AS depth ORDER BY depth, d.id", {"id": task_id}).fetchall()
        return [r.to_dict()["d"]["properties"] | {"depth": r.to_dict()["depth"]} for r in rows]

    def blast_radius(self, agent_name: str) -> dict:
        with self.graph._session():
            claimed = [r.to_dict()["t"]["properties"] for r in self.graph.db.execute(
                "MATCH (a:Agent {name: $name})-[:CLAIMED]->(t:Task)"
                " WHERE t.status = 'claimed' AND t.claimed_by = a.name RETURN DISTINCT t ORDER BY t.id",
                {"name": agent_name}).fetchall()]
            downstream = [r.to_dict()["d"]["properties"] for r in self.graph.db.execute(
                "MATCH (a:Agent {name: $name})-[:CLAIMED]->(t:Task),"
                " p=(d:Task)-[:BLOCKED_BY*1..]->(t)"
                " WHERE t.status = 'claimed' AND t.claimed_by = a.name"
                " AND all(n IN nodes(p) WHERE n.status <> 'done')"
                " RETURN DISTINCT d ORDER BY d.id", {"name": agent_name}).fetchall()]
            agents = [r.to_dict()["a"]["properties"] for r in self.graph.db.execute(
                "MATCH (owner:Agent {name: $name})-[:CLAIMED]->(t:Task),"
                " p=(d:Task)-[:BLOCKED_BY*1..]->(t), (a:Agent)-[:CLAIMED|WAITING_ON]->(d)"
                " WHERE t.status = 'claimed' AND t.claimed_by = owner.name AND a.name <> $name"
                " AND all(n IN nodes(p) WHERE n.status <> 'done')"
                " RETURN DISTINCT a ORDER BY a.name", {"name": agent_name}).fetchall()]
        return {"claimed_tasks": claimed, "downstream_tasks": downstream, "waiting_agents": agents}

    def lineage(self, task_id: str) -> list[dict]:
        # Follow NEXT from the task's first event, not the global event log.
        rows = self.graph.db.execute(
            "MATCH (t:Task {id: $id})<-[:ON]-(first:Event),"
            " p=(first)-[:NEXT*0..]->(e:Event)"
            " WHERE NOT EXISTS { MATCH (:Event)-[:NEXT]->(first) }"
            " OPTIONAL MATCH (e)-[:BY]->(a:Agent)"
            " RETURN e, a, length(p) AS hop ORDER BY hop", {"id": task_id}).fetchall()
        return [r.to_dict()["e"]["properties"] | {"hop": r.to_dict()["hop"]} for r in rows]
