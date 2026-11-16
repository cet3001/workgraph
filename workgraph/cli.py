"""Command line interface; handler modules export handler(task) -> str."""
from __future__ import annotations

import argparse
import importlib
import json
import time

from .core import WorkGraph
from .queries import Queries
from .worker import Worker


def default_handler(task: dict) -> str:
    return f"Completed: {task['title']}"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m workgraph")
    commands = parser.add_subparsers(dest="command", required=True)
    init = commands.add_parser("init")
    init.add_argument("path")
    add = commands.add_parser("add")
    add.add_argument("title")
    add.add_argument("--depends-on", nargs="+", default=[])
    worker = commands.add_parser("worker")
    worker.add_argument("name")
    worker.add_argument("--heartbeat-interval", type=float, default=5.0)
    worker.add_argument("--claim-ttl", type=float, default=45.0)
    worker.add_argument("--module", help="Importable module exporting handler(task) -> str")
    status = commands.add_parser("status")
    query = commands.add_parser("query")
    query.add_argument("query", choices=["critical-path", "blocked-by", "blast-radius", "lineage"])
    query.add_argument("target", nargs="?")
    for command in (add, worker, status, query):
        command.add_argument("--db", default="workgraph.rlf")
    args = parser.parse_args(argv)
    if args.command == "query" and (args.query != "critical-path") != (args.target is not None):
        parser.error("blocked-by, blast-radius and lineage require one target; critical-path takes none")
    if args.command == "worker":
        handler = default_handler
        if args.module:
            # Accept both a module exporting handler and a dotted function path.
            try:
                module = importlib.import_module(args.module)
            except ModuleNotFoundError as exc:
                parent, separator, attribute = args.module.rpartition(".")
                if not separator or exc.name != args.module:
                    raise
                handler = getattr(importlib.import_module(parent), attribute)
            else:
                handler = getattr(module, "handler")
        Worker(args.name, args.db, handler, args.heartbeat_interval, args.claim_ttl).run()
        return 0
    graph = WorkGraph(args.path if args.command == "init" else args.db)
    try:
        queries = Queries(graph)
        if args.command == "init":
            print(f"Initialized {args.path}")
        elif args.command == "add":
            print(graph.add_task(args.title, blocked_by=args.depends_on))
        elif args.command == "query":
            method = getattr(queries, args.query.replace("-", "_"))
            print(json.dumps(method(args.target) if args.target is not None else method(), indent=2))
        else:
            snapshot = graph.snapshot()
            print("BOARD")
            for task in snapshot["tasks"]:
                print(f"{task['id']}  {task['status']:<8} {task['claimed_by'] or '-':<12} {task['title']}")
            print("CRITICAL PATH")
            print(" -> ".join(t["title"] for t in queries.critical_path()) or "(none)")
            print("AGENT HEALTH")
            now = time.time()
            for agent in snapshot["agents"]:
                age = max(0.0, now - agent["last_seen"])
                health = agent["state"] if agent["state"] != "alive" else "stale" if age > 30 else "alive"
                print(f"{agent['name']}  {health}  last heartbeat {age:.1f}s ago")
    finally:
        graph.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
