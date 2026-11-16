# WorkGraph

A shared task graph for agent teams, built on Relate by Neo4j.

When a team of AI agents splits up work, the usual failure mode is
state living inside one process: the agent dies, and whatever it was
doing dies with it. WorkGraph puts the whole team's state - tasks,
dependencies, claims, heartbeats and a full event log - into one
single-file Relate graph that every agent reads and writes. A crashed
agent's claimed work shows up in the graph, and any teammate can pick
it up. Nothing to lose, because nothing important was in the process.

## Why a graph

Work state is relationships: this task blocks that one, this agent
claimed that task, this event happened to this task. Relate stores
those relationships natively, in one embedded file with zero servers
to run. Recovery is one query: tasks claimed by an agent whose
heartbeat stopped.

## The demo

Three agents - scout, builder, closer - ship a small feature as a
chain of six dependent tasks. Midway through, builder dies without
warning, holding a claim. The team notices the stale heartbeat,
requeues the orphaned task, and finishes the build. Run it and watch:

```
pip install -r requirements.txt
python demo.py            # then open http://localhost:8471
python demo.py --no-ui    # headless: prints the event log
```

The UI draws the task graph live: nodes colored by status (blue open,
amber claimed, green done), edges for dependencies, an agent strip
(red when an agent goes down), and the event log streaming on the
side. It refreshes twice a second from `GET /api/snapshot`.

## Use it from your own agents

```python
from workgraph.core import WorkGraph

wg = WorkGraph("team.rlf")          # one file, shared by every agent
wg.register_agent("scout")

task_id = wg.add_task("Map the legacy checkout flow")
wg.add_task("Write the failing test", blocked_by=[task_id])

for task in wg.claimable():          # open tasks with all deps done
    if wg.claim("scout", task["id"]):
        wg.complete("scout", task["id"], result="ok")
        break

# on any agent, any time:
wg.recover_orphans(stale_after=30)   # requeue work from dead agents
```

Every agent process opens the same file. Claims are atomic, so two
agents never take the same task. The event log (claims, completions,
deaths, recoveries) is part of the graph, so the UI and any auditor
see exactly what the team did.

Agents in the demo run as stepped coroutines in one process so the
demo fits in one file. The same API works across processes and
machines: point every agent at the same graph path on shared storage.

## Layout

- `workgraph/core.py` - the graph API: tasks, claims, heartbeats, recovery, events
- `workgraph/server.py` - dependency-free web UI (stdlib http.server)
- `demo.py` - the three-agent crash-and-recover scenario

## AI assistance

Built with AI pair-programming (design iteration, code drafting and
debugging), as the hackathon rules allow.

## License

MIT
