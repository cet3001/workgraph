# WorkGraph

A shared task graph for agent teams, built on Relate by Neo4j.

When a team of AI agents splits up work, the usual failure mode is
state trapped inside one process: the agent dies, and whatever it was
doing dies with it. WorkGraph puts the whole team's state - tasks,
dependencies, claims, heartbeats and a full event log - into one
Relate store directory that every agent reads and writes. A crashed
agent's claimed work shows up in the graph, and any teammate can pick
it up. Nothing to lose, because nothing important lived only in the process.

## Why a graph

Work state is about relationships: this task blocks that one, this agent
claimed that task, this event happened to that task. Relate stores
those relationships natively, in one embedded store directory with zero
servers to run. Recovery is one query: tasks claimed by an agent whose
heartbeat stopped.

## The demo

Three worker processes - scout, builder, closer - run seven dependent
tasks to build release notes from local fixtures. They extract items,
count changes, cross-check approved IDs, render a report, and verify
artifact checksums. Files are written under `demo_out/`.

While builder is working on the eight-second cross-check, the parent process
sends it SIGKILL. Its lease expires, closer reclaims the task, and the
pipeline finishes. The final output prints the task's timestamped
lineage, including orphaned and reclaimed events.

```
pip install -r requirements.txt
python demo2.py          # headless, finishes in under a minute
python demo2.py --ui     # live board at http://localhost:8471
```

The demo resets its own `demo.rlf/` and `demo_out/` directories on each
run. The board shows lease countdowns, heartbeat ages, worker output,
and the critical path. Click a task or use the task selector to read its
lineage. With `--ui`, the board stays open after completion until you press Ctrl+C.
The HTTP server listens only on localhost; the pipeline makes no network calls.

`demo.py` is the original simulated walkthrough. It steps through agents
in one process: `python demo.py` opens its board, and `python demo.py --no-ui`
runs it headless.

## Use it from your own agents

```python
from workgraph.core import WorkGraph

wg = WorkGraph("team.rlf")          # store directory, shared by every agent
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

Every agent process opens the same store directory (`workgraph.rlf/` by
default). Claims are atomic, so two agents never take the same task.
The event log (claims, completions, deaths, recoveries) is part of the
graph, so the UI and any auditor see exactly what the team did.

Workers share the store on one host. Short transactions and an OS file
lock serialize access between processes; handlers run outside the lock.

## Layout

- `workgraph/core.py` - the graph API: tasks, claims, heartbeats, recovery, events
- `workgraph/server.py` - dependency-free web UI (stdlib http.server)
- `demo2.py` - real worker processes and crash recovery
- `examples/pipeline_handlers.py` - disk-only release pipeline
- `demo.py` - the original simulated walkthrough

## AI assistance

Built with AI pair-programming (design iteration, code drafting and
debugging), as the hackathon rules allow.

## License

MIT
