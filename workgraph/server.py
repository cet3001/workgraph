"""Live web UI for a WorkGraph: one stdlib HTTP server, zero dependencies.

Serves /api/snapshot (the whole graph as JSON) and / (a canvas page that
draws tasks, agents, dependencies and the event log, refreshing four times a
second). Run it next to any agent team and watch the team think.
"""

from __future__ import annotations

import json
from urllib.parse import unquote, urlsplit
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


PAGE = """<!doctype html>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>WorkGraph</title>
<style>
  body { background:#0d1117; color:#e6edf3; font:14px/1.4 system-ui; margin:0; display:flex; }
  #graph { flex:1; min-width:0; cursor:pointer; }
  pre { white-space:pre-wrap; overflow-wrap:anywhere; font-size:12px; }
  button, select { background:#21262d; color:#e6edf3; padding:6px; max-width:100%; }
  #side { width:360px; padding:12px; border-left:1px solid #30363d; height:100vh; overflow:auto; box-sizing:border-box; }
  .ev { padding:4px 0; border-bottom:1px solid #21262d; font-size:12px; color:#9da7b3; }
  .ev b { color:#e6edf3; }
  h1 { font-size:16px; margin:12px 0 4px; }
  .legend span { margin-right:10px; font-size:12px; }
  @media(max-width:760px){ body { flex-direction:column; } #graph { width:100%; height:760px; flex:none; } #side { width:100%; height:auto; } }
</style>
<canvas id="graph" aria-label="Live task dependency graph. Use the task selector for lineage."></canvas>
<div id="side"><h1>WorkGraph</h1>
  <div class="legend">
    <span style="color:#3fb950">&#9679; done</span>
    <span style="color:#d29922">&#9679; claimed</span>
    <span style="color:#58a6ff">&#9679; open</span>
    <span style="color:#f85149">&#9679; agent down</span>
  </div>
  <p style="color:#bc8cff">Purple ring/edge: critical path</p>
  <p id="health" role="status">Loading board...</p>
  <label for="task">Task lineage</label> <select id="task"><option value="">Choose a task</option></select>
  <pre id="lineage" aria-live="polite">Click a task node or choose a task.</pre>
  <h1>Worker output</h1><pre id="output"></pre>
  <h1>Events</h1><div id="events"></div>
</div>
<script>
const cv = document.getElementById('graph'), ctx = cv.getContext('2d');
function resize(){ cv.width = cv.clientWidth; cv.height = Math.max(760,innerHeight); }
addEventListener('resize', resize); resize();

let positions = {}, critical = [], busy = false;
const selector = document.getElementById('task');
async function read(url){ const r=await fetch(url); if(!r.ok) throw Error('Board request failed: '+r.status); return r.json(); }
async function lineage(){
  if(!selector.value) return;
  try { const rows=await read('/api/lineage/'+encodeURIComponent(selector.value));
    document.getElementById('lineage').textContent=rows.map(e=>
      new Date(e.ts*1000).toISOString()+' '+e.kind+' ('+e.agent+') '+(e.detail||'')).join('\\n') || 'No events.';
  } catch(e){ document.getElementById('lineage').textContent=e.message; }
}
selector.addEventListener('change', lineage);
cv.addEventListener('click', event=>{
  const box=cv.getBoundingClientRect(), x=(event.clientX-box.left)*cv.width/box.width, y=(event.clientY-box.top)*cv.height/box.height;
  const hit=Object.entries(positions).find(([id,p])=>Math.hypot(p.x-x,p.y-y)<=32);
  if(hit){ selector.value=hit[0]; lineage(); }
});
async function tick(){
  if(busy) return; busy=true;
  try {
    const [s,path]=await Promise.all([read('/api/snapshot'),read('/api/critical-path')]);
    critical=path.map(t=>t.id);
    const chosen=selector.value;
    selector.replaceChildren(new Option('Choose a task',''),...s.tasks.map(t=>new Option(t.title,t.id)));
    selector.value=chosen;
    draw(s); events(s); await lineage();
    document.getElementById('output').textContent=(s.worker_output||[]).join('\\n') || 'No worker output yet.';
    document.getElementById('health').textContent=s.tasks.length ?
      s.tasks.filter(t=>t.status==='done').length+'/'+s.tasks.length+' tasks done' : 'No tasks yet.';
  } catch(e){ document.getElementById('health').textContent=e.message; }
  finally { busy=false; }
}
function draw(s){
  ctx.clearRect(0,0,cv.width,cv.height);
  const tasks = s.tasks;
  const pos = {}; positions=pos;
  // simple layered layout: depth = longest BLOCKED_BY chain
  const depths = {};
  const depth = (id, seen=new Set()) => {
    if(id in depths) return depths[id];
    if(seen.has(id)) return 0;
    const next=new Set(seen); next.add(id);
    return depths[id]=Math.max(0,...s.dependencies.filter(d=>d.from===id).map(d=>1+depth(d.to,next)));
  };
  const layers = {};
  tasks.forEach(t=>{ const d=depth(t.id); (layers[d]=layers[d]||[]).push(t); });
  const nLayers = Object.keys(layers).length;
  const yGap = nLayers > 1 ? (cv.height-190)/(nLayers-1) : 0;
  Object.entries(layers).forEach(([d,ts])=>{
    ts.forEach((t,i)=>{ pos[t.id] = { x: (i+1)*cv.width/(ts.length+1),
      y: 45 + d*yGap }; });
  });
  // edges
  ctx.strokeStyle = '#30363d'; ctx.lineWidth = 2;
  s.dependencies.forEach(dep=>{ const a=pos[dep.from], b=pos[dep.to];
    if(!a||!b) return;
    ctx.strokeStyle=critical.indexOf(dep.from)===critical.indexOf(dep.to)+1 && critical.includes(dep.to) ? '#bc8cff' : '#30363d';
    ctx.beginPath(); ctx.moveTo(a.x,a.y); ctx.lineTo(b.x,b.y); ctx.stroke();
    // arrowhead
    const ang = Math.atan2(b.y-a.y, b.x-a.x);
    ctx.beginPath(); ctx.moveTo(b.x-34*Math.cos(ang)-10*Math.sin(ang)*0, b.y-34*Math.sin(ang));
    ctx.lineTo(b.x-44*Math.cos(ang)-6*Math.sin(ang), b.y-44*Math.sin(ang)+6*Math.cos(ang));
    ctx.lineTo(b.x-44*Math.cos(ang)+6*Math.sin(ang), b.y-44*Math.sin(ang)-6*Math.cos(ang));
    ctx.closePath(); ctx.fillStyle='#30363d'; ctx.fill();
  });
  // task nodes
  const col = { open:'#58a6ff', claimed:'#d29922', done:'#3fb950', failed:'#f85149' };
  tasks.forEach(t=>{ const p=pos[t.id]; if(!p) return;
    if(critical.includes(t.id)){ ctx.beginPath(); ctx.arc(p.x,p.y,35,0,7); ctx.strokeStyle='#bc8cff'; ctx.lineWidth=3; ctx.stroke(); }
    ctx.beginPath(); ctx.arc(p.x,p.y,30,0,7);
    ctx.fillStyle = col[t.status]||'#888'; ctx.globalAlpha=.25; ctx.fill();
    ctx.globalAlpha=1; ctx.lineWidth=2; ctx.strokeStyle=col[t.status]||'#888'; ctx.stroke();
    ctx.fillStyle='#e6edf3'; ctx.textAlign='center';
    ctx.font='12px system-ui';
    wrap(t.title, p.x, p.y+48, 180);
    if(t.claimed_by){ ctx.fillStyle='#e6edf3'; ctx.fillText('@'+t.claimed_by, p.x, p.y+4); }
    if(t.status==='claimed') ctx.fillText(Math.max(0,t.lease_expires-Date.now()/1000).toFixed(1)+'s lease',p.x,p.y+20);
  });
  // agents strip
  ctx.textAlign='left'; ctx.font='13px system-ui';
  s.agents.filter(a=>a.state!=='system').forEach((a,i)=>{
    const columns=Math.max(1,Math.floor(cv.width/210));
    const x=24+(i%columns)*210, y=cv.height-54-Math.floor(i/columns)*40;
    const age=Math.max(0,Date.now()/1000-a.last_seen);
    const expired=s.tasks.some(t=>t.status==='claimed' && t.claimed_by===a.name && t.lease_expires<=Date.now()/1000);
    const down=a.state==='dead'||a.process_down||expired;
    const stale=age>(a.heartbeat_interval||5);
    ctx.fillStyle = down||stale ? '#f85149' : '#3fb950';
    ctx.beginPath(); ctx.arc(x,y,6,0,7); ctx.fill();
    ctx.fillText((down?'☠ down ':stale?'stale ':'')+a.name, x+12, y+4);
    ctx.fillText('heartbeat '+age.toFixed(1)+'s ago',x+12,y+20);
  });
}
function wrap(text,x,y,w){ const words=(text||'').split(' '); let line='',dy=0;
  words.forEach(wd=>{ const test=line?line+' '+wd:wd;
    if(ctx.measureText(test).width>w && line){ ctx.fillText(line,x,y+dy); line=wd; dy+=14; }
    else line=test; });
  ctx.fillText(line,x,y+dy); }
function events(s){
  document.getElementById('events').replaceChildren(...s.events.map(e=>{
    const row=document.createElement('div'); row.className='ev';
    row.textContent=new Date(e.ts*1000).toLocaleTimeString()+' '+e.kind+' '+(e.agent||'')+' '+e.task_id+' '+(e.detail||'');
    return row;
  }));
}

tick(); setInterval(tick, 250);
</script>
"""


def create_server(get_snapshot, port: int = 8471, *, get_lineage=None, get_critical_path=None):
    """Build a loopback server. Query callbacks must be safe across threads."""
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            path = urlsplit(self.path).path
            status, content_type = 200, "application/json"
            try:
                if path == "/api/snapshot":
                    payload = get_snapshot()
                elif path.startswith("/api/lineage/") and get_lineage:
                    payload = get_lineage(unquote(path[len("/api/lineage/"):]))
                elif path == "/api/critical-path" and get_critical_path:
                    payload = get_critical_path()
                elif path == "/":
                    payload, content_type = PAGE, "text/html; charset=utf-8"
                else:
                    status, payload = 404, {"error": "Not found"}
                body = (payload if content_type.startswith("text/html") else json.dumps(payload)).encode()
            except Exception:
                status, body = 500, b'{"error": "Unable to read graph"}'
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass

    return ThreadingHTTPServer(("127.0.0.1", port), Handler)


def serve(get_snapshot, port: int = 8471, **queries) -> None:
    with create_server(get_snapshot, port, **queries) as server:
        print(f"WorkGraph UI on http://localhost:{server.server_port}")
        server.serve_forever()
