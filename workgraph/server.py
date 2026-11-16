"""Live web UI for a WorkGraph: one stdlib HTTP server, zero dependencies.

Serves /api/snapshot (the whole graph as JSON) and / (a canvas page that
draws tasks, agents, dependencies and the event log, refreshing twice a
second). Run it next to any agent team and watch the team think.
"""

from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


PAGE = """<!doctype html>
<meta charset="utf-8">
<title>WorkGraph</title>
<style>
  body { background:#0d1117; color:#e6edf3; font:14px/1.4 system-ui; margin:0; display:flex; }
  #graph { flex:1; }
  #side { width:360px; padding:12px; border-left:1px solid #30363d; height:100vh; overflow:auto; box-sizing:border-box; }
  .ev { padding:4px 0; border-bottom:1px solid #21262d; font-size:12px; color:#9da7b3; }
  .ev b { color:#e6edf3; }
  h1 { font-size:16px; margin:12px 0 4px; }
  .legend span { margin-right:10px; font-size:12px; }
</style>
<canvas id="graph"></canvas>
<div id="side"><h1>WorkGraph</h1>
  <div class="legend">
    <span style="color:#3fb950">&#9679; done</span>
    <span style="color:#d29922">&#9679; claimed</span>
    <span style="color:#58a6ff">&#9679; open</span>
    <span style="color:#f85149">&#9679; agent down</span>
  </div>
  <div id="events"></div>
</div>
<script>
const cv = document.getElementById('graph'), ctx = cv.getContext('2d');
function resize(){ cv.width = cv.clientWidth; cv.height = innerHeight; }
addEventListener('resize', resize); resize();

async function tick(){
  const s = await (await fetch('/api/snapshot')).json();
  draw(s); events(s);
}
function draw(s){
  ctx.clearRect(0,0,cv.width,cv.height);
  const tasks = s.tasks, n = tasks.length;
  const pos = {};
  // simple layered layout: depth = longest BLOCKED_BY chain
  const byId = Object.fromEntries(tasks.map(t=>[t.id,t]));
  const depth = id => { let d=0, cur=byId[id], seen=new Set();
    while(cur){ const dep = s.dependencies.find(x=>x.from===cur.id);
      if(!dep || seen.has(dep.to)) break; seen.add(dep.to); cur = byId[dep.to]; d++; }
    return d; };
  const layers = {};
  tasks.forEach(t=>{ const d=depth(t.id); (layers[d]=layers[d]||[]).push(t); });
  const nLayers = Object.keys(layers).length;
  const xGap = nLayers > 1 ? Math.min(220, (cv.width-200)/(nLayers-1)) : 0;
  Object.entries(layers).forEach(([d,ts])=>{
    ts.forEach((t,i)=>{ pos[t.id] = { x: 100 + d*xGap,
      y: 90 + i*(cv.height-160)/Math.max(ts.length,1) }; });
  });
  // edges
  ctx.strokeStyle = '#30363d'; ctx.lineWidth = 2;
  s.dependencies.forEach(dep=>{ const a=pos[dep.from], b=pos[dep.to];
    if(!a||!b) return;
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
    ctx.beginPath(); ctx.arc(p.x,p.y,30,0,7);
    ctx.fillStyle = col[t.status]||'#888'; ctx.globalAlpha=.25; ctx.fill();
    ctx.globalAlpha=1; ctx.lineWidth=2; ctx.strokeStyle=col[t.status]||'#888'; ctx.stroke();
    ctx.fillStyle='#e6edf3'; ctx.textAlign='center';
    ctx.font='12px system-ui';
    wrap(t.title, p.x, p.y+48, 180);
    if(t.claimed_by){ ctx.fillStyle='#9da7b3'; ctx.fillText('@'+t.claimed_by, p.x, p.y+4); }
  });
  // agents strip
  ctx.textAlign='left'; ctx.font='13px system-ui';
  s.agents.forEach((a,i)=>{ const x=24+i*180, y=cv.height-36;
    ctx.fillStyle = a.state==='dead' ? '#f85149' : '#3fb950';
    ctx.beginPath(); ctx.arc(x,y,6,0,7); ctx.fill();
    ctx.fillStyle='#e6edf3'; ctx.fillText(a.name, x+12, y+4);
  });
}
function wrap(text,x,y,w){ const words=(text||'').split(' '); let line='',dy=0;
  words.forEach(wd=>{ const test=line?line+' '+wd:wd;
    if(ctx.measureText(test).width>w && line){ ctx.fillText(line,x,y+dy); line=wd; dy+=14; }
    else line=test; });
  ctx.fillText(line,x,y+dy); }
function events(s){
  document.getElementById('events').innerHTML = s.events.map(e=>{
    const t = new Date(e.ts*1000).toLocaleTimeString();
    return `<div class="ev">${t} <b>${e.kind}</b> ${e.agent?('@'+e.agent):''} ${e.task_id} ${e.detail||''}</div>`;
  }).join('');
}
tick(); setInterval(tick, 500);
</script>
"""


def serve(get_snapshot, port: int = 8471) -> None:
    """get_snapshot: zero-arg callable returning the current snapshot dict.

    The UI never touches the database itself: relate connections are
    thread-affine, so the owning agent team hands snapshots over instead.
    """
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path == "/api/snapshot":
                body = json.dumps(get_snapshot()).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
            else:
                body = PAGE.encode()
                self.send_response(200)
                self.send_header("Content-Type", "text/html")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):  # quiet
            pass

    print(f"WorkGraph UI on http://localhost:{port}")
    ThreadingHTTPServer(("127.0.0.1", port), Handler).serve_forever()
