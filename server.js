// Forge Adventure 실시간 서버 (Node.js + ws)
const http = require('http'), fs = require('fs'), path = require('path');
const { WebSocketServer } = require('ws');
const PORT = process.env.PORT || 3000;

const server = http.createServer((req, res) => {
  if (req.url === '/' || req.url.startsWith('/?')) {
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-cache' });
    res.end(fs.readFileSync(path.join(__dirname, 'index.html')));
  } else if (req.url === '/health') { res.writeHead(200); res.end('ok'); }
  else { res.writeHead(404); res.end(); }
});
const wss = new WebSocketServer({ server, maxPayload: 8192 });
const P = new Map(); // id -> { s: 마지막 상태, ws }

const num = (v, lo, hi) => Math.max(lo, Math.min(hi, +v || 0));
function clean(m) {
  if (!m || !m.id) return null;
  const o = {
    id: String(m.id).slice(0, 24), n: String(m.n || '모험가').slice(0, 8),
    x: num(m.x, -1e5, 1e5), y: num(m.y, -1e5, 1e5), d: num(m.d, 0, 3) | 0, c: num(m.c, 0, 99) | 0,
    lv: num(m.lv, 1, 1e6) | 0, wt: num(m.wt, 0, 99) | 0, mv: m.mv ? 1 : 0, z: String(m.z || 'w').slice(0, 6)
  };
  if (m.lk && JSON.stringify(m.lk).length < 600) o.lk = m.lk;
  o.ev = (Array.isArray(m.ev) ? m.ev : []).slice(0, 12).map(e => ({
    s: +e.s || 0, k: e.k === 'u' ? 'u' : (e.k === 'a' ? 'a' : 's'), a: num(e.a, 0, 200),
    c: /^#[0-9a-fA-F]{3,8}$/.test(e.c || '') ? e.c : '', ago: num(e.ago, 0, 5000)
  }));
  return o;
}
const send = (ws, o) => { if (ws.readyState === 1) ws.send(JSON.stringify(o)); };

wss.on('connection', ws => {
  ws.id = null; ws.alive = true; ws.cnt = 0; ws.win = Date.now();
  ws.on('pong', () => { ws.alive = true; });
  ws.on('message', raw => {
    const now = Date.now();
    if (now - ws.win > 1000) { ws.win = now; ws.cnt = 0; }
    if (++ws.cnt > 30) return;                       // 초당 30회 제한
    let m; try { m = JSON.parse(raw); } catch (e) { return; }
    const c = clean(m); if (!c) return;
    if (!ws.id) {
      ws.id = c.id;
      const old = P.get(c.id); if (old && old.ws !== ws) { try { old.ws.close(); } catch (e) {} }
      send(ws, { t: 'l', list: [...P.values()].filter(v => v.ws !== ws).map(v => v.s) });
    } else if (ws.id !== c.id) return;
    const prev = P.get(c.id);
    P.set(c.id, { s: Object.assign({}, c, { ev: [], lk: c.lk || (prev && prev.s.lk) || null }), ws });
    const out = JSON.stringify(Object.assign({ t: 's' }, c));
    wss.clients.forEach(o => { if (o !== ws && o.id && o.readyState === 1) o.send(out); });
  });
  ws.on('close', () => {
    const e = ws.id && P.get(ws.id);
    if (e && e.ws === ws) {
      P.delete(ws.id);
      const out = JSON.stringify({ t: 'x', id: ws.id });
      wss.clients.forEach(o => { if (o.readyState === 1) o.send(out); });
    }
  });
});
setInterval(() => wss.clients.forEach(ws => { if (!ws.alive) return ws.terminate(); ws.alive = false; ws.ping(); }), 10000);
server.listen(PORT, () => console.log('Forge Adventure 서버 실행: http://localhost:' + PORT));
