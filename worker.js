export default {
  async fetch(req, env) {
    const u = new URL(req.url);
    if (u.pathname == '/lobby' && req.headers.get('Upgrade') == 'websocket') {
      return env.LOBBY.get(env.LOBBY.idFromName('main')).fetch(req);
    }
    if (u.pathname.startsWith('/ws/') && req.headers.get('Upgrade') == 'websocket') {
      const id = env.ROOMS.idFromName(u.pathname.slice(4).toUpperCase().slice(0, 8));
      return env.ROOMS.get(id).fetch(req);
    }
    return new Response('Not found', { status: 404 });
  }
};

export class Room {
  constructor(s) { this.s = s; this.g = null; }
  async load() {
    if (!this.g) this.g = (await this.s.storage.get('g')) || { m: [], sd: {}, tl: null, base: 0, inc: 0, ts: 0, res: null, dr: null, init: 0 };
    return this.g;
  }
  async save() { await this.s.storage.put('g', this.g); }
  bc(o) { const x = JSON.stringify(o); for (const w of this.s.getWebSockets()) try { w.send(x); } catch {} }
  view(g) { return { m: g.m, tl: g.tl, res: g.res, base: g.base, inc: g.inc, dr: g.dr, sd: { w: !!g.sd.w, b: !!g.sd.b } }; }
  async fetch(req) {
    const [c, s] = Object.values(new WebSocketPair());
    this.s.acceptWebSocket(s);
    return new Response(null, { status: 101, webSocket: c });
  }
  tick(g) {
    const n = Date.now();
    if (g.tl && !g.res && g.m.length >= 2) {
      const t = 'wb'[g.m.length % 2];
      g.tl[t] -= n - g.ts;
      if (g.tl[t] <= 0) { g.tl[t] = 0; g.res = (t == 'w' ? 'b' : 'w') + ' time'; }
    }
    g.ts = n;
  }
  async arm(g) {
    if (g.tl && !g.res && g.m.length >= 2) await this.s.storage.setAlarm(Date.now() + Math.max(g.tl['wb'[g.m.length % 2]], 50) + 30);
    else await this.s.storage.deleteAlarm();
  }
  async alarm() {
    const g = await this.load();
    this.tick(g);
    if (g.res) { await this.save(); this.bc({ t: 'st', ...this.view(g) }); } else await this.arm(g);
  }
  async finish(g) { await this.save(); await this.arm(g); this.bc({ t: 'st', ...this.view(g) }); }
  async webSocketMessage(ws, raw) {
    let d; try { d = JSON.parse(raw); } catch { return; }
    const g = await this.load();
    if (d.t == 'join') {
      const id = String(d.id || '').slice(0, 40);
      let c = 's';
      if (!g.init) {
        g.init = 1;
        g.base = Math.min(Math.max(+d.base || 0, 0), 10800);
        g.inc = Math.min(Math.max(+d.inc || 0, 0), 60);
        g.tl = g.base ? { w: g.base * 1000, b: g.base * 1000 } : null;
      }
      if (id && g.sd.w == id) c = 'w';
      else if (id && g.sd.b == id) c = 'b';
      else if (id && !g.sd.w) { g.sd.w = id; c = 'w'; }
      else if (id && !g.sd.b) { g.sd.b = id; c = 'b'; }
      ws.serializeAttachment({ c });
      await this.save();
      ws.send(JSON.stringify({ t: 'hello', c, ...this.view(g) }));
      this.bc({ t: 'pres', sd: { w: !!g.sd.w, b: !!g.sd.b } });
      return;
    }
    const c = (ws.deserializeAttachment() || {}).c;
    if (!c || c == 's' || g.res) return;
    if (d.t == 'move') {
      if ('wb'[g.m.length % 2] != c || d.n != g.m.length || !/^[a-h][1-8][a-h][1-8][qrbn]?$/.test(d.u)) return;
      this.tick(g);
      if (g.res) { await this.save(); return this.bc({ t: 'st', ...this.view(g) }); }
      if (g.tl && g.m.length >= 2) g.tl[c] += g.inc * 1000;
      g.m.push(d.u); g.dr = null;
      if (d.r == 'mate') g.res = c + ' mate'; else if (d.r == 'stalemate') g.res = 'd stalemate';
      await this.save(); await this.arm(g);
      this.bc({ t: 'move', u: d.u, n: g.m.length - 1, tl: g.tl, res: g.res });
    } else if (d.t == 'resign') { g.res = (c == 'w' ? 'b' : 'w') + ' resign'; await this.finish(g); }
    else if (d.t == 'dr') { g.dr = c; await this.save(); this.bc({ t: 'dr', c }); }
    else if (d.t == 'dra' && g.dr && g.dr != c) { g.res = 'd agreed'; await this.finish(g); }
  }
  webSocketClose(ws) { try { ws.close(); } catch {} }
}

// Matchmaking lobby: pairs two waiting players (same clock) and hands both the same random room code.
export class Lobby {
  constructor(s) { this.s = s; }
  async fetch(req) {
    const [c, s] = Object.values(new WebSocketPair());
    this.s.acceptWebSocket(s);
    return new Response(null, { status: 101, webSocket: c });
  }
  async webSocketMessage(ws, raw) {
    let d; try { d = JSON.parse(raw); } catch { return; }
    if (d.t == 'cancel') { ws.serializeAttachment(null); return; }
    if (d.t != 'find') return;
    const me = {
      q: 1, ts: Date.now(),
      id: String(d.id || '').slice(0, 40),
      name: String(d.name || '').slice(0, 24),
      base: Math.min(Math.max(+d.base || 0, 0), 10800),
      inc: Math.min(Math.max(+d.inc || 0, 0), 60)
    };
    if (!me.id) return;
    for (const w of this.s.getWebSockets()) {
      const a = w.deserializeAttachment();
      if (!a || !a.q || a.id == me.id || a.base != me.base || a.inc != me.inc || Date.now() - a.ts > 20000) continue;
      const al = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789', r = crypto.getRandomValues(new Uint8Array(6));
      let code = ''; for (const x of r) code += al[x % al.length];
      try {
        w.send(JSON.stringify({ t: 'match', code, opp: me.name }));
        ws.send(JSON.stringify({ t: 'match', code, opp: a.name }));
        w.serializeAttachment(null); ws.serializeAttachment(null);
        return;
      } catch { w.serializeAttachment(null); }
    }
    ws.serializeAttachment(me);
  }
  webSocketClose(ws) { try { ws.close(); } catch {} }
}
