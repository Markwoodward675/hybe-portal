(() => {
  if (!window || !document) return;
  if (window.HybridAI) return;

  const DEFAULTS = {
    maxPassengers: 50,
    cities: ['Accra', 'London', 'Dubai', 'New York', 'Johannesburg', 'Paris', 'Amsterdam', 'Frankfurt', 'Singapore', 'Seoul'],
    wsPath: null,
    schemaInspect: false,
    notifications: true,
    aiPassengers: true,
    geoFlights: true,
  };

  const TZ = {
    Accra: { iana: 'Africa/Accra', utcOffsetHours: 0, lat: 5.6037, lon: -0.1870 },
    London: { iana: 'Europe/London', utcOffsetHours: 0, lat: 51.5072, lon: -0.1276 },
    Dubai: { iana: 'Asia/Dubai', utcOffsetHours: 4, lat: 25.2048, lon: 55.2708 },
    'New York': { iana: 'America/New_York', utcOffsetHours: -5, lat: 40.7128, lon: -74.0060 },
    Johannesburg: { iana: 'Africa/Johannesburg', utcOffsetHours: 2, lat: -26.2041, lon: 28.0473 },
    Paris: { iana: 'Europe/Paris', utcOffsetHours: 1, lat: 48.8566, lon: 2.3522 },
    Amsterdam: { iana: 'Europe/Amsterdam', utcOffsetHours: 1, lat: 52.3676, lon: 4.9041 },
    Frankfurt: { iana: 'Europe/Berlin', utcOffsetHours: 1, lat: 50.1109, lon: 8.6821 },
    Singapore: { iana: 'Asia/Singapore', utcOffsetHours: 8, lat: 1.3521, lon: 103.8198 },
    Seoul: { iana: 'Asia/Seoul', utcOffsetHours: 9, lat: 37.5665, lon: 126.9780 },
  };

  const cache = {
    aiPassengers: { items: null, expiresAt: 0 },
  };

  function nowIso() {
    return new Date().toISOString();
  }

  function clamp(min, v, max) {
    return Math.max(min, Math.min(max, v));
  }

  function randInt(min, max) {
    return Math.floor(Math.random() * (max - min + 1)) + min;
  }

  function pick(arr) {
    return arr[Math.floor(Math.random() * arr.length)];
  }

  function safeJsonParse(s) {
    try { return JSON.parse(s); } catch { return null; }
  }

  function showHybridNotification(n) {
    const title = (n && n.title) ? String(n.title) : 'Live Update';
    const msg = (n && n.message !== undefined) ? String(n.message) : String((n && n.msg) || '');
    const tone = (n && n.tone) ? String(n.tone) : 'accent';
    if (typeof window.showNotification === 'function') {
      window.showNotification(msg, { title, tone });
      return;
    }
    try {
      const el = document.createElement('div');
      el.style.cssText = 'position:fixed;top:16px;right:16px;z-index:9999;background:#fff;border:1px solid rgba(0,0,0,0.12);border-left:4px solid #ff4d6d;padding:12px 12px;border-radius:14px;max-width:340px;box-shadow:0 14px 40px rgba(0,0,0,0.18);font-family:system-ui;';
      el.innerHTML = `<div style="font-weight:900;font-size:0.9rem;">${title}</div><div style="margin-top:6px;font-weight:700;font-size:0.86rem;line-height:1.35;">${msg}</div>`;
      document.body.appendChild(el);
      setTimeout(() => el.remove(), 6500);
    } catch {}
  }

  async function fetchServerNotifications() {
    try {
      const res = await fetch('/api/public/notifications', { credentials: 'include' });
      if (!res.ok) return [];
      const out = await res.json().catch(() => ({}));
      const items = out && Array.isArray(out.items) ? out.items : [];
      return items.filter((x) => x && x.active !== false);
    } catch {
      return [];
    }
  }

  function tryWebSocket(onMessage, wsPath) {
    const handler = typeof onMessage === 'function' ? onMessage : () => {};
    try {
      const host = String(window.location && window.location.host ? window.location.host : '');
      if (host.endsWith('vercel.app')) return null;
      const targetPath = wsPath || DEFAULTS.wsPath;
      if (!targetPath) return null;
      const proto = window.location.protocol === 'https:' ? 'wss' : 'ws';
      const url = `${proto}://${window.location.host}${targetPath}`;
      const ws = new WebSocket(url);
      ws.addEventListener('message', (e) => {
        const data = typeof e.data === 'string' ? safeJsonParse(e.data) : null;
        if (data) handler(data);
      });
      ws.addEventListener('error', () => {});
      return ws;
    } catch {
      return null;
    }
  }

  function tryAppwriteRealtime(onMessage) {
    const handler = typeof onMessage === 'function' ? onMessage : () => {};
    if (typeof window.subscribeNotifications === 'function') {
      return window.subscribeNotifications((event) => {
        try {
          const payload = event && event.payload ? event.payload : null;
          if (!payload) return;
          if (payload.active === false) return;
          handler(payload);
        } catch {}
      });
    }
    return null;
  }

  function startHybridNotifications(options) {
    const enabled = options && options.notifications !== false;
    if (!enabled) return { stop: () => {} };

    let serverItems = [];
    let stopped = false;
    let ws = null;
    let sub = null;

    fetchServerNotifications().then((items) => { serverItems = items; });

    const push = (raw) => {
      if (stopped) return;
      const title = String(raw.title || 'Live Update');
      const message = String(raw.message || raw.msg || '');
      const tone = String(raw.tone || 'accent');
      if (!message) return;
      showHybridNotification({ title, message, tone });
    };

    ws = tryWebSocket(push, options.wsPath);
    sub = tryAppwriteRealtime(push);

    const firstDelay = randInt(3000, 5000);
    let timer = null;

    function scheduleNext() {
      if (stopped) return;
      const next = randInt(10000, 25000);
      timer = window.setTimeout(() => {
        const useServer = serverItems.length > 0 && Math.random() < 0.7;
        const useFake = !useServer || Math.random() < 0.3;
        if (useServer && serverItems.length) {
          const it = pick(serverItems);
          push(it);
        }
        if (useFake && typeof window.generateNotification === 'function') {
          const n = window.generateNotification();
          showHybridNotification({ title: n.title, message: n.msg, tone: n.tone });
        }
        if (Math.random() < 0.3) fetchServerNotifications().then((items) => { serverItems = items; });
        scheduleNext();
      }, next);
    }

    window.setTimeout(() => {
      if (stopped) return;
      const useServer = serverItems.length > 0 && Math.random() < 0.7;
      if (useServer) push(pick(serverItems));
      else if (typeof window.generateNotification === 'function') {
        const n = window.generateNotification();
        showHybridNotification({ title: n.title, message: n.msg, tone: n.tone });
      }
      scheduleNext();
    }, firstDelay);

    return {
      stop: () => {
        stopped = true;
        if (timer) window.clearTimeout(timer);
        try { if (ws) ws.close(); } catch {}
        try { if (sub && typeof sub === 'function') sub(); } catch {}
      },
    };
  }

  function haversineKm(a, b) {
    const R = 6371;
    const toRad = (d) => (d * Math.PI) / 180;
    const dLat = toRad(b.lat - a.lat);
    const dLon = toRad(b.lon - a.lon);
    const lat1 = toRad(a.lat);
    const lat2 = toRad(b.lat);
    const h =
      Math.sin(dLat / 2) * Math.sin(dLat / 2) +
      Math.sin(dLon / 2) * Math.sin(dLon / 2) * Math.cos(lat1) * Math.cos(lat2);
    const c = 2 * Math.atan2(Math.sqrt(h), Math.sqrt(1 - h));
    return R * c;
  }

  function computeFlightDurationHours(fromCity, toCity) {
    const a = TZ[fromCity];
    const b = TZ[toCity];
    if (!a || !b) return 2;
    const km = haversineKm(a, b);
    const cruise = 860;
    const hours = km / cruise;
    return clamp(1, Math.round((hours + 0.5) * 10) / 10, 18);
  }

  function timezoneDiffHours(fromCity, toCity) {
    const a = TZ[fromCity];
    const b = TZ[toCity];
    if (!a || !b) return 0;
    return b.utcOffsetHours - a.utcOffsetHours;
  }

  function generateGeoRoutes(cities, maxRoutes) {
    const list = Array.isArray(cities) && cities.length ? cities.slice() : DEFAULTS.cities.slice();
    const out = [];
    const used = new Set();
    const max = Math.max(1, Math.min(50, Number(maxRoutes) || 12));
    for (let i = 0; i < 300 && out.length < max; i++) {
      const from = pick(list);
      const to = pick(list);
      if (!from || !to || from === to) continue;
      const key = `${from}->${to}`;
      if (used.has(key)) continue;
      used.add(key);
      out.push({
        from,
        to,
        durationHours: computeFlightDurationHours(from, to),
        timezoneDiffHours: timezoneDiffHours(from, to),
      });
    }
    return out;
  }

  async function getAiPassengers(limit, options = {}) {
    const max = Math.max(1, Math.min(DEFAULTS.maxPassengers, Number(limit) || 30));
    const ttlMs = typeof options.ttlMs === 'number' ? options.ttlMs : 10 * 60 * 1000;

    if (cache.aiPassengers.items && Date.now() < cache.aiPassengers.expiresAt) {
      return cache.aiPassengers.items.slice(0, max);
    }

    try {
      const res = await fetch(`/api/public/ai/passengers?limit=${encodeURIComponent(max)}`, { credentials: 'include' });
      const out = await res.json().catch(() => ({}));
      const items = out && Array.isArray(out.items) ? out.items : [];
      cache.aiPassengers.items = items;
      cache.aiPassengers.expiresAt = Date.now() + ttlMs;
      return items.slice(0, max);
    } catch {
      return [];
    }
  }

  function assignPassengersToRoutes(passengers, routes) {
    const ps = Array.isArray(passengers) ? passengers.slice() : [];
    const rs = Array.isArray(routes) && routes.length ? routes : generateGeoRoutes(DEFAULTS.cities, 12);
    return ps.map((p, idx) => {
      const r = rs[idx % rs.length];
      return {
        ...p,
        route: r ? `${r.from}-${r.to}` : '',
        departureCity: r ? r.from : '',
        arrivalCity: r ? r.to : '',
        durationHours: r ? r.durationHours : null,
        timezoneDiffHours: r ? r.timezoneDiffHours : null,
      };
    });
  }

  async function adminListNotifications() {
    const res = await fetch('/api/admin/notifications', { credentials: 'include' });
    if (!res.ok) throw new Error('adminListNotifications failed');
    return res.json();
  }

  async function adminCreateNotification(payload) {
    const res = await fetch('/api/admin/notifications', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      credentials: 'include',
      body: JSON.stringify(payload || {}),
    });
    if (!res.ok) throw new Error('adminCreateNotification failed');
    return res.json();
  }

  async function adminUpdateNotification(payload) {
    const res = await fetch('/api/admin/notifications', {
      method: 'PATCH',
      headers: { 'content-type': 'application/json' },
      credentials: 'include',
      body: JSON.stringify(payload || {}),
    });
    if (!res.ok) throw new Error('adminUpdateNotification failed');
    return res.json();
  }

  async function adminDeleteNotification(id) {
    const res = await fetch(`/api/admin/notifications/${encodeURIComponent(String(id || ''))}`, {
      method: 'DELETE',
      credentials: 'include',
    });
    if (!res.ok) throw new Error('adminDeleteNotification failed');
    return res.json();
  }

  async function inspectSchema() {
    const res = await fetch('/api/admin/schema/inspect', { credentials: 'include' });
    const out = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(out && out.error ? out.error : 'schema inspect failed');
    return out;
  }

  async function schemaSync() {
    const res = await fetch('/api/admin/schema/sync', { method: 'POST', credentials: 'include' });
    const out = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(out && out.error ? out.error : 'schema sync failed');
    return out;
  }

  function logSchemaSummary(schema) {
    try {
      console.log('TRIP Appwrite Schema Summary', schema);
    } catch {}
  }

  function init(opts = {}) {
    const options = { ...DEFAULTS, ...(opts || {}) };

    let notifController = null;
    if (options.notifications) {
      notifController = startHybridNotifications(options);
    }

    if (options.schemaInspect) {
      inspectSchema().then(logSchemaSummary).catch(() => {});
    }

    return {
      stop: () => {
        if (notifController) notifController.stop();
      },
    };
  }

  window.HybridAI = {
    init,
    getAiPassengers,
    generateGeoRoutes,
    assignPassengersToRoutes,
    startHybridNotifications,
    admin: {
      listNotifications: adminListNotifications,
      createNotification: adminCreateNotification,
      updateNotification: adminUpdateNotification,
      deleteNotification: adminDeleteNotification,
    },
    schema: {
      inspect: inspectSchema,
      sync: schemaSync,
    },
  };
})();
