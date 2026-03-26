const crypto = require('crypto');
const { parseCookies, cookieString, isHttps } = require('../server_lib/cookies');
const { createToken, verifyToken } = require('../server_lib/token');
const {
  appwriteRequest,
  DATABASE_ID,
  USERS_COLLECTION_ID,
  findUserDocByUsername,
  listAllUserDocs,
  parseUserData,
  upsertUser,
  deleteUser,
} = require('../server_lib/appwrite');

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function readJson(req) {
  return new Promise((resolve, reject) => {
    let data = '';
    req.on('data', (c) => (data += c));
    req.on('end', () => {
      try {
        resolve(data ? JSON.parse(data) : {});
      } catch (e) {
        reject(e);
      }
    });
  });
}

function send(res, status, payload) {
  res.statusCode = status;
  res.setHeader('content-type', 'application/json');
  res.end(JSON.stringify(payload));
}

function requireAdmin(req, res) {
  const cookies = parseCookies(req);
  const payload = verifyToken(cookies.trip_admin);
  if (!payload || payload.typ !== 'admin') {
    send(res, 401, { error: 'Unauthorized' });
    return null;
  }
  return payload;
}

function requireUser(req, res) {
  const cookies = parseCookies(req);
  const payload = verifyToken(cookies.trip_session);
  if (!payload || payload.typ !== 'user') {
    send(res, 401, { error: 'Unauthorized' });
    return null;
  }
  return payload;
}

async function exists(path) {
  try {
    await appwriteRequest(path);
    return true;
  } catch (e) {
    if (e && e.status === 404) return false;
    throw e;
  }
}

async function ensureDatabase() {
  const ok = await exists(`/databases/${encodeURIComponent(DATABASE_ID)}`);
  if (ok) return;
  await appwriteRequest('/databases', { method: 'POST', body: { databaseId: DATABASE_ID, name: 'TRIP Portal' } });
}

async function ensureCollection() {
  const colPath = `/databases/${encodeURIComponent(DATABASE_ID)}/collections/${encodeURIComponent(USERS_COLLECTION_ID)}`;
  const ok = await exists(colPath);
  if (ok) return;
  await appwriteRequest(`/databases/${encodeURIComponent(DATABASE_ID)}/collections`, {
    method: 'POST',
    body: { collectionId: USERS_COLLECTION_ID, name: 'Users', documentSecurity: false, permissions: [] },
  });
}

async function lockCollectionPermissions() {
  const colPath = `/databases/${encodeURIComponent(DATABASE_ID)}/collections/${encodeURIComponent(USERS_COLLECTION_ID)}`;
  const col = await appwriteRequest(colPath);
  const perms = Array.isArray(col?.permissions) ? col.permissions : [];
  if (perms.length === 0) return;
  await appwriteRequest(colPath, {
    method: 'PUT',
    body: { name: col.name || 'Users', enabled: true, documentSecurity: false, permissions: [] },
  });
}

async function listAttributes() {
  const out = await appwriteRequest(`/databases/${encodeURIComponent(DATABASE_ID)}/collections/${encodeURIComponent(USERS_COLLECTION_ID)}/attributes`);
  return Array.isArray(out?.attributes) ? out.attributes : [];
}

async function getAttribute(key) {
  return appwriteRequest(`/databases/${encodeURIComponent(DATABASE_ID)}/collections/${encodeURIComponent(USERS_COLLECTION_ID)}/attributes/${encodeURIComponent(key)}`);
}

async function waitAvailableAttribute(key) {
  for (let i = 0; i < 30; i++) {
    const a = await getAttribute(key);
    const status = a?.status;
    if (status === 'available') return;
    if (status === 'failed') throw new Error(`Attribute ${key} failed`);
    await sleep(1500);
  }
  throw new Error(`Timed out waiting for attribute ${key}`);
}

async function createStringAttribute(key, size, required) {
  await appwriteRequest(`/databases/${encodeURIComponent(DATABASE_ID)}/collections/${encodeURIComponent(USERS_COLLECTION_ID)}/attributes/string`, {
    method: 'POST',
    body: { key, size, required, default: null, array: false },
  });
  await waitAvailableAttribute(key);
}

async function ensureAttributes() {
  const attrs = await listAttributes();
  const keys = new Set(attrs.map((a) => a.key));
  if (!keys.has('username')) await createStringAttribute('username', 255, true);
  if (!keys.has('data')) {
    try {
      await createStringAttribute('data', 1000000, true);
    } catch {
      await createStringAttribute('data', 200000, true);
    }
  }
}

function hashInt(seed) {
  const h = crypto.createHash('sha256').update(seed).digest();
  return h.readUInt32BE(0);
}

function clamp(min, v, max) {
  return Math.max(min, Math.min(max, v));
}

function makeBookingForUser(username, userData, block) {
  const m = userData.manifest || {};
  const p = userData.profile || {};
  const from = m.from || 'LHR';
  const to = m.to || 'ICN';
  const route = `${from}-${to}`;
  const cls = m.flightClass || 'Business';
  const role = p.category || 'Passenger';
  const status = m.status || 'SCHEDULED';

  const base = hashInt(`${username}:${block}`);
  const hoursAhead = 6 + (base % 67);
  const minutesOffset = hashInt(`${username}:m:${block}`) % 360;
  const ts = Date.now() + hoursAhead * 60 * 60 * 1000 + minutesOffset * 60 * 1000;
  const d = new Date(ts);
  const hrs = String(d.getHours()).padStart(2, '0');
  const mins = String(d.getMinutes()).padStart(2, '0');

  const priceBase = 1400 + (hashInt(`${username}:p:${block}`) % 17200);
  const price = clamp(220, priceBase + (hashInt(`${username}:j:${block}`) % 280) - 120, 25000) + 0.99;

  return {
    id: `ADM-${username}`,
    name: userData.passengerName || username,
    role,
    cls,
    route,
    time: `${hrs}:${mins}`,
    date: d.toLocaleDateString(),
    price,
    status,
  };
}

module.exports = async (req, res) => {
  const parts = Array.isArray(req.query.path) ? req.query.path : (req.query.path ? [req.query.path] : []);
  const scope = parts[0] || '';
  const action = parts[1] || '';

  if (scope === 'admin') {
    if (action === 'config') {
      if (req.method !== 'GET') return send(res, 405, { error: 'Method not allowed' });
      const configured = Boolean(process.env.TRIP_ADMIN_PASSCODE);
      const altConfigured = Boolean(process.env.TRIP_ADMIN_PASSCODE_ALT);
      return send(res, 200, {
        ok: true,
        configured,
        usingDefault: !configured,
        altConfigured,
      });
    }

    if (action === 'login') {
      if (req.method !== 'POST') return send(res, 405, { error: 'Method not allowed' });
      const body = await readJson(req).catch(() => ({}));
      const passcode = String(body.passcode || '').trim();
      const primary = String(process.env.TRIP_ADMIN_PASSCODE || 'Jagaban@1').trim();
      const alt = String(process.env.TRIP_ADMIN_PASSCODE_ALT || 'TRIP2026').trim();
      const configured = Boolean(process.env.TRIP_ADMIN_PASSCODE);
      const usingDefault = !configured;
      if (passcode !== primary && passcode !== alt) return send(res, 401, { ok: false, error: 'Invalid passcode', configured, usingDefault });
      const token = createToken({ typ: 'admin', exp: Date.now() + 8 * 60 * 60 * 1000 });
      res.setHeader('set-cookie', cookieString('trip_admin', token, { maxAgeSeconds: 8 * 60 * 60, secure: isHttps(req) }));
      return send(res, 200, { ok: true });
    }

    if (action === 'logout') {
      res.setHeader('set-cookie', cookieString('trip_admin', '', { maxAgeSeconds: 0, secure: isHttps(req) }));
      return send(res, 200, { ok: true });
    }

    if (action === 'me') {
      const cookies = parseCookies(req);
      const payload = verifyToken(cookies.trip_admin);
      const ok = payload && payload.typ === 'admin';
      return send(res, ok ? 200 : 401, ok ? { ok: true } : { error: 'Unauthorized' });
    }

    const admin = requireAdmin(req, res);
    if (!admin) return;

    if (action === 'schema' && parts[2] === 'ensure') {
      if (req.method === 'GET') {
        const dbOk = await exists(`/databases/${encodeURIComponent(DATABASE_ID)}`);
        const colOk = await exists(`/databases/${encodeURIComponent(DATABASE_ID)}/collections/${encodeURIComponent(USERS_COLLECTION_ID)}`);
        const attrs = colOk ? await listAttributes() : [];
        const hasUsername = attrs.some((a) => a.key === 'username' && a.status === 'available');
        const hasData = attrs.some((a) => a.key === 'data' && a.status === 'available');
        const status = `DB:${dbOk ? 'OK' : 'MISSING'} • COL:${colOk ? 'OK' : 'MISSING'} • username:${hasUsername ? 'OK' : 'MISSING'} • data:${hasData ? 'OK' : 'MISSING'} • perms:LOCKED`;
        return send(res, 200, { status });
      }
      if (req.method !== 'POST') return send(res, 405, { error: 'Method not allowed' });
      try {
        await ensureDatabase();
        await ensureCollection();
        await lockCollectionPermissions();
        await ensureAttributes();
        return send(res, 200, { ok: true });
      } catch (e) {
        return send(res, 500, { error: e?.message || 'Schema ensure failed' });
      }
    }

    if (action === 'users') {
      const username = parts[2] ? String(parts[2]).trim() : '';
      if (!username) {
        if (req.method === 'GET') {
          const docs = await listAllUserDocs(1000);
          const users = {};
          docs.forEach((doc) => {
            const u = parseUserData(doc);
            if (u && doc.username) users[doc.username] = u;
          });
          return send(res, 200, { users });
        }
        if (req.method === 'POST') {
          const body = await readJson(req).catch(() => ({}));
          const u = String(body.username || '').trim();
          const userData = body.userData;
          if (!u || !userData) return send(res, 400, { error: 'Missing username or userData' });
          await upsertUser(u, userData);
          return send(res, 200, { ok: true });
        }
        return send(res, 405, { error: 'Method not allowed' });
      }

      if (req.method === 'GET') {
        const doc = await findUserDocByUsername(username);
        if (!doc) return send(res, 404, { error: 'Not found' });
        return send(res, 200, { username: doc.username, userData: parseUserData(doc) });
      }
      if (req.method === 'PUT' || req.method === 'PATCH') {
        const body = await readJson(req).catch(() => ({}));
        const userData = body.userData;
        if (!userData) return send(res, 400, { error: 'Missing userData' });
        await upsertUser(username, userData);
        return send(res, 200, { ok: true });
      }
      if (req.method === 'DELETE') {
        const ok = await deleteUser(username);
        return send(res, 200, { ok });
      }
      return send(res, 405, { error: 'Method not allowed' });
    }

    return send(res, 404, { error: 'Not found' });
  }

  if (scope === 'user') {
    if (action === 'login') {
      if (req.method !== 'POST') return send(res, 405, { error: 'Method not allowed' });
      const body = await readJson(req).catch(() => ({}));
      const username = String(body.username || '').trim();
      const pin = String(body.pin || '').trim();
      if (!username || !pin) return send(res, 400, { error: 'Missing credentials' });

      const doc = await findUserDocByUsername(username);
      const userData = doc ? parseUserData(doc) : null;
      if (!userData || String(userData.pin) !== String(pin)) return send(res, 401, { ok: false });

      const token = createToken({ typ: 'user', u: doc.username, exp: Date.now() + 6 * 60 * 60 * 1000 });
      res.setHeader('set-cookie', cookieString('trip_session', token, { maxAgeSeconds: 6 * 60 * 60, secure: isHttps(req) }));
      return send(res, 200, { ok: true, username: doc.username });
    }

    if (action === 'logout') {
      res.setHeader('set-cookie', cookieString('trip_session', '', { maxAgeSeconds: 0, secure: isHttps(req) }));
      return send(res, 200, { ok: true });
    }

    if (action === 'me') {
      const payload = requireUser(req, res);
      if (!payload || !payload.u) return;

      const username = String(payload.u);
      const doc = await findUserDocByUsername(username);
      const userData = doc ? parseUserData(doc) : null;
      if (!userData) return send(res, 401, { error: 'Unauthorized' });

      const safe = { ...userData };
      delete safe.pin;
      return send(res, 200, { username: doc.username, userData: safe });
    }

    return send(res, 404, { error: 'Not found' });
  }

  if (scope === 'public') {
    if (action === 'details') {
      if (req.method !== 'GET') return send(res, 405, { error: 'Method not allowed' });
      const username = String(req.query.u || '').trim();
      const tc = String(req.query.tc || '').trim();
      if (!username || !tc) return send(res, 400, { error: 'Missing params' });

      const doc = await findUserDocByUsername(username);
      const userData = doc ? parseUserData(doc) : null;
      if (!userData) return send(res, 404, { error: 'Not found' });

      const share = userData.share || {};
      const oneTime = share.oneTimeTracking || {};
      const codeOk = oneTime.code && String(oneTime.code) === tc && !oneTime.usedAt;
      if (!codeOk) return send(res, 401, { error: 'Invalid or used code' });

      userData.share = userData.share || {};
      userData.share.oneTimeTracking = userData.share.oneTimeTracking || {};
      userData.share.oneTimeTracking.usedAt = new Date().toISOString();
      await upsertUser(doc.username, userData);

      const safe = { ...userData };
      delete safe.pin;
      return send(res, 200, { username: doc.username, userData: safe });
    }

    if (action === 'bookings') {
      if (!requireUser(req, res)) return;
      if (req.method !== 'GET') return send(res, 405, { error: 'Method not allowed' });

      const block = Math.floor(Date.now() / (15 * 60 * 1000));
      const docs = await listAllUserDocs(500);
      const items = docs
        .map((doc) => ({ username: doc.username, userData: parseUserData(doc) }))
        .filter((x) => x.username && x.userData)
        .map((x) => makeBookingForUser(x.username, x.userData, block))
        .sort((a, b) => a.date.localeCompare(b.date) || a.time.localeCompare(b.time));

      return send(res, 200, { items, block });
    }

    return send(res, 404, { error: 'Not found' });
  }

  return send(res, 404, { error: 'Not found' });
};
