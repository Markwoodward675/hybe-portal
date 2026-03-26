const { parseCookies, cookieString, isHttps } = require('../../server_lib/cookies');
const { createToken, verifyToken } = require('../../server_lib/token');
const { listAllUserDocs, parseUserData, upsertUser, deleteUser, findUserDocByUsername, appwriteRequest, DATABASE_ID, USERS_COLLECTION_ID } = require('../../server_lib/appwrite');

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

module.exports = async (req, res) => {
  const parts = Array.isArray(req.query.path) ? req.query.path : (req.query.path ? [req.query.path] : []);

  if (parts[0] === 'login') {
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

  if (parts[0] === 'logout') {
    res.setHeader('set-cookie', cookieString('trip_admin', '', { maxAgeSeconds: 0, secure: isHttps(req) }));
    return send(res, 200, { ok: true });
  }

  if (parts[0] === 'me') {
    const cookies = parseCookies(req);
    const payload = verifyToken(cookies.trip_admin);
    const ok = payload && payload.typ === 'admin';
    return send(res, ok ? 200 : 401, { ok: Boolean(ok) });
  }

  const admin = requireAdmin(req, res);
  if (!admin) return;

  if (parts[0] === 'schema' && parts[1] === 'ensure') {
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

  if (parts[0] === 'users') {
    if (parts.length === 1) {
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
        const username = String(body.username || '').trim();
        const userData = body.userData;
        if (!username || !userData) return send(res, 400, { error: 'Missing username or userData' });
        await upsertUser(username, userData);
        return send(res, 200, { ok: true });
      }
      return send(res, 405, { error: 'Method not allowed' });
    }

    const username = String(parts[1] || '').trim();
    if (!username) return send(res, 400, { error: 'Missing username' });

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
};
