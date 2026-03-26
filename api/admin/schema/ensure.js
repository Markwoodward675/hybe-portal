const { parseCookies } = require('../../_lib/cookies');
const { verifyToken } = require('../../_lib/token');
const { appwriteRequest, DATABASE_ID, USERS_COLLECTION_ID } = require('../../_lib/appwrite');

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function requireAdmin(req, res) {
  const cookies = parseCookies(req);
  const payload = verifyToken(cookies.trip_admin);
  if (!payload || payload.typ !== 'admin') {
    res.statusCode = 401;
    res.setHeader('content-type', 'application/json');
    res.end(JSON.stringify({ error: 'Unauthorized' }));
    return false;
  }
  return true;
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
  if (ok) return { created: false };
  await appwriteRequest('/databases', {
    method: 'POST',
    body: { databaseId: DATABASE_ID, name: 'TRIP Portal' },
  });
  return { created: true };
}

async function ensureCollection() {
  const colPath = `/databases/${encodeURIComponent(DATABASE_ID)}/collections/${encodeURIComponent(USERS_COLLECTION_ID)}`;
  const ok = await exists(colPath);
  if (ok) return { created: false };
  await appwriteRequest(`/databases/${encodeURIComponent(DATABASE_ID)}/collections`, {
    method: 'POST',
    body: {
      collectionId: USERS_COLLECTION_ID,
      name: 'Users',
      documentSecurity: false,
      permissions: [],
    },
  });
  return { created: true };
}

async function lockCollectionPermissions() {
  const colPath = `/databases/${encodeURIComponent(DATABASE_ID)}/collections/${encodeURIComponent(USERS_COLLECTION_ID)}`;
  const col = await appwriteRequest(colPath);
  const perms = Array.isArray(col?.permissions) ? col.permissions : [];
  if (perms.length === 0) return { locked: true };
  await appwriteRequest(colPath, {
    method: 'PUT',
    body: {
      name: col.name || 'Users',
      enabled: true,
      documentSecurity: false,
      permissions: [],
    },
  });
  return { locked: true };
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

  if (!keys.has('username')) {
    await createStringAttribute('username', 255, true);
  }

  if (!keys.has('data')) {
    try {
      await createStringAttribute('data', 1000000, true);
    } catch {
      await createStringAttribute('data', 200000, true);
    }
  }
}

module.exports = async (req, res) => {
  if (!requireAdmin(req, res)) return;

  if (req.method === 'GET') {
    const dbOk = await exists(`/databases/${encodeURIComponent(DATABASE_ID)}`);
    const colOk = await exists(`/databases/${encodeURIComponent(DATABASE_ID)}/collections/${encodeURIComponent(USERS_COLLECTION_ID)}`);
    const attrs = colOk ? await listAttributes() : [];
    const hasUsername = attrs.some((a) => a.key === 'username' && a.status === 'available');
    const hasData = attrs.some((a) => a.key === 'data' && a.status === 'available');
    const status = `DB:${dbOk ? 'OK' : 'MISSING'} • COL:${colOk ? 'OK' : 'MISSING'} • username:${hasUsername ? 'OK' : 'MISSING'} • data:${hasData ? 'OK' : 'MISSING'} • perms:LOCKED`;
    res.statusCode = 200;
    res.setHeader('content-type', 'application/json');
    res.end(JSON.stringify({ status }));
    return;
  }

  if (req.method !== 'POST') {
    res.statusCode = 405;
    res.setHeader('content-type', 'application/json');
    res.end(JSON.stringify({ error: 'Method not allowed' }));
    return;
  }

  try {
    await ensureDatabase();
    await ensureCollection();
    await lockCollectionPermissions();
    await ensureAttributes();
    res.statusCode = 200;
    res.setHeader('content-type', 'application/json');
    res.end(JSON.stringify({ ok: true }));
  } catch (e) {
    res.statusCode = 500;
    res.setHeader('content-type', 'application/json');
    res.end(JSON.stringify({ error: e?.message || 'Schema ensure failed' }));
  }
};

