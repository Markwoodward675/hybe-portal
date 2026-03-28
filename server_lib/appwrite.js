const crypto = require('crypto');

function requiredEnv(name) {
  const v = process.env[name];
  return v || '';
}

function normalizeEndpoint(endpoint) {
  const e = String(endpoint || '').replace(/\/+$/, '');
  return e.endsWith('/v1') ? e : `${e}/v1`;
}

const ENDPOINT = normalizeEndpoint(requiredEnv('APPWRITE_ENDPOINT'));
const PROJECT_ID = requiredEnv('APPWRITE_PROJECT_ID');
const API_KEY = requiredEnv('APPWRITE_API_KEY');
const DATABASE_ID = requiredEnv('APPWRITE_DATABASE_ID');
const USERS_COLLECTION_ID = requiredEnv('APPWRITE_COLLECTION_USERS_ID');
const NOTIFICATIONS_COLLECTION_ID = process.env.APPWRITE_COLLECTION_NOTIFICATIONS_ID || 'notifications';

function sha256hex(s) {
  return crypto.createHash('sha256').update(String(s || ''), 'utf8').digest('hex');
}

function authUserIdForUsername(username) {
  const ulc = String(username || '').trim().toLowerCase();
  const h = sha256hex(ulc).slice(0, 24);
  return `u_${h}`;
}

function authEmailForUsername(username) {
  const ulc = String(username || '').trim().toLowerCase().replace(/[^a-z0-9._-]/g, '_');
  return `${ulc}@trip.example`;
}

function authPasswordFromPin(pin) {
  const p = String(pin === undefined || pin === null ? '' : pin).trim();
  if (p.length >= 8) return p;
  if (!p) return `TRIP${sha256hex(Date.now()).slice(0, 8)}`;
  return (p.repeat(8)).slice(0, 8);
}

async function appwriteRequest(path, { method = 'GET', body } = {}) {
  if (!ENDPOINT || !PROJECT_ID || !API_KEY) {
    throw new Error('Appwrite not configured');
  }
  const url = `${ENDPOINT}${path.startsWith('/') ? '' : '/'}${path}`;
  const res = await fetch(url, {
    method,
    headers: {
      'content-type': 'application/json',
      'x-appwrite-project': PROJECT_ID,
      'x-appwrite-key': API_KEY,
    },
    body: body ? JSON.stringify(body) : undefined,
  });

  const text = await res.text();
  let json = null;
  try {
    json = text ? JSON.parse(text) : null;
  } catch {
    json = { raw: text };
  }

  if (!res.ok) {
    const err = new Error(`Appwrite HTTP ${res.status} for ${method} ${url}`);
    err.status = res.status;
    err.payload = json;
    throw err;
  }
  return json;
}

async function ensureAuthUser({ username, pin, name }) {
  const u = String(username || '').trim();
  if (!u) return { ok: false, error: 'Missing username' };
  const userId = authUserIdForUsername(u);
  const email = authEmailForUsername(u);
  const password = authPasswordFromPin(pin);
  const displayName = String(name || '').trim().slice(0, 128);

  let existsUser = false;
  try {
    await appwriteRequest(`/users/${encodeURIComponent(userId)}`);
    existsUser = true;
  } catch (e) {
    if (!(e && e.status === 404)) throw e;
  }

  if (!existsUser) {
    try {
      await appwriteRequest('/users', {
        method: 'POST',
        body: {
          userId,
          email,
          password,
          name: displayName || u,
        },
      });
    } catch (e) {
      if (!(e && e.status === 409)) throw e;
    }
  }

  try {
    if (displayName) {
      await appwriteRequest(`/users/${encodeURIComponent(userId)}/name`, { method: 'PATCH', body: { name: displayName } });
    }
  } catch {}
  try {
    await appwriteRequest(`/users/${encodeURIComponent(userId)}/password`, { method: 'PATCH', body: { password } });
  } catch {}

  return { ok: true, userId, email };
}

async function deleteAuthUser(username) {
  const u = String(username || '').trim();
  if (!u) return { ok: false };
  const userId = authUserIdForUsername(u);
  try {
    await appwriteRequest(`/users/${encodeURIComponent(userId)}`, { method: 'DELETE' });
    return { ok: true, userId };
  } catch (e) {
    if (e && e.status === 404) return { ok: true, userId, missing: true };
    return { ok: false, userId, error: e?.message || 'Delete failed' };
  }
}

function queryEqual(field, value) {
  return `equal("${field}",["${String(value).replaceAll('"', '\\"')}"])`;
}

async function findUserDocByUsername(username) {
  const u = String(username || '').trim();
  if (!u) return null;
  const ulc = u.toLowerCase();

  try {
    const q1 = encodeURIComponent(queryEqual('username_lc', ulc));
    const out1 = await appwriteRequest(`/databases/${encodeURIComponent(DATABASE_ID)}/collections/${encodeURIComponent(USERS_COLLECTION_ID)}/documents?queries[]=${q1}&limit=1`);
    const docs1 = Array.isArray(out1?.documents) ? out1.documents : [];
    if (docs1[0]) return docs1[0];
  } catch {}

  try {
    const q2 = encodeURIComponent(queryEqual('username', u));
    const out2 = await appwriteRequest(`/databases/${encodeURIComponent(DATABASE_ID)}/collections/${encodeURIComponent(USERS_COLLECTION_ID)}/documents?queries[]=${q2}&limit=1`);
    const docs2 = Array.isArray(out2?.documents) ? out2.documents : [];
    if (docs2[0]) return docs2[0];
  } catch {}

  try {
    const all = await listAllUserDocs(1000);
    return all.find((d) => String(d?.username || '').trim().toLowerCase() === ulc) || null;
  } catch {
    return null;
  }
}

async function listAllUserDocs(limit = 1000) {
  const docs = [];
  let cursor = null;

  while (docs.length < limit) {
    const parts = [];
    parts.push(`limit=${Math.min(100, limit - docs.length)}`);
    if (cursor) parts.push(`cursorAfter=${encodeURIComponent(cursor)}`);
    const out = await appwriteRequest(`/databases/${encodeURIComponent(DATABASE_ID)}/collections/${encodeURIComponent(USERS_COLLECTION_ID)}/documents?${parts.join('&')}`);
    const batch = Array.isArray(out?.documents) ? out.documents : [];
    docs.push(...batch);
    if (batch.length === 0) break;
    cursor = batch[batch.length - 1].$id;
  }

  return docs;
}

function parseUserData(doc) {
  const dataStr = doc?.data;
  let parsed = null;
  try {
    parsed = JSON.parse(dataStr);
  } catch {
    parsed = null;
  }
  if (parsed && !parsed.serviceCategory && doc?.service_category) {
    parsed.serviceCategory = String(doc.service_category);
  }
  return parsed;
}

async function upsertUser(username, userData) {
  const safeUserData = userData && typeof userData === 'object' ? { ...userData } : userData;
  if (safeUserData && safeUserData.pin !== undefined && safeUserData.pin !== null) {
    safeUserData.pin = String(safeUserData.pin).trim();
  }
  const cat = userData && (userData.serviceCategory || userData.service_category) ? String(userData.serviceCategory || userData.service_category).toUpperCase() : '';
  const service_category = cat === 'LOGISTICS' ? 'LOGISTICS' : (cat === 'FLIGHT' ? 'FLIGHT' : undefined);
  const existing = await findUserDocByUsername(username);
  if (existing) {
    return appwriteRequest(`/databases/${encodeURIComponent(DATABASE_ID)}/collections/${encodeURIComponent(USERS_COLLECTION_ID)}/documents/${encodeURIComponent(existing.$id)}`, {
      method: 'PATCH',
      body: {
        data: JSON.stringify(safeUserData),
        username_lc: String(username || '').trim().toLowerCase(),
        ...(service_category ? { service_category } : {}),
      },
    });
  }
  const documentId = crypto.randomUUID();
  return appwriteRequest(`/databases/${encodeURIComponent(DATABASE_ID)}/collections/${encodeURIComponent(USERS_COLLECTION_ID)}/documents`, {
    method: 'POST',
    body: {
      documentId,
      data: {
        username,
        username_lc: String(username || '').trim().toLowerCase(),
        data: JSON.stringify(safeUserData),
        ...(service_category ? { service_category } : {}),
      },
      permissions: [],
    },
  });
}

async function deleteUser(username) {
  const u = String(username || '').trim();
  if (!u) return false;
  const ulc = u.toLowerCase();

  let deletedCount = 0;
  for (let round = 0; round < 5; round++) {
    const ids = new Set();
    try {
      const q1 = encodeURIComponent(queryEqual('username_lc', ulc));
      const out1 = await appwriteRequest(`/databases/${encodeURIComponent(DATABASE_ID)}/collections/${encodeURIComponent(USERS_COLLECTION_ID)}/documents?queries[]=${q1}&limit=100`);
      const docs1 = Array.isArray(out1?.documents) ? out1.documents : [];
      docs1.forEach((d) => { if (d && d.$id) ids.add(String(d.$id)); });
    } catch {}

    try {
      const q2 = encodeURIComponent(queryEqual('username', u));
      const out2 = await appwriteRequest(`/databases/${encodeURIComponent(DATABASE_ID)}/collections/${encodeURIComponent(USERS_COLLECTION_ID)}/documents?queries[]=${q2}&limit=100`);
      const docs2 = Array.isArray(out2?.documents) ? out2.documents : [];
      docs2.forEach((d) => { if (d && d.$id) ids.add(String(d.$id)); });
    } catch {}

    if (ids.size === 0) {
      const existing = await findUserDocByUsername(u);
      if (existing && existing.$id) ids.add(String(existing.$id));
    }

    if (ids.size === 0) {
      const auth = await deleteAuthUser(u).catch(() => ({ ok: false }));
      return { ok: deletedCount > 0, deletedCount, remainingCount: 0, auth };
    }

    for (const id of ids) {
      await appwriteRequest(`/databases/${encodeURIComponent(DATABASE_ID)}/collections/${encodeURIComponent(USERS_COLLECTION_ID)}/documents/${encodeURIComponent(id)}`, {
        method: 'DELETE',
      });
      deletedCount += 1;
    }
  }

  try {
    const q1 = encodeURIComponent(queryEqual('username_lc', ulc));
    const out1 = await appwriteRequest(`/databases/${encodeURIComponent(DATABASE_ID)}/collections/${encodeURIComponent(USERS_COLLECTION_ID)}/documents?queries[]=${q1}&limit=1`);
    const docs1 = Array.isArray(out1?.documents) ? out1.documents : [];
    const auth = await deleteAuthUser(u).catch(() => ({ ok: false }));
    if (docs1.length === 0) return { ok: deletedCount > 0, deletedCount, remainingCount: 0, auth };
    return { ok: false, deletedCount, remainingCount: docs1.length, auth };
  } catch {
    const auth = await deleteAuthUser(u).catch(() => ({ ok: false }));
    return { ok: deletedCount > 0, deletedCount, remainingCount: 0, auth };
  }
}

module.exports = {
  DATABASE_ID,
  USERS_COLLECTION_ID,
  NOTIFICATIONS_COLLECTION_ID,
  appwriteRequest,
  ensureAuthUser,
  deleteAuthUser,
  findUserDocByUsername,
  listAllUserDocs,
  parseUserData,
  upsertUser,
  deleteUser,
};
