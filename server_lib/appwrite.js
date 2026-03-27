const crypto = require('crypto');

function requiredEnv(name) {
  const v = process.env[name];
  const strict = Boolean(process.env.VERCEL) || process.env.NODE_ENV === 'production';
  if (!v && strict) throw new Error(`Missing env: ${name}`);
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

function queryEqual(field, value) {
  return `equal("${field}", ["${String(value).replaceAll('"', '\\"')}"])`;
}

async function findUserDocByUsername(username) {
  const q = encodeURIComponent(queryEqual('username', username));
  const out = await appwriteRequest(`/databases/${encodeURIComponent(DATABASE_ID)}/collections/${encodeURIComponent(USERS_COLLECTION_ID)}/documents?queries[]=${q}&limit=1`);
  const docs = Array.isArray(out?.documents) ? out.documents : [];
  return docs[0] || null;
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
  const cat = userData && (userData.serviceCategory || userData.service_category) ? String(userData.serviceCategory || userData.service_category).toUpperCase() : '';
  const service_category = cat === 'LOGISTICS' ? 'LOGISTICS' : (cat === 'FLIGHT' ? 'FLIGHT' : undefined);
  const existing = await findUserDocByUsername(username);
  if (existing) {
    return appwriteRequest(`/databases/${encodeURIComponent(DATABASE_ID)}/collections/${encodeURIComponent(USERS_COLLECTION_ID)}/documents/${encodeURIComponent(existing.$id)}`, {
      method: 'PATCH',
      body: {
        data: JSON.stringify(userData),
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
        data: JSON.stringify(userData),
        ...(service_category ? { service_category } : {}),
      },
      permissions: [],
    },
  });
}

async function deleteUser(username) {
  const existing = await findUserDocByUsername(username);
  if (!existing) return false;
  await appwriteRequest(`/databases/${encodeURIComponent(DATABASE_ID)}/collections/${encodeURIComponent(USERS_COLLECTION_ID)}/documents/${encodeURIComponent(existing.$id)}`, {
    method: 'DELETE',
  });
  return true;
}

module.exports = {
  DATABASE_ID,
  USERS_COLLECTION_ID,
  NOTIFICATIONS_COLLECTION_ID,
  appwriteRequest,
  findUserDocByUsername,
  listAllUserDocs,
  parseUserData,
  upsertUser,
  deleteUser,
};
