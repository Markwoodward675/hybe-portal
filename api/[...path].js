const crypto = require('crypto');
const fs = require('fs');
const path = require('path');
const { parseCookies, cookieString, isHttps } = require('../server_lib/cookies');
const { createToken, verifyToken } = require('../server_lib/token');
const {
  appwriteRequest,
  DATABASE_ID,
  USERS_COLLECTION_ID,
  NOTIFICATIONS_COLLECTION_ID,
  ensureAuthUser,
  findUserDocByUsername,
  listAllUserDocs,
  parseUserData,
  upsertUser,
  deleteUser,
} = require('../server_lib/appwrite');

const LIVE_POPUPS_COLLECTION_ID = process.env.APPWRITE_COLLECTION_LIVE_POPUPS_ID || 'live_popups';
const KYC_BUCKET_ID = process.env.APPWRITE_BUCKET_KYC_ID || 'kyc_uploads';

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function normalizeEndpoint(endpoint) {
  const e = String(endpoint || '').replace(/\/+$/, '');
  return e.endsWith('/v1') ? e : `${e}/v1`;
}

function storageConfigured() {
  return Boolean(process.env.APPWRITE_ENDPOINT && process.env.APPWRITE_PROJECT_ID && process.env.APPWRITE_API_KEY);
}

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

function localUsersPath() {
  return path.resolve(process.cwd(), 'data', 'users.json');
}

function kvConfigured() {
  return Boolean(process.env.KV_REST_API_URL && process.env.KV_REST_API_TOKEN);
}

async function kvFetch(pathname) {
  const base = process.env.KV_REST_API_URL;
  const token = process.env.KV_REST_API_TOKEN;
  const res = await fetch(`${String(base).replace(/\/$/, '')}${pathname}`, {
    headers: { authorization: `Bearer ${token}` },
  });
  const text = await res.text();
  let json = null;
  try { json = text ? JSON.parse(text) : null; } catch { json = { raw: text }; }
  if (!res.ok) {
    const err = new Error(`KV HTTP ${res.status}`);
    err.status = res.status;
    err.payload = json;
    throw err;
  }
  return json;
}

async function kvGetJson(key) {
  const out = await kvFetch(`/get/${encodeURIComponent(key)}`);
  const v = out && out.result !== undefined ? out.result : null;
  if (!v) return null;
  if (typeof v === 'object') return v;
  try { return JSON.parse(String(v)); } catch { return null; }
}

async function kvSetJson(key, value, ttlSeconds) {
  const v = encodeURIComponent(JSON.stringify(value));
  const ttl = Number(ttlSeconds) > 0 ? `?ex=${encodeURIComponent(String(Math.floor(Number(ttlSeconds))))}` : '';
  await kvFetch(`/set/${encodeURIComponent(key)}/${v}${ttl}`);
}

async function kvDel(key) {
  await kvFetch(`/del/${encodeURIComponent(key)}`);
}

function kvUserKey(username) {
  return `trip:user:${String(username || '').trim().toLowerCase()}`;
}

function pinHash(pin) {
  const secret = process.env.TRIP_KV_SALT || process.env.TRIP_JWT_SECRET || 'trip';
  return crypto.createHash('sha256').update(`${secret}:${String(pin || '')}`, 'utf8').digest('hex');
}

function normalizePin(v) {
  return String(v === undefined || v === null ? '' : v).trim();
}

function gen6() {
  return String(Math.floor(100000 + Math.random() * 900000));
}

async function sendEmailViaResend({ to, subject, html }) {
  const apiKey = process.env.RESEND_API_KEY;
  const from = process.env.RESEND_FROM;
  if (!apiKey || !from) return { ok: false, hint: 'Email not configured' };
  const res = await fetch('https://api.resend.com/emails', {
    method: 'POST',
    headers: {
      authorization: `Bearer ${apiKey}`,
      'content-type': 'application/json',
    },
    body: JSON.stringify({
      from,
      to,
      subject,
      html,
    }),
  });
  const text = await res.text();
  let json = null;
  try { json = text ? JSON.parse(text) : null; } catch { json = { raw: text }; }
  if (!res.ok) return { ok: false, hint: 'Email send failed', payload: json };
  return { ok: true, payload: json };
}

function readLocalUsersSync() {
  const p = localUsersPath();
  const raw = fs.readFileSync(p, 'utf8');
  const json = raw ? JSON.parse(raw) : [];
  return Array.isArray(json) ? json : [];
}

function writeLocalUsersSync(items) {
  const p = localUsersPath();
  fs.mkdirSync(path.dirname(p), { recursive: true });
  const tmp = `${p}.tmp`;
  fs.writeFileSync(tmp, JSON.stringify(items, null, 2) + '\n', 'utf8');
  fs.renameSync(tmp, p);
}

function listLocalUsersCached() {
  const cache = globalThis.__tripLocalUsersCache;
  const now = Date.now();
  if (cache && cache.expiresAt && now < cache.expiresAt && Array.isArray(cache.items)) return cache.items;
  try {
    const items = readLocalUsersSync();
    globalThis.__tripLocalUsersCache = { items, expiresAt: now + 30 * 1000 };
    return items;
  } catch {
    globalThis.__tripLocalUsersCache = { items: [], expiresAt: now + 10 * 1000 };
    return [];
  }
}

function findLocalUser(username) {
  const u = String(username || '').trim();
  if (!u) return null;
  const items = listLocalUsersCached();
  const lu = u.toLowerCase();
  return items.find((x) => x && String(x.username || '').trim().toLowerCase() === lu) || null;
}

function upsertLocalUser(username, userData) {
  if (process.env.VERCEL) return { ok: false, hint: 'Local users.json is read-only on Vercel' };
  const u = String(username || '').trim();
  if (!u) return { ok: false, hint: 'Missing username' };
  const pin = userData && userData.pin !== undefined ? String(userData.pin) : '';
  const name = userData && (userData.passengerName || userData.name) ? String(userData.passengerName || userData.name) : '';
  const role = userData && userData.role ? String(userData.role) : 'passenger';
  if (!pin) return { ok: false, hint: 'Missing pin' };
  try {
    let items = [];
    try {
      items = readLocalUsersSync();
    } catch {
      items = [];
    }
    const lu = u.toLowerCase();
    const idx = items.findIndex((x) => x && String(x.username || '').trim().toLowerCase() === lu);
    const existing = idx >= 0 ? items[idx] : null;
    const next = {
      id: existing && existing.id ? String(existing.id) : `u-${crypto.randomUUID().slice(0, 8)}`,
      username: u,
      pin,
      role,
      name,
    };
    if (idx >= 0) items[idx] = next;
    else items.push(next);
    writeLocalUsersSync(items);
    globalThis.__tripLocalUsersCache = { items, expiresAt: Date.now() + 30 * 1000 };
    return { ok: true };
  } catch (e) {
    return { ok: false, hint: e?.message || 'Local users.json write failed' };
  }
}

function deleteLocalUser(username) {
  if (process.env.VERCEL) return { ok: false, hint: 'Local users.json is read-only on Vercel' };
  const u = String(username || '').trim();
  if (!u) return { ok: false, hint: 'Missing username' };
  try {
    let items = [];
    try {
      items = readLocalUsersSync();
    } catch {
      items = [];
    }
    const lu = u.toLowerCase();
    const next = items.filter((x) => String(x && x.username ? x.username : '').trim().toLowerCase() !== lu);
    writeLocalUsersSync(next);
    globalThis.__tripLocalUsersCache = { items: next, expiresAt: Date.now() + 30 * 1000 };
    return { ok: true };
  } catch (e) {
    return { ok: false, hint: e?.message || 'Local users.json delete failed' };
  }
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

async function listAttributesFor(collectionId) {
  const out = await appwriteRequest(`/databases/${encodeURIComponent(DATABASE_ID)}/collections/${encodeURIComponent(collectionId)}/attributes`);
  return Array.isArray(out?.attributes) ? out.attributes : [];
}

async function listIndexesFor(collectionId) {
  const out = await appwriteRequest(`/databases/${encodeURIComponent(DATABASE_ID)}/collections/${encodeURIComponent(collectionId)}/indexes`);
  return Array.isArray(out?.indexes) ? out.indexes : [];
}

async function createIndexFor(collectionId, key, type, attributes, orders) {
  await appwriteRequest(`/databases/${encodeURIComponent(DATABASE_ID)}/collections/${encodeURIComponent(collectionId)}/indexes`, {
    method: 'POST',
    body: {
      key,
      type,
      attributes,
      orders: Array.isArray(orders) ? orders : [],
    },
  });
}

async function ensureIndexFor(collectionId, key, type, attributes, orders) {
  const idx = await listIndexesFor(collectionId).catch(() => []);
  const existsKey = (idx || []).some((i) => i && i.key === key && i.status === 'available');
  if (existsKey) return { key, status: 'exists' };
  await createIndexFor(collectionId, key, type, attributes, orders);
  return { key, status: 'created' };
}

async function getAttribute(key) {
  return appwriteRequest(`/databases/${encodeURIComponent(DATABASE_ID)}/collections/${encodeURIComponent(USERS_COLLECTION_ID)}/attributes/${encodeURIComponent(key)}`);
}

async function getAttributeFor(collectionId, key) {
  return appwriteRequest(`/databases/${encodeURIComponent(DATABASE_ID)}/collections/${encodeURIComponent(collectionId)}/attributes/${encodeURIComponent(key)}`);
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

async function waitAvailableAttributeFor(collectionId, key) {
  for (let i = 0; i < 30; i++) {
    const a = await getAttributeFor(collectionId, key);
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

async function createStringAttributeFor(collectionId, key, size, required) {
  await appwriteRequest(`/databases/${encodeURIComponent(DATABASE_ID)}/collections/${encodeURIComponent(collectionId)}/attributes/string`, {
    method: 'POST',
    body: { key, size, required, default: null, array: false },
  });
  await waitAvailableAttributeFor(collectionId, key);
}

async function createLargeStringAttributeFor(collectionId, key, required) {
  try {
    await createStringAttributeFor(collectionId, key, 1000000, required);
  } catch {
    await createStringAttributeFor(collectionId, key, 200000, required);
  }
}

async function createBooleanAttributeFor(collectionId, key, required) {
  await appwriteRequest(`/databases/${encodeURIComponent(DATABASE_ID)}/collections/${encodeURIComponent(collectionId)}/attributes/boolean`, {
    method: 'POST',
    body: { key, required, default: null, array: false },
  });
  await waitAvailableAttributeFor(collectionId, key);
}

async function updateStringAttributeFor(collectionId, key, size, required) {
  await appwriteRequest(`/databases/${encodeURIComponent(DATABASE_ID)}/collections/${encodeURIComponent(collectionId)}/attributes/string/${encodeURIComponent(key)}`, {
    method: 'PUT',
    body: { key, size, required, default: null, array: false },
  });
  await waitAvailableAttributeFor(collectionId, key);
}

async function ensureStringAttributeFor(collectionId, key, size, required) {
  const attrs = await listAttributesFor(collectionId).catch(() => []);
  const existing = Array.isArray(attrs) ? attrs.find((a) => a.key === key) : null;
  if (!existing) {
    await createStringAttributeFor(collectionId, key, size, required);
    return { action: 'created', key };
  }
  if (existing.type !== 'string') return { action: 'skipped_type', key, type: existing.type };
  const currentSize = Number(existing.size) || 0;
  const desiredSize = Number(size) || 0;
  if (desiredSize > currentSize && currentSize > 0) {
    try {
      await updateStringAttributeFor(collectionId, key, desiredSize, Boolean(existing.required));
      return { action: 'updated_size', key, from: currentSize, to: desiredSize };
    } catch {
      return { action: 'update_failed', key, from: currentSize, to: desiredSize };
    }
  }
  return { action: 'ok', key };
}

async function ensureAttributes() {
  const attrs = await listAttributes();
  const keys = new Set(attrs.map((a) => a.key));
  if (!keys.has('username')) await createStringAttribute('username', 255, true);
  if (!keys.has('username_lc')) await createStringAttribute('username_lc', 255, false);
  if (!keys.has('data')) {
    try {
      await createStringAttribute('data', 1000000, true);
    } catch {
      await createStringAttribute('data', 200000, true);
    }
  }
  if (!keys.has('service_category')) {
    await createStringAttribute('service_category', 24, false);
  }
}

async function ensureSubmissionsCollection() {
  const colId = 'submissions';
  const colPath = `/databases/${encodeURIComponent(DATABASE_ID)}/collections/${encodeURIComponent(colId)}`;
  const ok = await exists(colPath);
  if (!ok) {
    await appwriteRequest(`/databases/${encodeURIComponent(DATABASE_ID)}/collections`, {
      method: 'POST',
      body: {
        collectionId: colId,
        name: 'Submissions',
        documentSecurity: false,
        permissions: [],
      },
    });
  }

  const attrs = await listAttributesFor(colId).catch(() => []);
  const keys = new Set((attrs || []).map((a) => a.key));
  if (!keys.has('username')) await createStringAttributeFor(colId, 'username', 255, true);
  if (!keys.has('type')) await createStringAttributeFor(colId, 'type', 24, true);
  if (!keys.has('title')) await createStringAttributeFor(colId, 'title', 180, false);
  if (!keys.has('status')) await createStringAttributeFor(colId, 'status', 24, false);
  if (!keys.has('submittedAt')) await createStringAttributeFor(colId, 'submittedAt', 64, false);
  if (!keys.has('reviewedAt')) await createStringAttributeFor(colId, 'reviewedAt', 64, false);
  if (!keys.has('data')) await createLargeStringAttributeFor(colId, 'data', false);
  if (!keys.has('signatureDataUrl')) await createLargeStringAttributeFor(colId, 'signatureDataUrl', false);
  if (!keys.has('signatureName')) await createStringAttributeFor(colId, 'signatureName', 180, false);
  if (!keys.has('signatureSignedAt')) await createStringAttributeFor(colId, 'signatureSignedAt', 64, false);
}

async function ensureRegistrationsCollection() {
  const colId = 'registrations';
  const colPath = `/databases/${encodeURIComponent(DATABASE_ID)}/collections/${encodeURIComponent(colId)}`;
  const ok = await exists(colPath);
  if (!ok) {
    await appwriteRequest(`/databases/${encodeURIComponent(DATABASE_ID)}/collections`, {
      method: 'POST',
      body: {
        collectionId: colId,
        name: 'Registrations',
        documentSecurity: false,
        permissions: ['read("any")'],
      },
    });
  }

  const attrs = await listAttributesFor(colId).catch(() => []);
  const keys = new Set((attrs || []).map((a) => a.key));
  if (!keys.has('fullName')) await createStringAttributeFor(colId, 'fullName', 180, true);
  if (!keys.has('email')) await createStringAttributeFor(colId, 'email', 255, true);
  if (!keys.has('phone')) await createStringAttributeFor(colId, 'phone', 80, true);
  if (!keys.has('optionalId')) await createStringAttributeFor(colId, 'optionalId', 120, false);
  if (!keys.has('codeHash')) await createStringAttributeFor(colId, 'codeHash', 80, true);
  if (!keys.has('codeSalt')) await createStringAttributeFor(colId, 'codeSalt', 64, true);
  if (!keys.has('createdAt')) await createStringAttributeFor(colId, 'createdAt', 64, false);
  if (!keys.has('verifiedAt')) await createStringAttributeFor(colId, 'verifiedAt', 64, false);
}

async function ensureNotificationsCollection() {
  const colPath = `/databases/${encodeURIComponent(DATABASE_ID)}/collections/${encodeURIComponent(NOTIFICATIONS_COLLECTION_ID)}`;
  const ok = await exists(colPath);
  if (!ok) {
    await appwriteRequest(`/databases/${encodeURIComponent(DATABASE_ID)}/collections`, {
      method: 'POST',
      body: {
        collectionId: NOTIFICATIONS_COLLECTION_ID,
        name: 'Notifications',
        documentSecurity: false,
        permissions: ['read("any")'],
      },
    });
  }

  const attrs = await listAttributesFor(NOTIFICATIONS_COLLECTION_ID).catch(() => []);
  const keys = new Set((attrs || []).map((a) => a.key));
  if (!keys.has('title')) await createStringAttributeFor(NOTIFICATIONS_COLLECTION_ID, 'title', 120, true);
  if (!keys.has('message')) await createStringAttributeFor(NOTIFICATIONS_COLLECTION_ID, 'message', 2000, true);
  if (!keys.has('tone')) await createStringAttributeFor(NOTIFICATIONS_COLLECTION_ID, 'tone', 24, false);
  if (!keys.has('active')) await createBooleanAttributeFor(NOTIFICATIONS_COLLECTION_ID, 'active', false);
  if (!keys.has('createdAt')) await createStringAttributeFor(NOTIFICATIONS_COLLECTION_ID, 'createdAt', 64, false);
}

async function ensureLivePopupsCollection() {
  const colPath = `/databases/${encodeURIComponent(DATABASE_ID)}/collections/${encodeURIComponent(LIVE_POPUPS_COLLECTION_ID)}`;
  const ok = await exists(colPath);
  if (!ok) {
    await appwriteRequest(`/databases/${encodeURIComponent(DATABASE_ID)}/collections`, {
      method: 'POST',
      body: {
        collectionId: LIVE_POPUPS_COLLECTION_ID,
        name: 'Live Popups',
        documentSecurity: false,
        permissions: ['read("any")'],
      },
    });
  }

  const attrs = await listAttributesFor(LIVE_POPUPS_COLLECTION_ID).catch(() => []);
  const keys = new Set((attrs || []).map((a) => a.key));
  if (!keys.has('data')) await createLargeStringAttributeFor(LIVE_POPUPS_COLLECTION_ID, 'data', true);
  if (!keys.has('updatedAt')) await createStringAttributeFor(LIVE_POPUPS_COLLECTION_ID, 'updatedAt', 64, false);
}

async function ensureKycBucket() {
  if (!storageConfigured()) return;
  const ok = await exists(`/storage/buckets/${encodeURIComponent(KYC_BUCKET_ID)}`);
  if (ok) return;
  await appwriteRequest('/storage/buckets', {
    method: 'POST',
    body: {
      bucketId: KYC_BUCKET_ID,
      name: 'KYC Uploads',
      fileSecurity: true,
      enabled: true,
      maximumFileSize: 20 * 1024 * 1024,
      allowedFileExtensions: ['jpg', 'jpeg', 'png', 'pdf'],
      compression: 'none',
      encryption: false,
      antivirus: false,
      permissions: [],
    },
  });
}

async function uploadKycFile({ filename, contentType, buffer }) {
  if (!storageConfigured()) throw new Error('Storage not configured');
  await ensureKycBucket();
  const endpoint = normalizeEndpoint(process.env.APPWRITE_ENDPOINT);
  const url = `${endpoint}/storage/buckets/${encodeURIComponent(KYC_BUCKET_ID)}/files`;
  const fileId = crypto.randomUUID();

  const form = new FormData();
  form.append('fileId', fileId);
  const blob = new Blob([buffer], { type: contentType || 'application/octet-stream' });
  form.append('file', blob, filename || 'upload.bin');

  const res = await fetch(url, {
    method: 'POST',
    headers: {
      'x-appwrite-project': String(process.env.APPWRITE_PROJECT_ID),
      'x-appwrite-key': String(process.env.APPWRITE_API_KEY),
    },
    body: form,
  });
  const text = await res.text();
  let json = null;
  try { json = text ? JSON.parse(text) : null; } catch { json = { raw: text }; }
  if (!res.ok) {
    const err = new Error(`Appwrite upload failed (${res.status})`);
    err.status = res.status;
    err.payload = json;
    throw err;
  }
  return { fileId: json && (json.$id || json.fileId) ? String(json.$id || json.fileId) : fileId };
}

function genFlightNo() {
  return `TRIP-${String(Math.floor(100 + Math.random() * 900))}`;
}

function pick(arr) {
  return arr[Math.floor(Math.random() * arr.length)];
}

function genPool(count, days, intervalSeconds) {
  const safeCount = clamp(1000, Number(count) || 1200, 50000);
  const safeDays = clamp(1, Number(days) || 7, 30);
  const safeInterval = clamp(10, Number(intervalSeconds) || 30, 600);

  const firstNames = ['Isabella', 'Noah', 'Emma', 'Sofia', 'Hassan', 'Marie', 'Rina', 'Owen', 'Clare', 'David', 'Priya', 'Liam', 'Amina', 'Yuna', 'Minho', 'Arjun', 'Zara', 'Mateo', 'Leila', 'Oliver', 'Maya', 'Ethan', 'Ava', 'Lucas', 'Mina'];
  const lastNames = ['Chen', 'Martins', 'Al‑Rashid', 'Dubois', 'Yamamoto', 'Parker', 'Bland', 'Brooks', 'Shah', 'Kim', 'Singh', 'Hernandez', 'Kowalski', 'Nguyen', 'Costa', 'Nakamura', 'Alvarez', 'Patel', 'Watanabe', 'Rahman', 'Schmidt', 'Novak', 'Santos', 'Lee', 'Garcia'];
  const airports = [
    { iata: 'LHR', city: 'London', country: 'United Kingdom' },
    { iata: 'MAN', city: 'Manchester', country: 'United Kingdom' },
    { iata: 'AMS', city: 'Amsterdam', country: 'Netherlands' },
    { iata: 'CDG', city: 'Paris', country: 'France' },
    { iata: 'FRA', city: 'Frankfurt', country: 'Germany' },
    { iata: 'DXB', city: 'Dubai', country: 'UAE' },
    { iata: 'DOH', city: 'Doha', country: 'Qatar' },
    { iata: 'JFK', city: 'New York', country: 'USA' },
    { iata: 'LAX', city: 'Los Angeles', country: 'USA' },
    { iata: 'ICN', city: 'Seoul/Incheon', country: 'Korea' },
    { iata: 'NRT', city: 'Tokyo/Narita', country: 'Japan' },
    { iata: 'SIN', city: 'Singapore', country: 'Singapore' },
    { iata: 'ZRH', city: 'Zurich', country: 'Switzerland' },
    { iata: 'MAD', city: 'Madrid', country: 'Spain' },
  ];
  const statuses = [
    { label: 'BOARDING', tone: 'warn' },
    { label: 'FINAL CALL', tone: 'bad' },
    { label: 'DEPARTED', tone: 'ok' },
    { label: 'EN ROUTE', tone: 'ok' },
    { label: 'DELAYED', tone: 'warn' },
    { label: 'ARRIVED', tone: 'ok' },
  ];

  const startAt = new Date();
  const endAt = new Date(startAt.getTime() + safeDays * 24 * 60 * 60 * 1000);
  const items = [];
  const used = new Set();
  while (items.length < safeCount) {
    const who = `${pick(firstNames)} ${pick(lastNames)}`;
    const from = pick(airports);
    let to = pick(airports);
    while (to.iata === from.iata) to = pick(airports);
    const st = pick(statuses);
    const flightNo = genFlightNo();
    const msg = `${who} • ${from.iata} ${from.city} → ${to.iata} ${to.city}`;
    const key = `${who}|${from.iata}|${to.iata}|${st.label}|${flightNo}`;
    if (used.has(key)) continue;
    used.add(key);
    items.push({
      id: `lp_${items.length + 1}`,
      title: 'Live Flight Update',
      status: st.label,
      tone: st.tone,
      flightNo,
      message: msg,
    });
  }

  return {
    enabled: true,
    intervalSeconds: safeInterval,
    startAt: startAt.toISOString(),
    validUntil: endAt.toISOString(),
    items,
  };
}

async function getLivePopupsPool() {
  await ensureDatabase();
  await ensureLivePopupsCollection();
  try {
    const doc = await appwriteRequest(`/databases/${encodeURIComponent(DATABASE_ID)}/collections/${encodeURIComponent(LIVE_POPUPS_COLLECTION_ID)}/documents/pool`);
    const raw = doc && doc.data ? String(doc.data) : '';
    const pool = raw ? JSON.parse(raw) : null;
    if (pool && pool.items && Array.isArray(pool.items) && pool.items.length >= 1000) {
      const until = pool.validUntil ? new Date(pool.validUntil) : null;
      const expired = until && !Number.isNaN(until.getTime()) && Date.now() > until.getTime();
      if (!expired) return pool;
      const startAt = pool.startAt ? new Date(pool.startAt) : null;
      const days = (startAt && until && !Number.isNaN(startAt.getTime())) ? Math.max(1, Math.round((until.getTime() - startAt.getTime()) / 86400000)) : 7;
      const intervalSeconds = pool.intervalSeconds !== undefined ? Number(pool.intervalSeconds) : 30;
      const next = genPool(pool.items.length, days, intervalSeconds);
      try { await saveLivePopupsPool(next); } catch {}
      return next;
    }
  } catch {}

  const pool = genPool(1200, 7, 30);
  try {
    await appwriteRequest(`/databases/${encodeURIComponent(DATABASE_ID)}/collections/${encodeURIComponent(LIVE_POPUPS_COLLECTION_ID)}/documents`, {
      method: 'POST',
      body: {
        documentId: 'pool',
        data: { data: JSON.stringify(pool), updatedAt: new Date().toISOString() },
        permissions: [],
      },
    });
  } catch {}
  return pool;
}

async function saveLivePopupsPool(pool) {
  await ensureDatabase();
  await ensureLivePopupsCollection();
  const payload = { data: JSON.stringify(pool), updatedAt: new Date().toISOString() };
  try {
    await appwriteRequest(`/databases/${encodeURIComponent(DATABASE_ID)}/collections/${encodeURIComponent(LIVE_POPUPS_COLLECTION_ID)}/documents/pool`, {
      method: 'PATCH',
      body: payload,
    });
    return true;
  } catch {
    await appwriteRequest(`/databases/${encodeURIComponent(DATABASE_ID)}/collections/${encodeURIComponent(LIVE_POPUPS_COLLECTION_ID)}/documents`, {
      method: 'POST',
      body: { documentId: 'pool', data: payload, permissions: [] },
    });
    return true;
  }
}

async function schemaSync() {
  const actions = [];
  await ensureDatabase();
  await ensureCollection();
  await lockCollectionPermissions();

  actions.push(await ensureStringAttributeFor(USERS_COLLECTION_ID, 'username', 255, true));
  actions.push(await ensureStringAttributeFor(USERS_COLLECTION_ID, 'username_lc', 255, false));
  actions.push(await ensureStringAttributeFor(USERS_COLLECTION_ID, 'data', 1000000, true));
  actions.push(await ensureStringAttributeFor(USERS_COLLECTION_ID, 'service_category', 24, false));

  await ensureRegistrationsCollection();
  await ensureSubmissionsCollection();
  await ensureNotificationsCollection();
  await ensureLivePopupsCollection();
  await ensureKycBucket();
  actions.push(await ensureIndexFor(USERS_COLLECTION_ID, 'idx_username_lc_unique', 'unique', ['username_lc'], []));
  actions.push(await ensureStringAttributeFor(NOTIFICATIONS_COLLECTION_ID, 'title', 120, true));
  actions.push(await ensureStringAttributeFor(NOTIFICATIONS_COLLECTION_ID, 'message', 2000, true));
  actions.push(await ensureStringAttributeFor(NOTIFICATIONS_COLLECTION_ID, 'tone', 24, false));
  actions.push(await ensureStringAttributeFor(NOTIFICATIONS_COLLECTION_ID, 'createdAt', 64, false));

  return actions;
}

function hashInt(seed) {
  const h = crypto.createHash('sha256').update(seed).digest();
  return h.readUInt32BE(0);
}

function sha256Hex(s) {
  return crypto.createHash('sha256').update(String(s || ''), 'utf8').digest('hex');
}

function clamp(min, v, max) {
  return Math.max(min, Math.min(max, v));
}

function isAppwriteConfigured() {
  return Boolean(
    process.env.APPWRITE_ENDPOINT &&
    process.env.APPWRITE_PROJECT_ID &&
    process.env.APPWRITE_API_KEY &&
    process.env.APPWRITE_DATABASE_ID &&
    process.env.APPWRITE_COLLECTION_USERS_ID
  );
}

function devUsersStore() {
  if (!globalThis.__tripDevUsersStore) globalThis.__tripDevUsersStore = {};
  return globalThis.__tripDevUsersStore;
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

async function generateAiPassengers(limit) {
  const max = Math.max(1, Math.min(50, Number(limit) || 30));
  const cached = globalThis.__tripAiPassengersCache;
  if (cached && cached.items && cached.expiresAt && Date.now() < cached.expiresAt && cached.items.length >= max) {
    return cached.items.slice(0, max);
  }

  const fallback = (() => {
    const names = [
      { fullName: 'Aiden Murphy', nationality: 'Irish', gender: 'Male' },
      { fullName: 'Isabella Chen', nationality: 'Chinese', gender: 'Female' },
      { fullName: 'Marie Dubois', nationality: 'French', gender: 'Female' },
      { fullName: 'Hassan Al‑Rashid', nationality: 'Emirati', gender: 'Male' },
      { fullName: 'Rina Yamamoto', nationality: 'Japanese', gender: 'Female' },
      { fullName: 'Owen Parker', nationality: 'British', gender: 'Male' },
      { fullName: 'Ama Mensah', nationality: 'Ghanaian', gender: 'Female' },
      { fullName: 'Sipho Dlamini', nationality: 'South African', gender: 'Male' },
      { fullName: 'Fatima Al‑Hassan', nationality: 'Qatari', gender: 'Female' },
      { fullName: 'Michael Johnson', nationality: 'American', gender: 'Male' },
    ];
    const classes = ['Economy', 'Business', 'First'];
    const out = [];
    for (let i = 0; i < max; i++) {
      const n = names[i % names.length];
      out.push({
        fullName: n.fullName,
        nationality: n.nationality,
        gender: n.gender,
        type: i % 7 === 0 ? 'Child' : 'Adult',
        seat: `${(10 + (i % 24))}${String.fromCharCode(65 + (i % 6))}`,
        flightNo: `TRIP-${100 + (i % 900)}`,
        flightClass: classes[i % classes.length],
      });
    }
    return out;
  })();

  const apiKey = process.env.OPENAI_API_KEY;
  if (!apiKey) {
    globalThis.__tripAiPassengersCache = { items: fallback, expiresAt: Date.now() + 10 * 60 * 1000 };
    return fallback;
  }

  try {
    const model = process.env.OPENAI_MODEL || 'gpt-4o-mini';
    const prompt = `Generate ${max} realistic passenger profiles as strict JSON array.
Each item must contain: fullName, nationality, gender, type, seat, flightNo, flightClass.
Constraints:
- flightNo format: TRIP-XXX (100-999)
- seat like 12A, 14C etc.
- flightClass one of: Economy, Business, First
- nationality should match name plausibly, include African + international mix.
Return ONLY JSON array, no markdown.`;

    const res = await fetch('https://api.openai.com/v1/chat/completions', {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        authorization: `Bearer ${apiKey}`,
      },
      body: JSON.stringify({
        model,
        messages: [
          { role: 'system', content: 'You generate realistic but fictional passenger data.' },
          { role: 'user', content: prompt },
        ],
        temperature: 0.6,
      }),
    });

    const text = await res.text();
    let json = null;
    try {
      json = text ? JSON.parse(text) : null;
    } catch {
      json = null;
    }
    const content = json && json.choices && json.choices[0] && json.choices[0].message ? json.choices[0].message.content : '';
    const arr = content ? JSON.parse(content) : null;
    if (!Array.isArray(arr)) throw new Error('Bad AI output');
    const items = arr.slice(0, max).map((x) => ({
      fullName: String(x.fullName || ''),
      nationality: String(x.nationality || ''),
      gender: String(x.gender || ''),
      type: String(x.type || ''),
      seat: String(x.seat || ''),
      flightNo: String(x.flightNo || ''),
      flightClass: String(x.flightClass || ''),
    })).filter((x) => x.fullName);
    const safe = items.length ? items : fallback;
    globalThis.__tripAiPassengersCache = { items: safe, expiresAt: Date.now() + 10 * 60 * 1000 };
    return safe;
  } catch {
    globalThis.__tripAiPassengersCache = { items: fallback, expiresAt: Date.now() + 10 * 60 * 1000 };
    return fallback;
  }
}

module.exports = async (req, res) => {
  try {
    const q = req.query || {};
    let parts = Array.isArray(q.path) ? q.path : (q.path ? [q.path] : []);
    if (!parts.length) {
      try {
        const u = new URL(req.url || '', `http://${req.headers.host || 'localhost'}`);
        const p = u.pathname || '';
        if (p.startsWith('/api/')) {
          const rest = p.slice('/api/'.length);
          parts = rest.split('/').filter(Boolean);
        }
      } catch {}
    }
    const scope = parts[0] || '';
    const action = parts[1] || '';

    if (scope === 'admin') {
    if (action === 'config') {
      if (req.method !== 'GET') return send(res, 405, { error: 'Method not allowed' });
      const configured = Boolean(process.env.TRIP_ADMIN_PASSCODE);
      const altConfigured = Boolean(process.env.TRIP_ADMIN_PASSCODE_ALT);
      const missing = [];
      ['APPWRITE_ENDPOINT', 'APPWRITE_PROJECT_ID', 'APPWRITE_API_KEY', 'APPWRITE_DATABASE_ID', 'APPWRITE_COLLECTION_USERS_ID', 'TRIP_JWT_SECRET'].forEach((k) => {
        if (!process.env[k]) missing.push(k);
      });
      return send(res, 200, {
        ok: true,
        configured,
        usingDefault: !configured,
        altConfigured,
        appwriteConfigured: isAppwriteConfigured(),
        kvConfigured: kvConfigured(),
        missingEnv: missing,
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
        if (!isAppwriteConfigured()) {
          return send(res, 200, { status: 'Appwrite not configured (dev local mode)' });
        }
        try {
          const dbOk = await exists(`/databases/${encodeURIComponent(DATABASE_ID)}`);
          const colOk = await exists(`/databases/${encodeURIComponent(DATABASE_ID)}/collections/${encodeURIComponent(USERS_COLLECTION_ID)}`);
          const attrs = colOk ? await listAttributes() : [];
          const hasUsername = attrs.some((a) => a.key === 'username' && a.status === 'available');
          const hasUsernameLc = attrs.some((a) => a.key === 'username_lc' && a.status === 'available');
          const hasData = attrs.some((a) => a.key === 'data' && a.status === 'available');
          let idxOk = false;
          try {
            const idx = colOk ? await listIndexesFor(USERS_COLLECTION_ID) : [];
            idxOk = (idx || []).some((i) => i && i.key === 'idx_username_lc_unique' && i.status === 'available');
          } catch {}
          const status = `DB:${dbOk ? 'OK' : 'MISSING'} • COL:${colOk ? 'OK' : 'MISSING'} • username:${hasUsername ? 'OK' : 'MISSING'} • username_lc:${hasUsernameLc ? 'OK' : 'MISSING'} • idx_username_lc_unique:${idxOk ? 'OK' : 'MISSING'} • data:${hasData ? 'OK' : 'MISSING'} • perms:LOCKED`;
          return send(res, 200, { status });
        } catch (e) {
          return send(res, 200, { status: 'Appwrite not available (dev local mode)' });
        }
      }
      if (req.method !== 'POST') return send(res, 405, { error: 'Method not allowed' });
      try {
        if (!isAppwriteConfigured()) {
          return send(res, 200, { ok: true, hint: 'Skipped (Appwrite not configured in dev)' });
        }
        await ensureDatabase();
        await ensureCollection();
        await lockCollectionPermissions();
        await ensureAttributes();
        await ensureRegistrationsCollection();
        await ensureSubmissionsCollection();
        await ensureNotificationsCollection();
        await ensureLivePopupsCollection();
        await ensureKycBucket();
        await ensureIndexFor(USERS_COLLECTION_ID, 'idx_username_lc_unique', 'unique', ['username_lc'], []);
        return send(res, 200, { ok: true });
      } catch (e) {
        return send(res, 500, { error: e?.message || 'Schema ensure failed' });
      }
    }

    if (action === 'schema' && parts[2] === 'inspect') {
      if (req.method !== 'GET') return send(res, 405, { error: 'Method not allowed' });
      try {
        if (!isAppwriteConfigured()) {
          return send(res, 200, { databaseId: null, collections: [], hint: 'Appwrite not configured (dev local mode)' });
        }
        const colsOut = await appwriteRequest(`/databases/${encodeURIComponent(DATABASE_ID)}/collections?limit=100`);
        const cols = Array.isArray(colsOut?.collections) ? colsOut.collections : [];
        const collections = [];
        for (const c of cols) {
          const colId = c.$id || c.collectionId || c.id;
          const attrsOut = await appwriteRequest(`/databases/${encodeURIComponent(DATABASE_ID)}/collections/${encodeURIComponent(colId)}/attributes`).catch(() => ({}));
          const idxOut = await appwriteRequest(`/databases/${encodeURIComponent(DATABASE_ID)}/collections/${encodeURIComponent(colId)}/indexes`).catch(() => ({}));
          const attrs = Array.isArray(attrsOut?.attributes) ? attrsOut.attributes : [];
          const indexes = Array.isArray(idxOut?.indexes) ? idxOut.indexes : [];
          collections.push({
            id: colId,
            name: c.name || '',
            documentSecurity: Boolean(c.documentSecurity),
            attributes: attrs.map((a) => ({
              key: a.key,
              type: a.type,
              status: a.status,
              required: Boolean(a.required),
              array: Boolean(a.array),
              size: a.size,
              format: a.format,
              relatedCollection: a.relatedCollection,
              relationType: a.relationType,
            })),
            indexes: indexes.map((i) => ({
              key: i.key,
              type: i.type,
              status: i.status,
              attributes: i.attributes,
              orders: i.orders,
            })),
          });
        }
        return send(res, 200, { databaseId: DATABASE_ID, collections });
      } catch (e) {
        return send(res, 500, { error: e?.message || 'Schema inspect failed' });
      }
    }

    if (action === 'schema' && parts[2] === 'sync') {
      if (req.method !== 'POST') return send(res, 405, { error: 'Method not allowed' });
      try {
        if (!isAppwriteConfigured()) {
          return send(res, 200, { ok: true, actions: [], syncedAt: new Date().toISOString(), hint: 'Skipped (Appwrite not configured in dev)' });
        }
        const actions = await schemaSync();
        return send(res, 200, { ok: true, actions, syncedAt: new Date().toISOString() });
      } catch (e) {
        return send(res, 500, { error: e?.message || 'Schema sync failed' });
      }
    }

    if (action === 'users') {
      const username = parts[2] ? String(parts[2]).trim() : '';
      if (!username) {
        if (req.method === 'GET') {
          if (!isAppwriteConfigured()) {
            return send(res, 200, { users: devUsersStore(), hint: 'Dev store (Appwrite not configured)' });
          }
          try {
            const docs = await listAllUserDocs(1000);
            const users = {};
            docs.forEach((doc) => {
              const u = parseUserData(doc);
              if (u && doc.username) users[doc.username] = u;
            });
            return send(res, 200, { users });
          } catch {
            return send(res, 200, { users: {} });
          }
        }
        if (req.method === 'POST') {
          if (!isAppwriteConfigured()) {
            if (process.env.VERCEL) {
              return send(res, 503, { error: 'Appwrite not configured. Admin provisioning is unavailable on Vercel without Appwrite env.' });
            }
            const body = await readJson(req).catch(() => ({}));
            const u = String(body.username || '').trim();
            const userData = body.userData;
            if (!u || !userData) return send(res, 400, { error: 'Missing username or userData' });
            devUsersStore()[u] = userData;
            const local = upsertLocalUser(u, userData);
            return send(res, 200, { ok: true, storedIn: 'dev', hint: 'Saved to dev store (Appwrite not configured)', local });
          }
          try {
            const body = await readJson(req).catch(() => ({}));
            const u = String(body.username || '').trim();
            const userData = body.userData && typeof body.userData === 'object' ? { ...body.userData } : body.userData;
            if (!u || !userData) return send(res, 400, { error: 'Missing username or userData' });
            if (userData && typeof userData === 'object') userData.pin = normalizePin(userData.pin);
            try {
              const auth = await ensureAuthUser({ username: u, pin: userData.pin, name: String(userData.passengerName || userData.name || '') });
              if (auth && auth.ok) {
                userData.auth = { userId: auth.userId, email: auth.email };
              }
            } catch {}
            await upsertUser(u, userData);
            const local = upsertLocalUser(u, userData);
            let kv = null;
            if (kvConfigured()) {
              try {
                await kvSetJson(kvUserKey(u), {
                  username: u,
                  name: String(userData.passengerName || userData.name || ''),
                  role: String(userData.role || 'passenger'),
                  serviceCategory: String(userData.serviceCategory || userData.service_category || 'FLIGHT').toUpperCase(),
                  pinHash: pinHash(normalizePin(userData.pin)),
                  updatedAt: new Date().toISOString(),
                }, 30 * 24 * 60 * 60);
                kv = { ok: true };
              } catch (e) {
                kv = { ok: false, hint: e?.message || 'KV write failed' };
              }
            }
            return send(res, 200, { ok: true, storedIn: 'appwrite', local, kv });
          } catch (e) {
            return send(res, 500, { error: e?.message || 'Upsert failed' });
          }
        }
        return send(res, 405, { error: 'Method not allowed' });
      }

      if (req.method === 'GET') {
        if (!isAppwriteConfigured()) {
          const u = devUsersStore()[username];
          if (!u) return send(res, 404, { error: 'Not found' });
          return send(res, 200, { username, userData: u });
        }
        try {
          const doc = await findUserDocByUsername(username);
          if (!doc) return send(res, 404, { error: 'Not found' });
          return send(res, 200, { username: doc.username, userData: parseUserData(doc) });
        } catch {
          return send(res, 404, { error: 'Not found' });
        }
      }
      if (req.method === 'PUT' || req.method === 'PATCH') {
        if (!isAppwriteConfigured()) {
          if (process.env.VERCEL) {
            return send(res, 503, { error: 'Appwrite not configured. Admin updates are unavailable on Vercel without Appwrite env.' });
          }
          const body = await readJson(req).catch(() => ({}));
          const userData = body.userData;
          if (!userData) return send(res, 400, { error: 'Missing userData' });
          devUsersStore()[username] = userData;
          const local = upsertLocalUser(username, userData);
          return send(res, 200, { ok: true, storedIn: 'dev', hint: 'Saved to dev store (Appwrite not configured)', local });
        }
        try {
          const body = await readJson(req).catch(() => ({}));
          const userData = body.userData && typeof body.userData === 'object' ? { ...body.userData } : body.userData;
          if (!userData) return send(res, 400, { error: 'Missing userData' });
          if (userData && typeof userData === 'object') userData.pin = normalizePin(userData.pin);
          try {
            const auth = await ensureAuthUser({ username, pin: userData.pin, name: String(userData.passengerName || userData.name || '') });
            if (auth && auth.ok) {
              userData.auth = { userId: auth.userId, email: auth.email };
            }
          } catch {}
          await upsertUser(username, userData);
          const local = upsertLocalUser(username, userData);
          let kv = null;
          if (kvConfigured()) {
            try {
              await kvSetJson(kvUserKey(username), {
                username,
                name: String(userData.passengerName || userData.name || ''),
                role: String(userData.role || 'passenger'),
                serviceCategory: String(userData.serviceCategory || userData.service_category || 'FLIGHT').toUpperCase(),
                pinHash: pinHash(normalizePin(userData.pin)),
                updatedAt: new Date().toISOString(),
              }, 30 * 24 * 60 * 60);
              kv = { ok: true };
            } catch (e) {
              kv = { ok: false, hint: e?.message || 'KV write failed' };
            }
          }
          return send(res, 200, { ok: true, storedIn: 'appwrite', local, kv });
        } catch (e) {
          return send(res, 500, { error: e?.message || 'Upsert failed' });
        }
      }
      if (req.method === 'DELETE') {
        if (!isAppwriteConfigured()) {
          delete devUsersStore()[username];
          const local = deleteLocalUser(username);
          return send(res, 200, { ok: true, hint: 'Deleted from dev store (Appwrite not configured)', local });
        }
        try {
          const result = await deleteUser(username);
          const ok = typeof result === 'object' ? Boolean(result.ok) : Boolean(result);
          const local = deleteLocalUser(username);
          let kv = null;
          if (kvConfigured()) {
            try {
              await kvDel(kvUserKey(username));
              kv = { ok: true };
            } catch (e) {
              kv = { ok: false, hint: e?.message || 'KV delete failed' };
            }
          }
          return send(res, 200, { ok, result: typeof result === 'object' ? result : null, local, kv });
        } catch (e) {
          return send(res, 500, { error: e?.message || 'Delete failed' });
        }
      }
      return send(res, 405, { error: 'Method not allowed' });
    }

    if (action === 'submissions') {
      if (req.method === 'GET') {
        try {
          const out = await appwriteRequest(`/databases/${encodeURIComponent(DATABASE_ID)}/collections/submissions/documents?limit=100`);
          const docs = Array.isArray(out?.documents) ? out.documents : [];
          const items = docs.map((d) => ({
            username: d.username,
            type: String(d.type || ''),
            id: d.$id,
            status: String(d.status || 'PENDING'),
            submittedAt: d.submittedAt || d.$createdAt || null,
            title: d.title || '',
            signature: d.signatureDataUrl ? { dataUrl: d.signatureDataUrl, name: d.signatureName || '', signedAt: d.signatureSignedAt || null } : null,
          }));
          items.sort((a, b) => String(b.submittedAt || '').localeCompare(String(a.submittedAt || '')));
          return send(res, 200, { items });
        } catch {
          const docs = await listAllUserDocs(1000);
          const items = [];
          docs.forEach((doc) => {
            const u = parseUserData(doc);
            if (!u || !doc.username) return;
            const subs = u.submissions && typeof u.submissions === 'object' ? u.submissions : {};
            ['kyc', 'requests', 'indemnity'].forEach((k) => {
              const arr = subs[k];
              if (!Array.isArray(arr)) return;
              arr.forEach((s) => {
                if (!s || typeof s !== 'object') return;
                items.push({
                  username: doc.username,
                  type: String(s.type || k),
                  id: String(s.id || ''),
                  status: String(s.status || 'PENDING'),
                  submittedAt: s.submittedAt || null,
                  title: s.title || '',
                  signature: s.signature || null,
                });
              });
            });
          });
          items.sort((a, b) => String(b.submittedAt || '').localeCompare(String(a.submittedAt || '')));
          return send(res, 200, { items });
        }
      }

      if (req.method === 'PATCH') {
        const body = await readJson(req).catch(() => ({}));
        const username = String(body.username || '').trim();
        const type = String(body.type || '').toLowerCase();
        const id = String(body.id || '').trim();
        const status = String(body.status || '').toUpperCase();
        const allowedTypes = new Set(['kyc', 'requests', 'indemnity']);
        const allowedStatus = new Set(['PENDING', 'APPROVED', 'REJECTED']);
        if (!username || !allowedTypes.has(type) || !id || !allowedStatus.has(status)) {
          return send(res, 400, { error: 'Invalid payload' });
        }

        try {
          await appwriteRequest(`/databases/${encodeURIComponent(DATABASE_ID)}/collections/submissions/documents/${encodeURIComponent(id)}`, {
            method: 'PATCH',
            body: { status, reviewedAt: new Date().toISOString() },
          });
        } catch {}

        const doc = await findUserDocByUsername(username);
        const userData = doc ? parseUserData(doc) : null;
        if (!userData) return send(res, 404, { error: 'Not found' });
        userData.submissions = userData.submissions && typeof userData.submissions === 'object' ? userData.submissions : {};
        userData.submissions[type] = Array.isArray(userData.submissions[type]) ? userData.submissions[type] : [];
        const idx = userData.submissions[type].findIndex((x) => x && String(x.id) === id);
        if (idx === -1) return send(res, 404, { error: 'Not found' });
        userData.submissions[type][idx] = {
          ...userData.submissions[type][idx],
          status,
          reviewedAt: new Date().toISOString(),
        };
        if (type === 'kyc') {
          userData.kyc = userData.kyc && typeof userData.kyc === 'object' ? userData.kyc : {};
          userData.kyc.status = status;
          userData.kyc.reviewedAt = new Date().toISOString();
        }
        await upsertUser(username, userData);
        return send(res, 200, { ok: true });
      }

      return send(res, 405, { error: 'Method not allowed' });
    }

    if (action === 'notifications') {
      if (req.method === 'GET') {
        try {
          const out = await appwriteRequest(`/databases/${encodeURIComponent(DATABASE_ID)}/collections/${encodeURIComponent(NOTIFICATIONS_COLLECTION_ID)}/documents?limit=100`);
          const docs = Array.isArray(out?.documents) ? out.documents : [];
          const items = docs.map((d) => ({
            id: d.$id,
            title: d.title || '',
            message: d.message || '',
            tone: d.tone || 'accent',
            active: d.active !== false,
            createdAt: d.createdAt || d.$createdAt || null,
            updatedAt: d.$updatedAt || null,
          }));
          items.sort((a, b) => String(b.createdAt || '').localeCompare(String(a.createdAt || '')));
          return send(res, 200, { items });
        } catch (e) {
          return send(res, 500, { error: e?.message || 'Notifications load failed' });
        }
      }

      if (req.method === 'POST') {
        try {
          const body = await readJson(req).catch(() => ({}));
          const title = String(body.title || '').trim();
          const message = String(body.message || '').trim();
          const tone = String(body.tone || 'accent').trim();
          const active = body.active !== false;
          if (!title || !message) return send(res, 400, { error: 'Missing title or message' });
          const documentId = crypto.randomUUID();
          await appwriteRequest(`/databases/${encodeURIComponent(DATABASE_ID)}/collections/${encodeURIComponent(NOTIFICATIONS_COLLECTION_ID)}/documents`, {
            method: 'POST',
            body: {
              documentId,
              data: { title, message, tone, active, createdAt: new Date().toISOString() },
              permissions: [],
            },
          });
          return send(res, 200, { ok: true });
        } catch (e) {
          return send(res, 500, { error: e?.message || 'Notification create failed' });
        }
      }

      if (req.method === 'PATCH') {
        try {
          const body = await readJson(req).catch(() => ({}));
          const id = String(body.id || '').trim();
          if (!id) return send(res, 400, { error: 'Missing id' });
          const patch = {};
          if (body.title !== undefined) patch.title = String(body.title || '').trim();
          if (body.message !== undefined) patch.message = String(body.message || '').trim();
          if (body.tone !== undefined) patch.tone = String(body.tone || 'accent').trim();
          if (body.active !== undefined) patch.active = Boolean(body.active);
          await appwriteRequest(`/databases/${encodeURIComponent(DATABASE_ID)}/collections/${encodeURIComponent(NOTIFICATIONS_COLLECTION_ID)}/documents/${encodeURIComponent(id)}`, {
            method: 'PATCH',
            body: patch,
          });
          return send(res, 200, { ok: true });
        } catch (e) {
          return send(res, 500, { error: e?.message || 'Notification update failed' });
        }
      }

      if (req.method === 'DELETE') {
        try {
          const id = parts[2] ? String(parts[2]).trim() : '';
          if (!id) return send(res, 400, { error: 'Missing id' });
          await appwriteRequest(`/databases/${encodeURIComponent(DATABASE_ID)}/collections/${encodeURIComponent(NOTIFICATIONS_COLLECTION_ID)}/documents/${encodeURIComponent(id)}`, {
            method: 'DELETE',
          });
          return send(res, 200, { ok: true });
        } catch (e) {
          return send(res, 500, { error: e?.message || 'Notification delete failed' });
        }
      }

      return send(res, 405, { error: 'Method not allowed' });
    }

    if (action === 'live-popups') {
      if (req.method === 'GET') {
        try {
          if (!isAppwriteConfigured()) return send(res, 200, { ok: true, ...genPool(1200, 7, 30) });
          const pool = await getLivePopupsPool();
          return send(res, 200, { ok: true, ...pool });
        } catch (e) {
          return send(res, 500, { error: e?.message || 'Live popups load failed' });
        }
      }

      if (req.method === 'POST') {
        try {
          const body = await readJson(req).catch(() => ({}));
          const count = body.count !== undefined ? Number(body.count) : 1200;
          const days = body.days !== undefined ? Number(body.days) : 7;
          const intervalSeconds = body.intervalSeconds !== undefined ? Number(body.intervalSeconds) : 30;
          const pool = genPool(count, days, intervalSeconds);
          if (isAppwriteConfigured()) await saveLivePopupsPool(pool);
          return send(res, 200, { ok: true, stats: { count: pool.items.length, days: Number(days) || 7, intervalSeconds: pool.intervalSeconds } });
        } catch (e) {
          return send(res, 500, { error: e?.message || 'Live popups generate failed' });
        }
      }

      if (req.method === 'PATCH') {
        try {
          const body = await readJson(req).catch(() => ({}));
          const pool = isAppwriteConfigured() ? await getLivePopupsPool() : genPool(1200, 7, 30);
          if (body.enabled !== undefined) pool.enabled = Boolean(body.enabled);
          if (body.intervalSeconds !== undefined) pool.intervalSeconds = clamp(10, Number(body.intervalSeconds) || pool.intervalSeconds, 600);
          if (body.index !== undefined && body.item && typeof body.item === 'object') {
            const idx = clamp(0, Number(body.index) || 0, Math.max(0, pool.items.length - 1));
            pool.items[idx] = { ...pool.items[idx], ...body.item };
          }
          if (body.deleteIndex !== undefined) {
            const idx = clamp(0, Number(body.deleteIndex) || 0, Math.max(0, pool.items.length - 1));
            pool.items.splice(idx, 1);
          }
          if (isAppwriteConfigured()) await saveLivePopupsPool(pool);
          return send(res, 200, { ok: true });
        } catch (e) {
          return send(res, 500, { error: e?.message || 'Live popups update failed' });
        }
      }

      if (req.method === 'DELETE') {
        try {
          const pool = { enabled: false, intervalSeconds: 30, startAt: new Date().toISOString(), validUntil: new Date(Date.now() + 7 * 86400000).toISOString(), items: [] };
          if (isAppwriteConfigured()) await saveLivePopupsPool(pool);
          return send(res, 200, { ok: true });
        } catch (e) {
          return send(res, 500, { error: e?.message || 'Live popups clear failed' });
        }
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
      const pin = normalizePin(body.pin);
      if (!username || !pin) return send(res, 400, { error: 'Missing credentials' });

      const local = findLocalUser(username);
      if (local) {
        if (normalizePin(local.pin) !== pin) return send(res, 401, { ok: false, error: 'Invalid Username or PIN' });
        const token = createToken({ typ: 'user', u: String(local.username || username), src: 'local', exp: Date.now() + 6 * 60 * 60 * 1000 });
        res.setHeader('set-cookie', cookieString('trip_session', token, { maxAgeSeconds: 6 * 60 * 60, secure: isHttps(req) }));
        return send(res, 200, { ok: true, username: String(local.username || username) });
      }

      if (!isAppwriteConfigured()) {
        const u = devUsersStore()[username];
        if (!u || normalizePin(u.pin) !== pin) {
          return send(res, 401, { ok: false, error: 'Invalid Username or PIN' });
        }
        const token = createToken({ typ: 'user', u: username, exp: Date.now() + 6 * 60 * 60 * 1000 });
        res.setHeader('set-cookie', cookieString('trip_session', token, { maxAgeSeconds: 6 * 60 * 60, secure: isHttps(req) }));
        return send(res, 200, { ok: true, username });
      }

      let doc = null;
      let appwriteFailed = false;
      try {
        doc = await findUserDocByUsername(username);
      } catch {
        doc = null;
        appwriteFailed = true;
      }
      if (doc) {
        const userData = parseUserData(doc);
        if (!userData || normalizePin(userData.pin) !== pin) {
          return send(res, 401, { ok: false, error: 'Invalid Username or PIN' });
        }
        const token = createToken({ typ: 'user', u: doc.username, exp: Date.now() + 6 * 60 * 60 * 1000 });
        res.setHeader('set-cookie', cookieString('trip_session', token, { maxAgeSeconds: 6 * 60 * 60, secure: isHttps(req) }));
        return send(res, 200, { ok: true, username: doc.username });
      }

      if (!appwriteFailed) {
        return send(res, 401, { ok: false, error: 'Invalid Username or PIN' });
      }

      if (kvConfigured()) {
        try {
          const cached = await kvGetJson(kvUserKey(username));
          const ok = cached && cached.pinHash && cached.pinHash === pinHash(pin);
          if (!ok) return send(res, 401, { ok: false, error: 'Invalid Username or PIN' });
          const token = createToken({ typ: 'user', u: username, src: 'kv', exp: Date.now() + 6 * 60 * 60 * 1000 });
          res.setHeader('set-cookie', cookieString('trip_session', token, { maxAgeSeconds: 6 * 60 * 60, secure: isHttps(req) }));
          return send(res, 200, { ok: true, username });
        } catch {
          return send(res, 503, { ok: false, error: 'Login service unavailable' });
        }
      }

      return send(res, 503, { ok: false, error: 'Login service unavailable' });
    }

    if (action === 'logout') {
      res.setHeader('set-cookie', cookieString('trip_session', '', { maxAgeSeconds: 0, secure: isHttps(req) }));
      return send(res, 200, { ok: true });
    }

    if (action === 'me') {
      const payload = requireUser(req, res);
      if (!payload || !payload.u) return;

      const username = String(payload.u);
      if (payload.src === 'local') {
        const local = findLocalUser(username);
        if (!local) return send(res, 401, { error: 'Unauthorized' });
        return send(res, 200, {
          username,
          userData: {
            username,
            passengerName: String(local.name || ''),
            role: String(local.role || 'user'),
            serviceCategory: String(local.serviceCategory || 'FLIGHT').toUpperCase(),
          },
        });
      }
      if (payload.src === 'kv' && kvConfigured()) {
        try {
          const cached = await kvGetJson(kvUserKey(username));
          if (!cached) return send(res, 401, { error: 'Unauthorized' });
          return send(res, 200, {
            username,
            userData: {
              username,
              passengerName: String(cached.name || ''),
              role: String(cached.role || 'user'),
              serviceCategory: String(cached.serviceCategory || 'FLIGHT').toUpperCase(),
            },
          });
        } catch {
          return send(res, 401, { error: 'Unauthorized' });
        }
      }
      if (!isAppwriteConfigured()) {
        const u = devUsersStore()[username];
        if (!u) return send(res, 401, { error: 'Unauthorized' });
        const safe = { ...u };
        delete safe.pin;
        return send(res, 200, { username, userData: safe });
      }
      const doc = await findUserDocByUsername(username);
      const userData = doc ? parseUserData(doc) : null;
      if (!userData) return send(res, 401, { error: 'Unauthorized' });

      const safe = { ...userData };
      delete safe.pin;
      return send(res, 200, { username: doc.username, userData: safe });
    }

    if (action === 'verify-email') {
      const payload = requireUser(req, res);
      if (!payload || !payload.u) return;
      const username = String(payload.u);

      if (!kvConfigured()) return send(res, 500, { error: 'Verification service unavailable' });

      if (req.method === 'POST') {
        const body = await readJson(req).catch(() => ({}));
        const email = String(body.email || '').trim();
        if (!email || !email.includes('@')) return send(res, 400, { error: 'Invalid email' });
        const code = gen6();
        const codeHash = pinHash(code);
        const key = `trip:emailverify:${username.toLowerCase()}`;
        await kvSetJson(key, { username, email, codeHash, createdAt: new Date().toISOString() }, 10 * 60);
        const mail = await sendEmailViaResend({
          to: email,
          subject: 'TRIP Verification Code',
          html: `<div style="font-family:system-ui;line-height:1.4"><div style="font-weight:800;font-size:18px">TRIP Verification</div><div style="margin-top:12px">Your code is:</div><div style="margin-top:10px;font-size:28px;font-weight:900;letter-spacing:4px">${code}</div><div style="margin-top:14px;color:#555">This code expires in 10 minutes.</div></div>`,
        });
        if (!mail.ok) {
          if (!process.env.VERCEL) return send(res, 200, { ok: true, debugCode: code, hint: mail.hint });
          return send(res, 500, { error: mail.hint || 'Email send failed' });
        }
        return send(res, 200, { ok: true });
      }

      if (req.method === 'PUT') {
        const body = await readJson(req).catch(() => ({}));
        const code = String(body.code || '').trim();
        if (!code) return send(res, 400, { error: 'Missing code' });
        const key = `trip:emailverify:${username.toLowerCase()}`;
        const rec = await kvGetJson(key);
        if (!rec) return send(res, 400, { error: 'Code expired' });
        if (String(rec.codeHash) !== pinHash(code)) return send(res, 401, { error: 'Invalid code' });

        if (isAppwriteConfigured()) {
          const doc = await findUserDocByUsername(username);
          const userData = doc ? parseUserData(doc) : null;
          if (userData) {
            userData.profile = userData.profile && typeof userData.profile === 'object' ? userData.profile : {};
            userData.profile.email = String(rec.email || '');
            userData.profile.emailVerified = true;
            userData.profile.emailVerifiedAt = new Date().toISOString();
            await upsertUser(username, userData);
          }
        }

        await kvDel(key);
        return send(res, 200, { ok: true });
      }

      if (req.method === 'PATCH') {
        const key = `trip:emailverify:${username.toLowerCase()}`;
        const rec = await kvGetJson(key);
        if (!rec || !rec.email) return send(res, 400, { error: 'No pending verification' });
        const code = gen6();
        const codeHash = pinHash(code);
        await kvSetJson(key, { ...rec, codeHash, resentAt: new Date().toISOString() }, 10 * 60);
        const mail = await sendEmailViaResend({
          to: String(rec.email),
          subject: 'TRIP Verification Code (Resent)',
          html: `<div style="font-family:system-ui;line-height:1.4"><div style="font-weight:800;font-size:18px">TRIP Verification</div><div style="margin-top:12px">Your new code is:</div><div style="margin-top:10px;font-size:28px;font-weight:900;letter-spacing:4px">${code}</div><div style="margin-top:14px;color:#555">This code expires in 10 minutes.</div></div>`,
        });
        if (!mail.ok) {
          if (!process.env.VERCEL) return send(res, 200, { ok: true, debugCode: code, hint: mail.hint });
          return send(res, 500, { error: mail.hint || 'Email send failed' });
        }
        return send(res, 200, { ok: true });
      }

      return send(res, 405, { error: 'Method not allowed' });
    }

    if (action === 'form') {
      if (req.method !== 'POST') return send(res, 405, { error: 'Method not allowed' });
      const payload = requireUser(req, res);
      if (!payload || !payload.u) return;

      const body = await readJson(req).catch(() => ({}));
      const signatureDataUrl = String(body.signatureDataUrl || '');
      const name = String(body.name || '').trim();
      if (!signatureDataUrl.startsWith('data:image/png;base64,') || !name) {
        return send(res, 400, { error: 'Invalid payload' });
      }

      const username = String(payload.u);
      const doc = await findUserDocByUsername(username);
      const userData = doc ? parseUserData(doc) : null;
      if (!userData) return send(res, 401, { error: 'Unauthorized' });

      userData.form = userData.form && typeof userData.form === 'object' ? userData.form : {};
      userData.form.signature = {
        dataUrl: signatureDataUrl,
        name,
        signedAt: new Date().toISOString(),
      };

      await upsertUser(doc.username, userData);
      return send(res, 200, { ok: true });
    }

    if (action === 'kyc-upload') {
      if (req.method !== 'POST') return send(res, 405, { error: 'Method not allowed' });
      const payload = requireUser(req, res);
      if (!payload || !payload.u) return;

      const body = await readJson(req).catch(() => ({}));
      const filename = String(body.filename || '').trim() || 'kyc_upload';
      const contentType = String(body.contentType || '').trim() || 'application/octet-stream';
      const dataBase64 = String(body.dataBase64 || '').trim();
      if (!dataBase64) return send(res, 400, { error: 'Missing data' });

      const sizeBytes = Math.floor((dataBase64.length * 3) / 4);
      if (sizeBytes > 10 * 1024 * 1024) return send(res, 413, { error: 'File too large' });

      try {
        const buffer = Buffer.from(dataBase64, 'base64');
        const out = await uploadKycFile({ filename, contentType, buffer });
        return send(res, 200, { ok: true, fileId: out.fileId, bucketId: KYC_BUCKET_ID });
      } catch (e) {
        return send(res, 500, { error: e?.payload?.message || e?.message || 'Upload failed' });
      }
    }

    if (action === 'submissions') {
      const payload = requireUser(req, res);
      if (!payload || !payload.u) return;
      const username = String(payload.u);
      const doc = await findUserDocByUsername(username);
      const userData = doc ? parseUserData(doc) : null;
      if (!userData) return send(res, 401, { error: 'Unauthorized' });

      userData.submissions = userData.submissions && typeof userData.submissions === 'object' ? userData.submissions : {};
      userData.submissions.kyc = Array.isArray(userData.submissions.kyc) ? userData.submissions.kyc : [];
      userData.submissions.requests = Array.isArray(userData.submissions.requests) ? userData.submissions.requests : [];
      userData.submissions.indemnity = Array.isArray(userData.submissions.indemnity) ? userData.submissions.indemnity : [];

      if (req.method === 'GET') {
        return send(res, 200, { submissions: userData.submissions });
      }

      if (req.method === 'POST') {
        const body = await readJson(req).catch(() => ({}));
        const type = String(body.type || '').toLowerCase();
        const allowedTypes = new Set(['kyc', 'requests', 'indemnity']);
        if (!allowedTypes.has(type)) return send(res, 400, { error: 'Invalid type' });

        const data = body.data && typeof body.data === 'object' ? body.data : {};
        const signatureDataUrl = body.signatureDataUrl ? String(body.signatureDataUrl) : '';
        const signatureName = body.signatureName ? String(body.signatureName).trim() : '';
        const needsSig = type === 'kyc' || type === 'indemnity';
        if (needsSig) {
          if (!signatureDataUrl.startsWith('data:image/png;base64,')) return send(res, 400, { error: 'Signature required' });
          if (!signatureName) return send(res, 400, { error: 'Signature name required' });
        }

        const id = crypto.randomUUID();
        const item = {
          id,
          type,
          status: 'PENDING',
          submittedAt: new Date().toISOString(),
          title: String(body.title || ''),
          data,
          ...(needsSig
            ? { signature: { dataUrl: signatureDataUrl, name: signatureName, signedAt: new Date().toISOString() } }
            : {}),
        };

        userData.submissions[type].unshift(item);
        await upsertUser(doc.username, userData);

        try {
          await appwriteRequest(`/databases/${encodeURIComponent(DATABASE_ID)}/collections/submissions/documents`, {
            method: 'POST',
            body: {
              documentId: id,
              data: {
                username: doc.username,
                type,
                title: item.title,
                status: item.status,
                submittedAt: item.submittedAt,
                data: JSON.stringify(data),
                ...(needsSig ? { signatureDataUrl, signatureName, signatureSignedAt: item.signature.signedAt } : {}),
              },
              permissions: [],
            },
          });
        } catch {}

        return send(res, 200, { ok: true, id });
      }

      return send(res, 405, { error: 'Method not allowed' });
    }

    return send(res, 404, { error: 'Not found' });
  }

    if (scope === 'public') {
    if (action === 'verify' && parts[2] === 'request') {
      if (req.method !== 'POST') return send(res, 405, { error: 'Method not allowed' });
      const body = await readJson(req).catch(() => ({}));
      const fullName = String(body.fullName || '').trim();
      const email = String(body.email || '').trim();
      const phone = String(body.phone || '').trim();
      const optionalId = String(body.optionalId || '').trim();
      if (!fullName || !email || !phone) return send(res, 400, { error: 'Missing fields' });

      const code = String(Math.floor(100000 + Math.random() * 900000));
      const salt = crypto.randomBytes(16).toString('hex');
      const codeHash = sha256Hex(`${salt}:${code}`);
      const requestId = crypto.randomUUID();
      const createdAt = new Date().toISOString();

      if (!isAppwriteConfigured()) {
        globalThis.__tripRegistrations = globalThis.__tripRegistrations || new Map();
        globalThis.__tripRegistrations.set(requestId, { fullName, email, phone, optionalId, codeHash, salt, createdAt, verifiedAt: null });
        return send(res, 200, { ok: true, requestId, debugCode: code });
      }

      await ensureRegistrationsCollection();
      await appwriteRequest(`/databases/${encodeURIComponent(DATABASE_ID)}/collections/registrations/documents`, {
        method: 'POST',
        body: {
          documentId: requestId,
          data: { fullName, email, phone, optionalId, codeHash, codeSalt: salt, createdAt, verifiedAt: null },
          permissions: [],
        },
      });

      const debugCode = process.env.VERCEL ? undefined : code;
      return send(res, 200, { ok: true, requestId, ...(debugCode ? { debugCode } : {}) });
    }

    if (action === 'verify' && parts[2] === 'confirm') {
      if (req.method !== 'POST') return send(res, 405, { error: 'Method not allowed' });
      const body = await readJson(req).catch(() => ({}));
      const requestId = String(body.requestId || '').trim();
      const code = String(body.code || '').trim();
      if (!requestId || !code) return send(res, 400, { error: 'Missing fields' });

      const verifiedAt = new Date().toISOString();
      if (!isAppwriteConfigured()) {
        const m = globalThis.__tripRegistrations;
        const it = m && m.get ? m.get(requestId) : null;
        if (!it) return send(res, 404, { error: 'Not found' });
        const ok = sha256Hex(`${it.salt}:${code}`) === it.codeHash;
        if (!ok) return send(res, 401, { error: 'Invalid code' });
        it.verifiedAt = verifiedAt;
        m.set(requestId, it);
        return send(res, 200, { ok: true });
      }

      const doc = await appwriteRequest(`/databases/${encodeURIComponent(DATABASE_ID)}/collections/registrations/documents/${encodeURIComponent(requestId)}`, {
        method: 'GET',
      });
      const salt = String(doc.codeSalt || '');
      const expected = String(doc.codeHash || '');
      const ok = sha256Hex(`${salt}:${code}`) === expected;
      if (!ok) return send(res, 401, { error: 'Invalid code' });
      await appwriteRequest(`/databases/${encodeURIComponent(DATABASE_ID)}/collections/registrations/documents/${encodeURIComponent(requestId)}`, {
        method: 'PATCH',
        body: { verifiedAt },
      });
      return send(res, 200, { ok: true });
    }

    if (action === 'ai' && parts[2] === 'passengers') {
      if (req.method !== 'GET') return send(res, 405, { error: 'Method not allowed' });
      const limit = req.query.limit;
      const items = await generateAiPassengers(limit);
      return send(res, 200, { items });
    }

    if (action === 'notifications') {
      if (req.method !== 'GET') return send(res, 405, { error: 'Method not allowed' });
      try {
        const out = await appwriteRequest(`/databases/${encodeURIComponent(DATABASE_ID)}/collections/${encodeURIComponent(NOTIFICATIONS_COLLECTION_ID)}/documents?limit=100`);
        const docs = Array.isArray(out?.documents) ? out.documents : [];
        const items = docs
          .map((d) => ({
            id: d.$id,
            title: d.title || '',
            message: d.message || '',
            tone: d.tone || 'accent',
            active: d.active !== false,
            createdAt: d.createdAt || d.$createdAt || null,
          }))
          .filter((x) => x.active);
        items.sort((a, b) => String(b.createdAt || '').localeCompare(String(a.createdAt || '')));
        return send(res, 200, { items });
      } catch {
        return send(res, 200, { items: [] });
      }
    }

    if (action === 'live-popups') {
      if (req.method !== 'GET') return send(res, 405, { error: 'Method not allowed' });
      try {
        if (!isAppwriteConfigured()) {
          const pool = genPool(1200, 7, 30);
          return send(res, 200, { ok: true, ...pool });
        }
        const pool = await getLivePopupsPool();
        return send(res, 200, { ok: true, ...pool });
      } catch {
        const pool = genPool(1200, 7, 30);
        return send(res, 200, { ok: true, ...pool });
      }
    }

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

    if (action === 'boardingpass') {
      if (req.method !== 'GET') return send(res, 405, { error: 'Method not allowed' });
      const username = String(req.query.u || '').trim();
      const key = String(req.query.k || '').trim();
      if (!username || !key) return send(res, 400, { error: 'Missing params' });

      const doc = await findUserDocByUsername(username);
      const userData = doc ? parseUserData(doc) : null;
      if (!userData) return send(res, 404, { error: 'Not found' });

      const legacyShare = userData.share || {};
      const flightShare = userData.flight && userData.flight.share ? userData.flight.share : {};
      const stored = flightShare.boardingPassKey || legacyShare.boardingPassKey || '';
      const ok = stored && String(stored) === key;
      if (!ok) return send(res, 401, { error: 'Invalid key' });

      const safe = { ...userData };
      delete safe.pin;
      return send(res, 200, { username: doc.username, userData: safe });
    }

    if (action === 'eticket') {
      if (req.method !== 'GET') return send(res, 405, { error: 'Method not allowed' });
      const username = String(req.query.u || '').trim();
      const key = String(req.query.k || '').trim();
      if (!username || !key) return send(res, 400, { error: 'Missing params' });

      const doc = await findUserDocByUsername(username);
      const userData = doc ? parseUserData(doc) : null;
      if (!userData) return send(res, 404, { error: 'Not found' });

      const flightShare = userData.flight && userData.flight.share ? userData.flight.share : {};
      const stored = flightShare.eticketKey || '';
      const ok = stored && String(stored) === key;
      if (!ok) return send(res, 401, { error: 'Invalid key' });

      const safe = { ...userData };
      delete safe.pin;
      return send(res, 200, { username: doc.username, userData: safe });
    }

    if (action === 'earrival') {
      if (req.method !== 'GET') return send(res, 405, { error: 'Method not allowed' });
      const username = String(req.query.u || '').trim();
      const key = String(req.query.k || '').trim();
      if (!username || !key) return send(res, 400, { error: 'Missing params' });

      const doc = await findUserDocByUsername(username);
      const userData = doc ? parseUserData(doc) : null;
      if (!userData) return send(res, 404, { error: 'Not found' });

      const flightShare = userData.flight && userData.flight.share ? userData.flight.share : {};
      const stored = flightShare.earrivalKey || '';
      const ok = stored && String(stored) === key;
      if (!ok) return send(res, 401, { error: 'Invalid key' });

      const safe = { ...userData };
      delete safe.pin;
      return send(res, 200, { username: doc.username, userData: safe });
    }

    if (action === 'logistics') {
      if (req.method !== 'GET') return send(res, 405, { error: 'Method not allowed' });
      const username = String(req.query.u || '').trim();
      const tc = String(req.query.tc || '').trim();
      if (!username || !tc) return send(res, 400, { error: 'Missing params' });

      const doc = await findUserDocByUsername(username);
      const userData = doc ? parseUserData(doc) : null;
      if (!userData) return send(res, 404, { error: 'Not found' });

      const share = userData.share || {};
      const oneTime = share.oneTimeLogistics || {};
      const codeOk = oneTime.code && String(oneTime.code) === tc && !oneTime.usedAt;
      if (!codeOk) return send(res, 401, { error: 'Invalid or expired link' });

      userData.share = userData.share || {};
      userData.share.oneTimeLogistics = userData.share.oneTimeLogistics || {};
      userData.share.oneTimeLogistics.usedAt = new Date().toISOString();
      await upsertUser(doc.username, userData);

      const steps = userData.logistics && Array.isArray(userData.logistics.steps) ? userData.logistics.steps : [];
      return send(res, 200, {
        username: doc.username,
        passengerName: userData.passengerName || doc.username,
        manifest: userData.logisticsManifest && typeof userData.logisticsManifest === 'object' ? userData.logisticsManifest : {},
        steps,
      });
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
  } catch (e) {
    try {
      if (!res.headersSent) {
        return send(res, 500, { error: e?.message || 'Internal Server Error' });
      }
    } catch {}
    try { res.end(); } catch {}
    return;
  }
};
