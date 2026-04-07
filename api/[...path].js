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

const {
  localUsersPath,
  readLocalUsersSync,
  writeLocalUsersSync,
  listLocalUsersCached,
  findLocalUser,
  upsertLocalUser,
  deleteLocalUser,
} = require('../server_lib/local_users');

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

function getAllowedOrigin(req) {
  const origin = String(req.headers?.origin || '').trim();
  if (!origin) return '';
  if (/^http:\/\/localhost:\d+$/i.test(origin)) return origin;
  if (/^http:\/\/127\.0\.0\.1:\d+$/i.test(origin)) return origin;
  if (/^https:\/\/hybe-portal([a-z0-9-]*)\.vercel\.app$/i.test(origin) || /^https:\/\/hybe-portal\.vercel\.app$/i.test(origin)) return origin;
  return '';
}

function applyCors(req, res) {
  const allow = getAllowedOrigin(req);
  if (!allow) return;
  res.setHeader('access-control-allow-origin', allow);
  res.setHeader('vary', 'Origin');
  res.setHeader('access-control-allow-credentials', 'true');
  res.setHeader('access-control-allow-methods', 'GET,POST,PUT,PATCH,DELETE,OPTIONS');
  res.setHeader('access-control-allow-headers', 'content-type, authorization');
}

function cookieSameSite(req) {
  const origin = String(req.headers?.origin || '').trim();
  const host = String(req.headers?.host || '').trim();
  if (!origin || !host) return 'Lax';
  try {
    const o = new URL(origin);
    if (String(o.host).toLowerCase() !== String(host).toLowerCase()) return 'None';
  } catch {}
  return 'Lax';
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
  const existing = (idx || []).find((i) => i && i.key === key);
  if (existing) return { key, status: String(existing.status || 'exists') };
  try {
    await createIndexFor(collectionId, key, type, attributes, orders);
    return { key, status: 'created' };
  } catch (e) {
    if (e && e.status === 409) return { key, status: 'exists' };
    throw e;
  }
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
  if (!keys.has('data')) await createLargeStringAttributeFor(LIVE_POPUPS_COLLECTION_ID, 'data', false);
  if (!keys.has('updatedAt')) await createStringAttributeFor(LIVE_POPUPS_COLLECTION_ID, 'updatedAt', 64, false);
}

async function ensureKycBucket() {
  if (!storageConfigured()) return { key: 'kyc_bucket', status: 'skipped', reason: 'not_configured' };
  let ok = false;
  try {
    ok = await exists(`/storage/buckets/${encodeURIComponent(KYC_BUCKET_ID)}`);
  } catch (e) {
    if (e && (e.status === 401 || e.status === 403)) return { key: 'kyc_bucket', status: 'forbidden' };
    throw e;
  }
  if (ok) return { key: 'kyc_bucket', status: 'exists' };
  try {
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
    return { key: 'kyc_bucket', status: 'created' };
  } catch (e) {
    if (e && (e.status === 401 || e.status === 403)) return { key: 'kyc_bucket', status: 'forbidden' };
    if (e && e.status === 409) return { key: 'kyc_bucket', status: 'exists' };
    throw e;
  }
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

  const firstNames = ['Isabella', 'Noah', 'Emma', 'Sofia', 'Hassan', 'Marie', 'Rina', 'Owen', 'Clare', 'David', 'Priya', 'Liam', 'Amina', 'Yuna', 'Minho', 'Arjun', 'Zara', 'Mateo', 'Leila', 'Oliver', 'Maya', 'Ethan', 'Ava', 'Lucas', 'Mina', 'Elena', 'Leo', 'Zoe', 'Kenji', 'Sarah'];
  const lastNames = ['Chen', 'Martins', 'Al‑Rashid', 'Dubois', 'Yamamoto', 'Parker', 'Bland', 'Brooks', 'Shah', 'Kim', 'Singh', 'Hernandez', 'Kowalski', 'Nguyen', 'Costa', 'Nakamura', 'Alvarez', 'Patel', 'Watanabe', 'Rahman', 'Schmidt', 'Novak', 'Santos', 'Lee', 'Garcia', 'Moretti', 'Silva', 'Tan', 'Ivanov', 'Müller'];
  
  const positiveReviews = [
    "Finally found a platform that actually teaches you how to trade. Earned my first $500 today!",
    "The educational resources here are top-notch. I've learned more in a week than in months of YouTube.",
    "Invested wisely following the portal's insights and my portfolio is finally green.",
    "Best decision I made this year was joining this trading community. The results speak for themselves.",
    "Earned back my initial investment in just 3 days! This system is incredible.",
    "Highly recommend for anyone serious about learning the markets. Transparent and effective.",
    "The support here is amazing. They actually help you understand the 'why' behind trades.",
    "Solid returns and even better education. My trading psychology has improved so much.",
    "I was skeptical at first, but the results are real. Learned, invested, and earned!",
    "A game changer for retail traders. The tools provided are professional grade.",
    "Started with zero knowledge, now I'm making consistent daily gains. Thank you!",
    "The most comprehensive trading portal I've used. Worth every penny.",
    "Earned $1,200 this week alone. The strategies taught here are pure gold.",
    "So glad I took the leap. The community is supportive and the insights are sharp.",
    "Professional, reliable, and profitable. What more could a trader ask for?",
    "My retirement account is finally growing thanks to the wise investment tips here.",
    "The community is so welcoming and the knowledge sharing is top tier.",
    "Finally, a trading platform that prioritizes user education over hype.",
    "Consistent daily profits have changed my financial outlook completely.",
    "Best trading education I've ever received. The results speak for themselves.",
    "Learned, invested, and now I'm earning. The portal is a game changer.",
    "The real-time market insights are incredibly accurate and helpful.",
    "So grateful for the mentorship. My trading skills have leveled up significantly.",
    "A transparent and reliable community. Highly recommend for any serious trader.",
    "Earned my first $1,000 month following the strategies here. Life-changing!",
    "The risk management tools are worth the admission alone. Saved my account!",
    "Incredible support team. They truly care about your success as a trader.",
    "Simplified my trading process and boosted my profits. Simply the best.",
    "The educational modules are clear, concise, and highly effective.",
    "My trading confidence has never been higher. Thank you for the great tools!",
    "Profitability is finally a reality. The portal's guidance is unmatched.",
    "The best investment I've made in myself. Earning while I learn is amazing.",
    "A solid community of professional traders. The insights are invaluable.",
    "Hit my target for the quarter in just one month. Incredible results!",
    "The portal's market analysis is consistently spot-on. Highly impressed.",
    "My trading journey has been transformed. Earning consistently now.",
    "Grateful for the clear direction and effective tools. A must-have platform.",
    "The mentorship program is outstanding. Learned so much in a short time.",
    "Consistent gains and a wealth of knowledge. This portal is essential.",
    "The community calls provide so much value. Earning and learning together.",
    "My portfolio has never looked better. Thank you for the wise investment tips!",
  ];

  const negativeReviews = [
    "Took me a while to get the hang of it. The first two days were a bit overwhelming.",
    "Lost a small trade today because I didn't follow the rules. Lesson learned, the system works if you do.",
    "The interface took some time to master, but the support team helped me through it.",
    "A bit of a learning curve at the start, but sticking with it paid off.",
    "Had a rough start, but the educational resources helped me turn things around.",
  ];

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
  
  // Helper to pick random element
  const rand = (arr) => arr[Math.floor(Math.random() * arr.length)];

  while (items.length < safeCount) {
    const typeRoll = Math.random();
    let item = null;

    if (typeRoll < 0.25) { // 25% chance for a trader review
      const isPositive = Math.random() < 0.93; // ~28/30 chance for positive
      const who = `${rand(firstNames)} ${rand(lastNames)}`;
      const msg = isPositive ? rand(positiveReviews) : rand(negativeReviews);
      const title = isPositive ? 'Trader Success' : 'Trader Insight';
      const tone = isPositive ? 'ok' : 'warn';
      const key = `rev|${who}|${msg.slice(0, 20)}`;
      
      if (!used.has(key)) {
        used.add(key);
        item = {
          id: `lp_${items.length + 1}`,
          title,
          status: isPositive ? 'EARNED' : 'LEARNED',
          tone,
          message: `${who}: "${msg}"`,
        };
      }
    } else { // 75% chance for flight update
      const who = `${rand(firstNames)} ${rand(lastNames)}`;
      const from = rand(airports);
      let to = rand(airports);
      while (to.iata === from.iata) to = rand(airports);
      const st = rand(statuses);
      const flightNo = genFlightNo();
      const msg = `${who} • ${from.iata} ${from.city} → ${to.iata} ${to.city}`;
      const key = `flt|${who}|${from.iata}|${to.iata}|${st.label}|${flightNo}`;
      
      if (!used.has(key)) {
        used.add(key);
        item = {
          id: `lp_${items.length + 1}`,
          title: 'Live Flight Update',
          status: st.label,
          tone: st.tone,
          flightNo,
          message: msg,
        };
      }
    }

    if (item) items.push(item);
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
  const now = Date.now();
  if (globalThis.__tripLivePopupsCache && globalThis.__tripLivePopupsCache.expiresAt > now) {
    return globalThis.__tripLivePopupsCache.pool;
  }

  try {
    if (!isAppwriteConfigured()) {
      const p = genPool(1200, 7, 30);
      globalThis.__tripLivePopupsCache = { pool: p, expiresAt: now + 5 * 60 * 1000 };
      return p;
    }
    
    // We only ensure collection exists if we fail to fetch the document
    let doc;
    try {
      doc = await appwriteRequest(`/databases/${encodeURIComponent(DATABASE_ID)}/collections/${encodeURIComponent(LIVE_POPUPS_COLLECTION_ID)}/documents/pool`);
    } catch (e) {
      if (e && e.status === 404) {
        await ensureDatabase();
        await ensureLivePopupsCollection();
        // Document missing, it will be created below
      } else {
        throw e;
      }
    }

    const raw = doc && doc.data ? String(doc.data) : '';
    const pool = raw ? JSON.parse(raw) : null;
    if (pool && pool.items && Array.isArray(pool.items) && pool.items.length >= 1000) {
      const until = pool.validUntil ? new Date(pool.validUntil) : null;
      const expired = until && !Number.isNaN(until.getTime()) && Date.now() > until.getTime();
      if (!expired) {
        globalThis.__tripLivePopupsCache = { pool, expiresAt: now + 5 * 60 * 1000 };
        return pool;
      }
      const startAt = pool.startAt ? new Date(pool.startAt) : null;
      const days = (startAt && until && !Number.isNaN(startAt.getTime())) ? Math.max(1, Math.round((until.getTime() - startAt.getTime()) / 86400000)) : 7;
      const intervalSeconds = pool.intervalSeconds !== undefined ? Number(pool.intervalSeconds) : 30;
      const next = genPool(pool.items.length, days, intervalSeconds);
      try { await saveLivePopupsPool(next); } catch {}
      globalThis.__tripLivePopupsCache = { pool: next, expiresAt: now + 5 * 60 * 1000 };
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
  globalThis.__tripLivePopupsCache = { pool, expiresAt: now + 5 * 60 * 1000 };
  return pool;
}

async function saveLivePopupsPool(pool) {
  const payload = { data: JSON.stringify(pool), updatedAt: new Date().toISOString() };
  try {
    await appwriteRequest(`/databases/${encodeURIComponent(DATABASE_ID)}/collections/${encodeURIComponent(LIVE_POPUPS_COLLECTION_ID)}/documents/pool`, {
      method: 'PATCH',
      body: payload,
    });
  } catch {
    await appwriteRequest(`/databases/${encodeURIComponent(DATABASE_ID)}/collections/${encodeURIComponent(LIVE_POPUPS_COLLECTION_ID)}/documents`, {
      method: 'POST',
      body: { documentId: 'pool', data: payload, permissions: [] },
    });
  }
  globalThis.__tripLivePopupsCache = { pool, expiresAt: Date.now() + 5 * 60 * 1000 };
  return true;
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
  try { actions.push(await ensureKycBucket()); } catch {}
  try {
    const idx = await listIndexesFor(USERS_COLLECTION_ID).catch(() => []);
    const hasAnyUsernameKey = (idx || []).some((i) => i && i.type === 'key' && Array.isArray(i.attributes) && i.attributes.length === 1 && String(i.attributes[0]) === 'username');
    if (hasAnyUsernameKey) actions.push({ key: 'idx_username_key', status: 'exists' });
    else actions.push(await ensureIndexFor(USERS_COLLECTION_ID, 'idx_username_key', 'key', ['username'], ['asc']));
  } catch {}
  actions.push(await ensureIndexFor(USERS_COLLECTION_ID, 'idx_username_lc_unique', 'unique', ['username_lc'], []));
  try { actions.push(await ensureIndexFor('submissions', 'idx_sub_username_key', 'key', ['username'], ['asc'])); } catch {}
  try { actions.push(await ensureIndexFor('submissions', 'idx_sub_type_key', 'key', ['type'], ['asc'])); } catch {}
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
  const firstNames = ['Isabella', 'Noah', 'Emma', 'Sofia', 'Hassan', 'Marie', 'Rina', 'Owen', 'Clare', 'David', 'Priya', 'Liam', 'Amina', 'Yuna', 'Minho', 'Arjun', 'Zara', 'Mateo', 'Leila', 'Oliver', 'Maya', 'Ethan', 'Ava', 'Lucas', 'Mina'];
  const lastNames = ['Chen', 'Martins', 'Dubois', 'Yamamoto', 'Parker', 'Brooks', 'Shah', 'Kim', 'Singh', 'Hernandez', 'Kowalski', 'Nguyen', 'Costa', 'Nakamura', 'Alvarez', 'Patel', 'Watanabe', 'Rahman', 'Schmidt', 'Novak', 'Santos', 'Lee', 'Garcia', 'Murphy', 'Lopez'];
  const classes = ['Economy', 'Premium', 'Business', 'First'];
  const statuses = ['PENDING', 'CONFIRMED', 'HOLD', 'SCHEDULED'];

  const base = hashInt(`${username}:${block}`);
  const pick = (arr, salt) => arr[hashInt(`${username}:${salt}:${block}`) % arr.length];
  const fn = pick(firstNames, 'fn');
  const ln = pick(lastNames, 'ln');
  const name = `${fn} ${ln}`;
  const from = pick(airports, 'from');
  let to = pick(airports, 'to');
  if (to.iata === from.iata) to = airports[(airports.findIndex((x) => x.iata === to.iata) + 3) % airports.length];
  const route = `${from.iata} ${from.city} → ${to.iata} ${to.city}`;
  const cls = pick(classes, 'cls');
  const status = pick(statuses, 'st');

  const hoursAhead = 6 + (base % 67);
  const minutesOffset = hashInt(`${username}:m:${block}`) % 360;
  const ts = Date.now() + hoursAhead * 60 * 60 * 1000 + minutesOffset * 60 * 1000;
  const d = new Date(ts);
  const hrs = String(d.getHours()).padStart(2, '0');
  const mins = String(d.getMinutes()).padStart(2, '0');

  const priceBase = 1400 + (hashInt(`${username}:p:${block}`) % 17200);
  const price = clamp(220, priceBase + (hashInt(`${username}:j:${block}`) % 280) - 120, 25000) + 0.99;

  return {
    id: `BK-${String((hashInt(`${username}:id:${block}`) % 9000) + 1000)}`,
    name,
    cls,
    route,
    time: `${hrs}:${mins}`,
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
  applyCors(req, res);
  if (req.method === 'OPTIONS') {
    res.statusCode = 204;
    res.end('');
    return;
  }
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

    if (scope === 'users.json' || action === 'users.json') {
      if (req.method === 'GET') {
        const users = listLocalUsersCached();
        return send(res, 200, users);
      }
      return send(res, 405, { error: 'Method not allowed' });
    }

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
          kvConfigured: Boolean(process.env.KV_REST_API_URL && process.env.KV_REST_API_TOKEN),
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
        if (process.env.VERCEL && usingDefault) {
          return send(res, 503, { ok: false, error: 'Admin passcode not configured', configured, usingDefault });
        }
        if (passcode !== primary && passcode !== alt) return send(res, 401, { ok: false, error: 'Invalid passcode', configured, usingDefault });
        const token = createToken({ typ: 'admin', exp: Date.now() + 8 * 60 * 60 * 1000 });
        res.setHeader('set-cookie', cookieString('trip_admin', token, { maxAgeSeconds: 8 * 60 * 60, secure: isHttps(req), sameSite: cookieSameSite(req) }));
        return send(res, 200, { ok: true });
      }

      if (action === 'logout') {
        res.setHeader('set-cookie', cookieString('trip_admin', '', { maxAgeSeconds: 0, secure: isHttps(req), sameSite: cookieSameSite(req) }));
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
            let idxUsernameOk = false;
            try {
              const idx = colOk ? await listIndexesFor(USERS_COLLECTION_ID) : [];
              idxOk = (idx || []).some((i) => i && i.key === 'idx_username_lc_unique' && i.status === 'available');
              idxUsernameOk = (idx || []).some((i) => i && i.type === 'key' && Array.isArray(i.attributes) && i.attributes.length === 1 && String(i.attributes[0]) === 'username' && i.status === 'available');
            } catch {}
            const status = `DB:${dbOk ? 'OK' : 'MISSING'} • COL:${colOk ? 'OK' : 'MISSING'} • username:${hasUsername ? 'OK' : 'MISSING'} • idx_username_key:${idxUsernameOk ? 'OK' : 'MISSING'} • username_lc:${hasUsernameLc ? 'OK' : 'MISSING'} • idx_username_lc_unique:${idxOk ? 'OK' : 'MISSING'} • data:${hasData ? 'OK' : 'MISSING'} • perms:LOCKED`;
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
          const actions = await schemaSync();
          return send(res, 200, { ok: true, actions, ensuredAt: new Date().toISOString() });
        } catch (e) {
          return send(res, 500, { error: e?.payload?.message || e?.payload?.error || e?.message || 'Schema ensure failed', status: e?.status, details: e?.payload || null });
        }
      }

      if (action === 'users') {
        const username = parts[2] ? String(parts[2]).trim() : '';
        if (!username) {
          if (req.method === 'GET') {
            try {
              const docs = await listAllUserDocs(1000);
              const users = {};
              docs.forEach((doc) => {
                const u = parseUserData(doc);
                if (u && doc.username) users[doc.username] = u;
              });
              return send(res, 200, { users });
            } catch (e) {
              return send(res, 200, { users: {} });
            }
          }
          if (req.method === 'POST') {
            try {
              const body = await readJson(req).catch(() => ({}));
              const u = String(body.username || '').trim();
              const userData = body.userData && typeof body.userData === 'object' ? { ...body.userData } : body.userData;
              if (!u || !userData) return send(res, 400, { error: 'Missing username or userData' });
              
              await upsertUser(u, userData);
              return send(res, 200, { ok: true });
            } catch (e) {
              return send(res, 500, { error: e?.message || 'Upsert failed' });
            }
          }
        } else {
          if (req.method === 'GET') {
            try {
              const doc = await findUserDocByUsername(username);
              if (!doc) return send(res, 404, { error: 'Not found' });
              return send(res, 200, { username: doc.username, userData: parseUserData(doc) });
            } catch {
              return send(res, 404, { error: 'Not found' });
            }
          }
          if (req.method === 'DELETE') {
            try {
              const result = await deleteUser(username);
              const ok = typeof result === 'object' ? Boolean(result.ok) : Boolean(result);
              return send(res, 200, { ok });
            } catch (e) {
              return send(res, 500, { error: e?.message || 'Delete failed' });
            }
          }
        }
        return send(res, 405, { error: 'Method not allowed' });
      }

      if (action === 'submissions') {
        if (req.method === 'GET') {
          try {
            const out = await appwriteRequest(`/databases/${encodeURIComponent(DATABASE_ID)}/collections/submissions/documents?queries[]=${encodeURIComponent('limit(100)')}`);
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
            return send(res, 200, { items: [] });
          }
        }
        return send(res, 405, { error: 'Method not allowed' });
      }

      if (action === 'notifications') {
        if (req.method !== 'GET') return send(res, 405, { error: 'Method not allowed' });
        try {
          const out = await appwriteRequest(`/databases/${encodeURIComponent(DATABASE_ID)}/collections/${encodeURIComponent(NOTIFICATIONS_COLLECTION_ID)}/documents?queries[]=${encodeURIComponent('limit(100)')}`);
          const docs = Array.isArray(out?.documents) ? out.documents : [];
          const items = docs.map((d) => ({
            id: d.$id,
            title: d.title || '',
            message: d.message || '',
            tone: d.tone || 'accent',
            active: d.active !== false,
            createdAt: d.createdAt || d.$createdAt || null,
          }));
          return send(res, 200, { items });
        } catch (e) {
          return send(res, 500, { error: e?.message || 'Notifications load failed' });
        }
      }

      if (action === 'live-popups') {
        if (req.method === 'GET') {
          try {
            const pool = await getLivePopupsPool();
            return send(res, 200, { ok: true, ...pool });
          } catch (e) {
            return send(res, 500, { error: e?.message || 'Live popups load failed' });
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
        const pin = String(body.pin || '').trim();
        if (!username || !pin) return send(res, 400, { error: 'Missing credentials' });

        const local = findLocalUser(username);
        if (local && String(local.pin) === pin) {
          const token = createToken({ typ: 'user', u: username, src: 'local', exp: Date.now() + 6 * 60 * 60 * 1000 });
          res.setHeader('set-cookie', cookieString('trip_session', token, { maxAgeSeconds: 6 * 60 * 60, secure: isHttps(req), sameSite: cookieSameSite(req) }));
          return send(res, 200, { ok: true, username });
        }

        const doc = await findUserDocByUsername(username);
        const userData = doc ? parseUserData(doc) : null;
        if (userData && String(userData.pin) === pin) {
          const token = createToken({ typ: 'user', u: username, exp: Date.now() + 6 * 60 * 60 * 1000 });
          res.setHeader('set-cookie', cookieString('trip_session', token, { maxAgeSeconds: 6 * 60 * 60, secure: isHttps(req), sameSite: cookieSameSite(req) }));
          return send(res, 200, { ok: true, username });
        }

        return send(res, 401, { error: 'Invalid credentials' });
      }

      if (action === 'me') {
        const payload = requireUser(req, res);
        if (!payload) return;
        const doc = await findUserDocByUsername(payload.u);
        if (!doc) return send(res, 404, { error: 'User not found' });
        return send(res, 200, { username: doc.username, userData: parseUserData(doc) });
      }

      return send(res, 404, { error: 'Not found' });
    }

    if (scope === 'public') {
      if (action === 'live-popups') {
        const pool = await getLivePopupsPool();
        return send(res, 200, { ok: true, ...pool });
      }
      return send(res, 404, { error: 'Not found' });
    }

    return send(res, 404, { error: 'Not found' });
  } catch (e) {
    console.error('API Error:', e);
    return send(res, 500, { error: e?.message || 'Internal Server Error' });
  }
};