const crypto = require('crypto');
const { parseCookies, cookieString, isHttps } = require('../server_lib/cookies');
const { createToken, verifyToken } = require('../server_lib/token');
const {
  appwriteRequest,
  DATABASE_ID,
  USERS_COLLECTION_ID,
  NOTIFICATIONS_COLLECTION_ID,
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

async function listAttributesFor(collectionId) {
  const out = await appwriteRequest(`/databases/${encodeURIComponent(DATABASE_ID)}/collections/${encodeURIComponent(collectionId)}/attributes`);
  return Array.isArray(out?.attributes) ? out.attributes : [];
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

async function schemaSync() {
  const actions = [];
  await ensureDatabase();
  await ensureCollection();
  await lockCollectionPermissions();

  actions.push(await ensureStringAttributeFor(USERS_COLLECTION_ID, 'username', 255, true));
  actions.push(await ensureStringAttributeFor(USERS_COLLECTION_ID, 'data', 1000000, true));
  actions.push(await ensureStringAttributeFor(USERS_COLLECTION_ID, 'service_category', 24, false));

  await ensureNotificationsCollection();
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

async function generateAiPassengers(limit) {
  const max = Math.max(1, Math.min(50, Number(limit) || 30));
  const cached = globalThis.__tripAiPassengersCache;
  if (cached && cached.items && cached.expiresAt && Date.now() < cached.expiresAt && cached.items.length >= max) {
    return cached.items.slice(0, max);
  }

  const fallback = (() => {
    const names = [
      { fullName: 'Adebayo K.', nationality: 'Nigerian', gender: 'Male' },
      { fullName: 'Isabella Chen', nationality: 'Chinese', gender: 'Female' },
      { fullName: 'Marie Dubois', nationality: 'French', gender: 'Female' },
      { fullName: 'Hassan Al‑Rashid', nationality: 'Emirati', gender: 'Male' },
      { fullName: 'Rina Yamamoto', nationality: 'Japanese', gender: 'Female' },
      { fullName: 'Owen Parker', nationality: 'British', gender: 'Male' },
      { fullName: 'Ama Mensah', nationality: 'Ghanaian', gender: 'Female' },
      { fullName: 'Sipho Dlamini', nationality: 'South African', gender: 'Male' },
      { fullName: 'Fatima Abdullahi', nationality: 'Nigerian', gender: 'Female' },
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
        await ensureNotificationsCollection();
        return send(res, 200, { ok: true });
      } catch (e) {
        return send(res, 500, { error: e?.message || 'Schema ensure failed' });
      }
    }

    if (action === 'schema' && parts[2] === 'inspect') {
      if (req.method !== 'GET') return send(res, 405, { error: 'Method not allowed' });
      try {
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

    if (action === 'submissions') {
      if (req.method === 'GET') {
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
        await upsertUser(username, userData);
        return send(res, 200, { ok: true });
      }

      return send(res, 405, { error: 'Method not allowed' });
    }

    if (action === 'notifications') {
      if (req.method === 'GET') {
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
      }

      if (req.method === 'POST') {
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
      }

      if (req.method === 'PATCH') {
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
      }

      if (req.method === 'DELETE') {
        const id = parts[2] ? String(parts[2]).trim() : '';
        if (!id) return send(res, 400, { error: 'Missing id' });
        await appwriteRequest(`/databases/${encodeURIComponent(DATABASE_ID)}/collections/${encodeURIComponent(NOTIFICATIONS_COLLECTION_ID)}/documents/${encodeURIComponent(id)}`, {
          method: 'DELETE',
        });
        return send(res, 200, { ok: true });
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
        return send(res, 200, { ok: true, id });
      }

      return send(res, 405, { error: 'Method not allowed' });
    }

    return send(res, 404, { error: 'Not found' });
  }

  if (scope === 'public') {
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
};
