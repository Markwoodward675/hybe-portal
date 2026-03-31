const { parseCookies, cookieString, isHttps } = require('../../server_lib/cookies');
const { createToken, verifyToken } = require('../../server_lib/token');
const { listAllUserDocs, parseUserData, upsertUser, deleteUser, findUserDocByUsername, appwriteRequest, DATABASE_ID, USERS_COLLECTION_ID, NOTIFICATIONS_COLLECTION_ID } = require('../../server_lib/appwrite');

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const SUBMISSIONS_COLLECTION_ID = process.env.APPWRITE_COLLECTION_SUBMISSIONS_ID || 'submissions';
const REGISTRATIONS_COLLECTION_ID = process.env.APPWRITE_COLLECTION_REGISTRATIONS_ID || 'registrations';
const LIVE_POPUPS_COLLECTION_ID = process.env.APPWRITE_COLLECTION_LIVE_POPUPS_ID || 'live_popups';
const KYC_BUCKET_ID = process.env.APPWRITE_BUCKET_KYC_ID || 'kyc_uploads';

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

async function createLargeStringAttributeFor(collectionId, key, required) {
  try {
    await createStringAttributeFor(collectionId, key, 1000000, required);
  } catch {
    await createStringAttributeFor(collectionId, key, 200000, required);
  }
}

async function createBooleanAttributeFor(collectionId, key, defaultValue) {
  await appwriteRequest(`/databases/${encodeURIComponent(DATABASE_ID)}/collections/${encodeURIComponent(collectionId)}/attributes/boolean`, {
    method: 'POST',
    body: { key, required: false, default: Boolean(defaultValue), array: false },
  });
  await waitAvailableAttributeFor(collectionId, key);
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
  if (!keys.has('service_category')) await createStringAttribute('service_category', 24, false);
}

async function listIndexesFor(collectionId) {
  const out = await appwriteRequest(`/databases/${encodeURIComponent(DATABASE_ID)}/collections/${encodeURIComponent(collectionId)}/indexes`);
  return Array.isArray(out?.indexes) ? out.indexes : [];
}

async function getIndexFor(collectionId, key) {
  return appwriteRequest(`/databases/${encodeURIComponent(DATABASE_ID)}/collections/${encodeURIComponent(collectionId)}/indexes/${encodeURIComponent(key)}`);
}

async function waitIndexAvailable(collectionId, key) {
  for (let i = 0; i < 30; i++) {
    const idx = await getIndexFor(collectionId, key);
    const status = idx?.status;
    if (status === 'available') return;
    if (status === 'failed') throw new Error(`Index ${key} failed`);
    await sleep(1500);
  }
  throw new Error(`Timed out waiting for index ${key}`);
}

async function ensureIndexFor(collectionId, key, type, attributes, orders) {
  const list = await listIndexesFor(collectionId).catch(() => []);
  const existsIdx = (list || []).some((i) => i && i.key === key && i.status === 'available');
  if (existsIdx) return { key, status: 'exists' };
  try {
    await appwriteRequest(`/databases/${encodeURIComponent(DATABASE_ID)}/collections/${encodeURIComponent(collectionId)}/indexes`, {
      method: 'POST',
      body: {
        key,
        type,
        attributes: Array.isArray(attributes) ? attributes : [],
        orders: Array.isArray(orders) ? orders : [],
      },
    });
    await waitIndexAvailable(collectionId, key);
    return { key, status: 'created' };
  } catch (e) {
    if (e && e.status === 409) return { key, status: 'exists' };
    return { key, status: 'failed', error: e?.message || 'index create failed' };
  }
}

async function ensureGenericCollection({ collectionId, name, permissions }) {
  const colPath = `/databases/${encodeURIComponent(DATABASE_ID)}/collections/${encodeURIComponent(collectionId)}`;
  const ok = await exists(colPath);
  if (ok) return { key: collectionId, status: 'exists' };
  await appwriteRequest(`/databases/${encodeURIComponent(DATABASE_ID)}/collections`, {
    method: 'POST',
    body: { collectionId, name, documentSecurity: false, permissions: Array.isArray(permissions) ? permissions : [] },
  });
  return { key: collectionId, status: 'created' };
}

function storageConfigured() {
  return Boolean(process.env.APPWRITE_ENDPOINT && process.env.APPWRITE_PROJECT_ID && process.env.APPWRITE_API_KEY);
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

async function ensureSubmissionsCollection() {
  await ensureGenericCollection({ collectionId: SUBMISSIONS_COLLECTION_ID, name: 'Submissions', permissions: [] });
  const attrs = await listAttributesFor(SUBMISSIONS_COLLECTION_ID).catch(() => []);
  const keys = new Set((attrs || []).map((a) => a.key));
  if (!keys.has('username')) await createStringAttributeFor(SUBMISSIONS_COLLECTION_ID, 'username', 255, true);
  if (!keys.has('type')) await createStringAttributeFor(SUBMISSIONS_COLLECTION_ID, 'type', 24, true);
  if (!keys.has('title')) await createStringAttributeFor(SUBMISSIONS_COLLECTION_ID, 'title', 180, false);
  if (!keys.has('status')) await createStringAttributeFor(SUBMISSIONS_COLLECTION_ID, 'status', 24, false);
  if (!keys.has('submittedAt')) await createStringAttributeFor(SUBMISSIONS_COLLECTION_ID, 'submittedAt', 64, false);
  if (!keys.has('reviewedAt')) await createStringAttributeFor(SUBMISSIONS_COLLECTION_ID, 'reviewedAt', 64, false);
  if (!keys.has('data')) await createLargeStringAttributeFor(SUBMISSIONS_COLLECTION_ID, 'data', false);
  if (!keys.has('signatureDataUrl')) await createLargeStringAttributeFor(SUBMISSIONS_COLLECTION_ID, 'signatureDataUrl', false);
  if (!keys.has('signatureName')) await createStringAttributeFor(SUBMISSIONS_COLLECTION_ID, 'signatureName', 180, false);
  if (!keys.has('signatureSignedAt')) await createStringAttributeFor(SUBMISSIONS_COLLECTION_ID, 'signatureSignedAt', 64, false);
}

async function ensureRegistrationsCollection() {
  await ensureGenericCollection({ collectionId: REGISTRATIONS_COLLECTION_ID, name: 'Registrations', permissions: ['read("any")'] });
  const attrs = await listAttributesFor(REGISTRATIONS_COLLECTION_ID).catch(() => []);
  const keys = new Set((attrs || []).map((a) => a.key));
  if (!keys.has('fullName')) await createStringAttributeFor(REGISTRATIONS_COLLECTION_ID, 'fullName', 180, true);
  if (!keys.has('email')) await createStringAttributeFor(REGISTRATIONS_COLLECTION_ID, 'email', 255, true);
  if (!keys.has('phone')) await createStringAttributeFor(REGISTRATIONS_COLLECTION_ID, 'phone', 80, true);
  if (!keys.has('optionalId')) await createStringAttributeFor(REGISTRATIONS_COLLECTION_ID, 'optionalId', 120, false);
  if (!keys.has('codeHash')) await createStringAttributeFor(REGISTRATIONS_COLLECTION_ID, 'codeHash', 80, true);
  if (!keys.has('codeSalt')) await createStringAttributeFor(REGISTRATIONS_COLLECTION_ID, 'codeSalt', 64, true);
  if (!keys.has('createdAt')) await createStringAttributeFor(REGISTRATIONS_COLLECTION_ID, 'createdAt', 64, false);
  if (!keys.has('verifiedAt')) await createStringAttributeFor(REGISTRATIONS_COLLECTION_ID, 'verifiedAt', 64, false);
}

async function ensureNotificationsCollection() {
  await ensureGenericCollection({ collectionId: NOTIFICATIONS_COLLECTION_ID, name: 'Notifications', permissions: ['read("any")'] });
  const attrs = await listAttributesFor(NOTIFICATIONS_COLLECTION_ID).catch(() => []);
  const keys = new Set((attrs || []).map((a) => a.key));
  if (!keys.has('title')) await createStringAttributeFor(NOTIFICATIONS_COLLECTION_ID, 'title', 120, true);
  if (!keys.has('message')) await createStringAttributeFor(NOTIFICATIONS_COLLECTION_ID, 'message', 2000, true);
  if (!keys.has('tone')) await createStringAttributeFor(NOTIFICATIONS_COLLECTION_ID, 'tone', 24, false);
  if (!keys.has('active')) await createBooleanAttributeFor(NOTIFICATIONS_COLLECTION_ID, 'active', false);
  if (!keys.has('createdAt')) await createStringAttributeFor(NOTIFICATIONS_COLLECTION_ID, 'createdAt', 64, false);
}

async function ensureLivePopupsCollection() {
  await ensureGenericCollection({ collectionId: LIVE_POPUPS_COLLECTION_ID, name: 'Live Popups', permissions: ['read("any")'] });
  const attrs = await listAttributesFor(LIVE_POPUPS_COLLECTION_ID).catch(() => []);
  const keys = new Set((attrs || []).map((a) => a.key));
  if (!keys.has('data')) await createLargeStringAttributeFor(LIVE_POPUPS_COLLECTION_ID, 'data', true);
  if (!keys.has('updatedAt')) await createStringAttributeFor(LIVE_POPUPS_COLLECTION_ID, 'updatedAt', 64, false);
}

async function schemaEnsureAll() {
  const actions = [];
  await ensureDatabase();
  await ensureCollection();
  await lockCollectionPermissions();
  await ensureAttributes();

  actions.push(await ensureIndexFor(USERS_COLLECTION_ID, 'idx_username_lc_unique', 'unique', ['username_lc'], []));
  actions.push(await ensureIndexFor(USERS_COLLECTION_ID, 'idx_username_key', 'key', ['username'], ['asc']));

  await ensureRegistrationsCollection();
  await ensureSubmissionsCollection();
  await ensureNotificationsCollection();
  await ensureLivePopupsCollection();

  actions.push(await ensureIndexFor(SUBMISSIONS_COLLECTION_ID, 'idx_sub_username_key', 'key', ['username'], ['asc']));
  actions.push(await ensureIndexFor(SUBMISSIONS_COLLECTION_ID, 'idx_sub_type_key', 'key', ['type'], ['asc']));

  try { actions.push(await ensureKycBucket()); } catch (e) { actions.push({ key: 'kyc_bucket', status: 'failed', error: e?.message || 'failed' }); }
  return actions;
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
      const usersAttrs = colOk ? await listAttributes() : [];
      const hasUsername = usersAttrs.some((a) => a.key === 'username' && a.status === 'available');
      const hasUsernameLc = usersAttrs.some((a) => a.key === 'username_lc' && a.status === 'available');
      const hasData = usersAttrs.some((a) => a.key === 'data' && a.status === 'available');
      const hasSvc = usersAttrs.some((a) => a.key === 'service_category' && a.status === 'available');
      const subOk = await exists(`/databases/${encodeURIComponent(DATABASE_ID)}/collections/${encodeURIComponent(SUBMISSIONS_COLLECTION_ID)}`).catch(() => false);
      const regOk = await exists(`/databases/${encodeURIComponent(DATABASE_ID)}/collections/${encodeURIComponent(REGISTRATIONS_COLLECTION_ID)}`).catch(() => false);
      const notOk = await exists(`/databases/${encodeURIComponent(DATABASE_ID)}/collections/${encodeURIComponent(NOTIFICATIONS_COLLECTION_ID)}`).catch(() => false);
      const lpOk = await exists(`/databases/${encodeURIComponent(DATABASE_ID)}/collections/${encodeURIComponent(LIVE_POPUPS_COLLECTION_ID)}`).catch(() => false);
      const status = `DB:${dbOk ? 'OK' : 'MISSING'} • USERS:${colOk ? 'OK' : 'MISSING'} • username:${hasUsername ? 'OK' : 'MISSING'} • username_lc:${hasUsernameLc ? 'OK' : 'MISSING'} • data:${hasData ? 'OK' : 'MISSING'} • service_category:${hasSvc ? 'OK' : 'MISSING'} • submissions:${subOk ? 'OK' : 'MISSING'} • registrations:${regOk ? 'OK' : 'MISSING'} • notifications:${notOk ? 'OK' : 'MISSING'} • live_popups:${lpOk ? 'OK' : 'MISSING'}`;
      return send(res, 200, { status });
    }
    if (req.method !== 'POST') return send(res, 405, { error: 'Method not allowed' });
    try {
      const actions = await schemaEnsureAll();
      return send(res, 200, { ok: true, actions });
    } catch (e) {
      return send(res, 500, { error: e?.message || 'Schema ensure failed' });
    }
  }

  if (parts[0] === 'schema' && parts[1] === 'sync') {
    if (req.method !== 'POST') return send(res, 405, { error: 'Method not allowed' });
    try {
      const actions = await schemaEnsureAll();
      return send(res, 200, { ok: true, actions });
    } catch (e) {
      return send(res, 500, { error: e?.message || 'Schema sync failed' });
    }
  }

  if (parts[0] === 'schema' && parts[1] === 'inspect') {
    if (req.method !== 'GET') return send(res, 405, { error: 'Method not allowed' });
    try {
      const db = await appwriteRequest(`/databases/${encodeURIComponent(DATABASE_ID)}`);
      const collectionsOut = await appwriteRequest(`/databases/${encodeURIComponent(DATABASE_ID)}/collections`);
      const collections = Array.isArray(collectionsOut?.collections) ? collectionsOut.collections : [];
      return send(res, 200, { databaseId: db?.$id || DATABASE_ID, collections });
    } catch (e) {
      return send(res, 500, { error: e?.message || 'Schema inspect failed' });
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
