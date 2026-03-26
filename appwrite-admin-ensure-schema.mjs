const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

import fs from 'node:fs';
import path from 'node:path';

function loadDotEnvFileIfPresent(filename) {
  const envPath = path.resolve(process.cwd(), filename);
  if (!fs.existsSync(envPath)) return false;
  const raw = fs.readFileSync(envPath, 'utf8');
  raw.split(/\r?\n/).forEach((line) => {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith('#')) return;
    const eq = trimmed.indexOf('=');
    if (eq === -1) return;
    const key = trimmed.slice(0, eq).trim();
    let val = trimmed.slice(eq + 1).trim();
    if ((val.startsWith('"') && val.endsWith('"')) || (val.startsWith("'") && val.endsWith("'"))) {
      val = val.slice(1, -1);
    }
    if (!process.env[key]) process.env[key] = val;
  });
  console.log(`Loaded env from ${filename}`);
  return true;
}

(() => {
  console.log(`CWD: ${process.cwd()}`);
  const loaded =
    loadDotEnvFileIfPresent('.env') ||
    loadDotEnvFileIfPresent('.env.local') ||
    loadDotEnvFileIfPresent('.env.example');
  if (!loaded) {
    console.log('No .env / .env.local / .env.example found. Using process environment only.');
  }
})();

function requiredEnv(name) {
  const v = process.env[name];
  if (!v) {
    const present = Object.keys(process.env)
      .filter((k) => k.startsWith('APPWRITE_'))
      .sort()
      .join(', ');
    throw new Error(`Missing env: ${name}. Present APPWRITE_* keys: ${present || '(none)'}`);
  }
  return v;
}

function normalizeEndpoint(endpoint) {
  const e = endpoint.replace(/\/+$/, '');
  if (!e.endsWith('/v1')) return `${e}/v1`;
  return e;
}

const ENDPOINT = normalizeEndpoint(requiredEnv('APPWRITE_ENDPOINT'));
const PROJECT_ID = requiredEnv('APPWRITE_PROJECT_ID');
const API_KEY = requiredEnv('APPWRITE_API_KEY');
const DATABASE_ID = requiredEnv('APPWRITE_DATABASE_ID');
const USERS_COLLECTION_ID = requiredEnv('APPWRITE_COLLECTION_USERS_ID');

async function request(path, { method = 'GET', body } = {}) {
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
    const err = new Error(`HTTP ${res.status} ${res.statusText} for ${method} ${url}`);
    err.status = res.status;
    err.payload = json;
    throw err;
  }
  return json;
}

async function exists(path) {
  try {
    await request(path);
    return true;
  } catch (e) {
    if (e.status === 404) return false;
    throw e;
  }
}

async function ensureDatabase() {
  const dbPath = `/databases/${encodeURIComponent(DATABASE_ID)}`;
  const ok = await exists(dbPath);
  if (ok) return;

  await request('/databases', {
    method: 'POST',
    body: {
      databaseId: DATABASE_ID,
      name: 'TRIP Portal',
    },
  });
}

async function ensureUsersCollection() {
  const colPath = `/databases/${encodeURIComponent(DATABASE_ID)}/collections/${encodeURIComponent(USERS_COLLECTION_ID)}`;
  const ok = await exists(colPath);
  if (ok) return;

  await request(`/databases/${encodeURIComponent(DATABASE_ID)}/collections`, {
    method: 'POST',
    body: {
      collectionId: USERS_COLLECTION_ID,
      name: 'Users',
      documentSecurity: false,
      permissions: [
        'read("any")',
        'create("any")',
        'update("any")',
        'delete("any")',
      ],
    },
  });
}

async function listAttributes() {
  const out = await request(`/databases/${encodeURIComponent(DATABASE_ID)}/collections/${encodeURIComponent(USERS_COLLECTION_ID)}/attributes`);
  const attrs = Array.isArray(out?.attributes) ? out.attributes : [];
  return attrs;
}

async function getAttribute(key) {
  return request(`/databases/${encodeURIComponent(DATABASE_ID)}/collections/${encodeURIComponent(USERS_COLLECTION_ID)}/attributes/${encodeURIComponent(key)}`);
}

async function waitForAttribute(key) {
  for (let i = 0; i < 30; i++) {
    const a = await getAttribute(key);
    const status = a?.status;
    if (status === 'available') return;
    if (status === 'failed') throw new Error(`Attribute ${key} failed: ${JSON.stringify(a)}`);
    await sleep(2000);
  }
  throw new Error(`Timed out waiting for attribute ${key} to become available.`);
}

async function createStringAttribute({ key, size, required }) {
  await request(`/databases/${encodeURIComponent(DATABASE_ID)}/collections/${encodeURIComponent(USERS_COLLECTION_ID)}/attributes/string`, {
    method: 'POST',
    body: {
      key,
      size,
      required,
      default: null,
      array: false,
    },
  });
  await waitForAttribute(key);
}

async function ensureAttributes() {
  const attrs = await listAttributes();
  const keys = new Set(attrs.map((a) => a.key));

  if (!keys.has('username')) {
    await createStringAttribute({ key: 'username', size: 255, required: true });
  }

  if (!keys.has('data')) {
    try {
      await createStringAttribute({ key: 'data', size: 1000000, required: true });
    } catch (e) {
      await createStringAttribute({ key: 'data', size: 200000, required: true });
    }
  }
}

async function listIndexes() {
  const out = await request(`/databases/${encodeURIComponent(DATABASE_ID)}/collections/${encodeURIComponent(USERS_COLLECTION_ID)}/indexes`);
  const idx = Array.isArray(out?.indexes) ? out.indexes : [];
  return idx;
}

async function getIndex(key) {
  return request(`/databases/${encodeURIComponent(DATABASE_ID)}/collections/${encodeURIComponent(USERS_COLLECTION_ID)}/indexes/${encodeURIComponent(key)}`);
}

async function waitForIndex(key) {
  for (let i = 0; i < 30; i++) {
    const idx = await getIndex(key);
    const status = idx?.status;
    if (status === 'available') return;
    if (status === 'failed') throw new Error(`Index ${key} failed: ${JSON.stringify(idx)}`);
    await sleep(2000);
  }
  throw new Error(`Timed out waiting for index ${key} to become available.`);
}

async function ensureIndexes() {
  const indexes = await listIndexes();
  const keys = new Set(indexes.map((i) => i.key));
  if (keys.has('username_idx')) return;

  try {
    await request(`/databases/${encodeURIComponent(DATABASE_ID)}/collections/${encodeURIComponent(USERS_COLLECTION_ID)}/indexes`, {
      method: 'POST',
      body: {
        key: 'username_idx',
        type: 'key',
        attributes: ['username'],
        orders: ['ASC'],
      },
    });
    await waitForIndex('username_idx');
  } catch {
    // Index creation isn't strictly required for this app; ignore if Appwrite rejects parameters.
  }
}

async function main() {
  console.log('TRIP Admin • Ensuring Appwrite schema...');
  console.log(`Endpoint: ${ENDPOINT}`);
  console.log(`Project:  ${PROJECT_ID}`);
  console.log(`DB:       ${DATABASE_ID}`);
  console.log(`Users:    ${USERS_COLLECTION_ID}`);

  await ensureDatabase();
  await ensureUsersCollection();
  await ensureAttributes();
  await ensureIndexes();

  console.log('Schema ready.');
}

main().catch((e) => {
  console.error('Schema ensure failed.');
  console.error(e?.message || e);
  if (e?.payload) console.error(JSON.stringify(e.payload, null, 2));
  process.exit(1);
});
