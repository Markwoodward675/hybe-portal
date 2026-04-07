const crypto = require('crypto');
const fs = require('fs');
const path = require('path');

function localUsersPath() {
  return path.resolve(process.cwd(), 'data', 'users.json');
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

module.exports = {
  localUsersPath,
  readLocalUsersSync,
  writeLocalUsersSync,
  listLocalUsersCached,
  findLocalUser,
  upsertLocalUser,
  deleteLocalUser,
};