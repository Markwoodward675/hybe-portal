const crypto = require('crypto');
const { parseCookies } = require('../../server_lib/cookies');
const { verifyToken } = require('../../server_lib/token');
const { findUserDocByUsername, parseUserData, upsertUser, listAllUserDocs } = require('../../server_lib/appwrite');

function send(res, status, payload) {
  res.statusCode = status;
  res.setHeader('content-type', 'application/json');
  res.end(JSON.stringify(payload));
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

module.exports = async (req, res) => {
  const parts = Array.isArray(req.query.path) ? req.query.path : (req.query.path ? [req.query.path] : []);

  if (parts[0] === 'details') {
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

  if (parts[0] === 'bookings') {
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
};

