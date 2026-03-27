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
  const handler = require('../[...path].js');
  const parts = Array.isArray(req.query.path) ? req.query.path : (req.query.path ? [req.query.path] : []);
  req.query = req.query || {};
  req.query.path = ['public', ...parts];
  return handler(req, res);
};
