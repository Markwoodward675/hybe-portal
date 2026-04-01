const { parseCookies, cookieString, isHttps } = require('../../server_lib/cookies');
const { createToken, verifyToken } = require('../../server_lib/token');
const { findUserDocByUsername, parseUserData } = require('../../server_lib/appwrite');

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

module.exports = async (req, res) => {
  applyCors(req, res);
  if (req.method === 'OPTIONS') {
    res.statusCode = 204;
    res.end('');
    return;
  }
  const parts = Array.isArray(req.query.path) ? req.query.path : (req.query.path ? [req.query.path] : []);

  if (parts[0] === 'login') {
    if (req.method !== 'POST') return send(res, 405, { error: 'Method not allowed' });
    const body = await readJson(req).catch(() => ({}));
    const username = String(body.username || '').trim();
    const pin = String(body.pin || '').trim();
    if (!username || !pin) return send(res, 400, { error: 'Missing credentials' });

    const doc = await findUserDocByUsername(username);
    const userData = doc ? parseUserData(doc) : null;
    if (!userData || String(userData.pin) !== String(pin)) return send(res, 401, { ok: false });

    const token = createToken({ typ: 'user', u: doc.username, exp: Date.now() + 6 * 60 * 60 * 1000 });
    const secure = isHttps(req);
    const sameSite = cookieSameSite(req);
    res.setHeader('set-cookie', cookieString('trip_session', token, { maxAgeSeconds: 6 * 60 * 60, secure, sameSite }));
    return send(res, 200, { ok: true, username: doc.username });
  }

  if (parts[0] === 'logout') {
    const secure = isHttps(req);
    const sameSite = cookieSameSite(req);
    res.setHeader('set-cookie', cookieString('trip_session', '', { maxAgeSeconds: 0, secure, sameSite }));
    return send(res, 200, { ok: true });
  }

  if (parts[0] === 'me') {
    const cookies = parseCookies(req);
    const payload = verifyToken(cookies.trip_session);
    if (!payload || payload.typ !== 'user' || !payload.u) return send(res, 401, { error: 'Unauthorized' });

    const username = String(payload.u);
    const doc = await findUserDocByUsername(username);
    const userData = doc ? parseUserData(doc) : null;
    if (!userData) return send(res, 401, { error: 'Unauthorized' });

    const safe = { ...userData };
    delete safe.pin;
    return send(res, 200, { username: doc.username, userData: safe });
  }

  return send(res, 404, { error: 'Not found' });
};
