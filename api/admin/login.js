const { cookieString, isHttps } = require('../_lib/cookies');
const { createToken } = require('../_lib/token');

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

module.exports = async (req, res) => {
  const origin = String(req.headers?.origin || '').trim();
  const allow = origin && (/^http:\/\/localhost:\d+$/i.test(origin) || /^http:\/\/127\.0\.0\.1:\d+$/i.test(origin) || /^https:\/\/hybe-portal([a-z0-9-]*)\.vercel\.app$/i.test(origin) || /^https:\/\/hybe-portal\.vercel\.app$/i.test(origin)) ? origin : '';
  if (allow) {
    res.setHeader('access-control-allow-origin', allow);
    res.setHeader('vary', 'Origin');
    res.setHeader('access-control-allow-credentials', 'true');
    res.setHeader('access-control-allow-methods', 'GET,POST,PUT,PATCH,DELETE,OPTIONS');
    res.setHeader('access-control-allow-headers', 'content-type, authorization');
  }
  if (req.method === 'OPTIONS') {
    res.statusCode = 204;
    res.end('');
    return;
  }
  if (req.method !== 'POST') {
    res.statusCode = 405;
    res.setHeader('content-type', 'application/json');
    res.end(JSON.stringify({ error: 'Method not allowed' }));
    return;
  }

  const body = await readJson(req).catch(() => ({}));
  const passcode = String(body.passcode || '');
  const expected = process.env.TRIP_ADMIN_PASSCODE || 'HYBE2026';

  if (passcode !== expected) {
    res.statusCode = 401;
    res.setHeader('content-type', 'application/json');
    res.end(JSON.stringify({ ok: false }));
    return;
  }

  const token = createToken({
    typ: 'admin',
    exp: Date.now() + 8 * 60 * 60 * 1000,
  });

  res.statusCode = 200;
  const host = String(req.headers?.host || '').trim();
  let sameSite = 'Lax';
  try {
    if (allow && host) {
      const o = new URL(allow);
      if (String(o.host).toLowerCase() !== String(host).toLowerCase()) sameSite = 'None';
    }
  } catch {}
  res.setHeader('set-cookie', cookieString('trip_admin', token, { maxAgeSeconds: 8 * 60 * 60, secure: isHttps(req), sameSite }));
  res.setHeader('content-type', 'application/json');
  res.end(JSON.stringify({ ok: true }));
};
