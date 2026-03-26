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
  res.setHeader('set-cookie', cookieString('trip_admin', token, { maxAgeSeconds: 8 * 60 * 60, secure: isHttps(req) }));
  res.setHeader('content-type', 'application/json');
  res.end(JSON.stringify({ ok: true }));
};

