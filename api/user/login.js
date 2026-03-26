const { cookieString, isHttps } = require('../_lib/cookies');
const { createToken } = require('../_lib/token');
const { findUserDocByUsername, parseUserData } = require('../_lib/appwrite');

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
  const username = String(body.username || '').trim();
  const pin = String(body.pin || '').trim();
  if (!username || !pin) {
    res.statusCode = 400;
    res.setHeader('content-type', 'application/json');
    res.end(JSON.stringify({ error: 'Missing credentials' }));
    return;
  }

  const doc = await findUserDocByUsername(username);
  const userData = doc ? parseUserData(doc) : null;
  if (!userData || String(userData.pin) !== String(pin)) {
    res.statusCode = 401;
    res.setHeader('content-type', 'application/json');
    res.end(JSON.stringify({ ok: false }));
    return;
  }

  const token = createToken({
    typ: 'user',
    u: doc.username,
    exp: Date.now() + 6 * 60 * 60 * 1000,
  });

  res.statusCode = 200;
  res.setHeader('set-cookie', cookieString('trip_session', token, { maxAgeSeconds: 6 * 60 * 60, secure: isHttps(req) }));
  res.setHeader('content-type', 'application/json');
  res.end(JSON.stringify({ ok: true, username: doc.username }));
};

