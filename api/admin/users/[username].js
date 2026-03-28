const { parseCookies } = require('../../_lib/cookies');
const { verifyToken } = require('../../_lib/token');
const { findUserDocByUsername, parseUserData, upsertUser, deleteUser } = require('../../_lib/appwrite');

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

function requireAdmin(req, res) {
  const cookies = parseCookies(req);
  const payload = verifyToken(cookies.trip_admin);
  if (!payload || payload.typ !== 'admin') {
    res.statusCode = 401;
    res.setHeader('content-type', 'application/json');
    res.end(JSON.stringify({ error: 'Unauthorized' }));
    return false;
  }
  return true;
}

module.exports = async (req, res) => {
  if (!requireAdmin(req, res)) return;

  const username = String(req.query.username || '').trim();
  if (!username) {
    res.statusCode = 400;
    res.setHeader('content-type', 'application/json');
    res.end(JSON.stringify({ error: 'Missing username' }));
    return;
  }

  if (req.method === 'GET') {
    const doc = await findUserDocByUsername(username);
    if (!doc) {
      res.statusCode = 404;
      res.setHeader('content-type', 'application/json');
      res.end(JSON.stringify({ error: 'Not found' }));
      return;
    }
    res.statusCode = 200;
    res.setHeader('content-type', 'application/json');
    res.end(JSON.stringify({ username: doc.username, userData: parseUserData(doc) }));
    return;
  }

  if (req.method === 'PUT' || req.method === 'PATCH') {
    const body = await readJson(req).catch(() => ({}));
    const userData = body.userData;
    if (!userData) {
      res.statusCode = 400;
      res.setHeader('content-type', 'application/json');
      res.end(JSON.stringify({ error: 'Missing userData' }));
      return;
    }
    await upsertUser(username, userData);
    res.statusCode = 200;
    res.setHeader('content-type', 'application/json');
    res.end(JSON.stringify({ ok: true }));
    return;
  }

  if (req.method === 'DELETE') {
    const result = await deleteUser(username);
    const ok = typeof result === 'object' ? Boolean(result.ok) : Boolean(result);
    res.statusCode = 200;
    res.setHeader('content-type', 'application/json');
    res.end(JSON.stringify({ ok, result: typeof result === 'object' ? result : null }));
    return;
  }

  res.statusCode = 405;
  res.setHeader('content-type', 'application/json');
  res.end(JSON.stringify({ error: 'Method not allowed' }));
};
