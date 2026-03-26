const { parseCookies } = require('../_lib/cookies');
const { verifyToken } = require('../_lib/token');
const { listAllUserDocs, parseUserData, upsertUser } = require('../_lib/appwrite');

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

  if (req.method === 'GET') {
    const docs = await listAllUserDocs(1000);
    const users = {};
    docs.forEach((doc) => {
      const u = parseUserData(doc);
      if (u && doc.username) users[doc.username] = u;
    });
    res.statusCode = 200;
    res.setHeader('content-type', 'application/json');
    res.end(JSON.stringify({ users }));
    return;
  }

  if (req.method === 'POST') {
    const body = await readJson(req).catch(() => ({}));
    const username = String(body.username || '').trim();
    const userData = body.userData;
    if (!username || !userData) {
      res.statusCode = 400;
      res.setHeader('content-type', 'application/json');
      res.end(JSON.stringify({ error: 'Missing username or userData' }));
      return;
    }
    await upsertUser(username, userData);
    res.statusCode = 200;
    res.setHeader('content-type', 'application/json');
    res.end(JSON.stringify({ ok: true }));
    return;
  }

  res.statusCode = 405;
  res.setHeader('content-type', 'application/json');
  res.end(JSON.stringify({ error: 'Method not allowed' }));
};

