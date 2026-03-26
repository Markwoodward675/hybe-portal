const { parseCookies } = require('../_lib/cookies');
const { verifyToken } = require('../_lib/token');
const { findUserDocByUsername, parseUserData } = require('../_lib/appwrite');

module.exports = async (req, res) => {
  const cookies = parseCookies(req);
  const payload = verifyToken(cookies.trip_session);
  if (!payload || payload.typ !== 'user' || !payload.u) {
    res.statusCode = 401;
    res.setHeader('content-type', 'application/json');
    res.end(JSON.stringify({ error: 'Unauthorized' }));
    return;
  }

  const username = String(payload.u);
  const doc = await findUserDocByUsername(username);
  const userData = doc ? parseUserData(doc) : null;
  if (!userData) {
    res.statusCode = 401;
    res.setHeader('content-type', 'application/json');
    res.end(JSON.stringify({ error: 'Unauthorized' }));
    return;
  }

  const safe = { ...userData };
  delete safe.pin;

  res.statusCode = 200;
  res.setHeader('content-type', 'application/json');
  res.end(JSON.stringify({ username: doc.username, userData: safe }));
};

