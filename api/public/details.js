const { findUserDocByUsername, parseUserData, upsertUser } = require('../_lib/appwrite');

module.exports = async (req, res) => {
  if (req.method !== 'GET') {
    res.statusCode = 405;
    res.setHeader('content-type', 'application/json');
    res.end(JSON.stringify({ error: 'Method not allowed' }));
    return;
  }

  const username = String(req.query.u || '').trim();
  const tc = String(req.query.tc || '').trim();
  if (!username || !tc) {
    res.statusCode = 400;
    res.setHeader('content-type', 'application/json');
    res.end(JSON.stringify({ error: 'Missing params' }));
    return;
  }

  const doc = await findUserDocByUsername(username);
  const userData = doc ? parseUserData(doc) : null;
  if (!userData) {
    res.statusCode = 404;
    res.setHeader('content-type', 'application/json');
    res.end(JSON.stringify({ error: 'Not found' }));
    return;
  }

  const share = userData.share || {};
  const oneTime = share.oneTimeTracking || {};
  const codeOk = oneTime.code && String(oneTime.code) === tc && !oneTime.usedAt;
  if (!codeOk) {
    res.statusCode = 401;
    res.setHeader('content-type', 'application/json');
    res.end(JSON.stringify({ error: 'Invalid or used code' }));
    return;
  }

  userData.share = userData.share || {};
  userData.share.oneTimeTracking = userData.share.oneTimeTracking || {};
  userData.share.oneTimeTracking.usedAt = new Date().toISOString();
  await upsertUser(doc.username, userData);

  const safe = { ...userData };
  delete safe.pin;

  res.statusCode = 200;
  res.setHeader('content-type', 'application/json');
  res.end(JSON.stringify({ username: doc.username, userData: safe }));
};

