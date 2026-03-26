const { cookieString, isHttps } = require('../_lib/cookies');

module.exports = async (req, res) => {
  res.statusCode = 200;
  res.setHeader('set-cookie', cookieString('trip_admin', '', { maxAgeSeconds: 0, secure: isHttps(req) }));
  res.setHeader('content-type', 'application/json');
  res.end(JSON.stringify({ ok: true }));
};

