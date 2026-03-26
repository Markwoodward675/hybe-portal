const { parseCookies } = require('../_lib/cookies');
const { verifyToken } = require('../_lib/token');

module.exports = async (req, res) => {
  const cookies = parseCookies(req);
  const token = cookies.trip_admin;
  const payload = verifyToken(token);
  const ok = payload && payload.typ === 'admin';

  res.statusCode = ok ? 200 : 401;
  res.setHeader('content-type', 'application/json');
  res.end(JSON.stringify({ ok: Boolean(ok) }));
};

