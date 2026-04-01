const { cookieString, isHttps } = require('../_lib/cookies');

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
  res.statusCode = 200;
  const host = String(req.headers?.host || '').trim();
  let sameSite = 'Lax';
  try {
    if (allow && host) {
      const o = new URL(allow);
      if (String(o.host).toLowerCase() !== String(host).toLowerCase()) sameSite = 'None';
    }
  } catch {}
  res.setHeader('set-cookie', cookieString('trip_session', '', { maxAgeSeconds: 0, secure: isHttps(req), sameSite }));
  res.setHeader('content-type', 'application/json');
  res.end(JSON.stringify({ ok: true }));
};
