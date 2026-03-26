function parseCookies(req) {
  const header = req.headers?.cookie || '';
  const out = {};
  header.split(';').forEach((part) => {
    const s = part.trim();
    if (!s) return;
    const eq = s.indexOf('=');
    if (eq === -1) return;
    const k = s.slice(0, eq).trim();
    const v = s.slice(eq + 1).trim();
    out[k] = decodeURIComponent(v);
  });
  return out;
}

function isHttps(req) {
  const proto = (req.headers && (req.headers['x-forwarded-proto'] || req.headers['x-forwarded-protocol'])) || '';
  return String(proto).toLowerCase().includes('https');
}

function cookieString(name, value, { maxAgeSeconds, httpOnly = true, sameSite = 'Lax', path = '/', secure } = {}) {
  const parts = [];
  parts.push(`${name}=${encodeURIComponent(value)}`);
  parts.push(`Path=${path}`);
  if (typeof maxAgeSeconds === 'number') parts.push(`Max-Age=${Math.floor(maxAgeSeconds)}`);
  if (httpOnly) parts.push('HttpOnly');
  if (secure) parts.push('Secure');
  if (sameSite) parts.push(`SameSite=${sameSite}`);
  return parts.join('; ');
}

module.exports = { parseCookies, isHttps, cookieString };

