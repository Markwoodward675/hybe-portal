const http = require('http');
const fs = require('fs');
const path = require('path');

const apiHandler = require('../api/[...path].js');

const root = path.resolve(__dirname, '..');
const port = Number(process.env.PORT || 3000);

function mimeType(p) {
  const ext = path.extname(p).toLowerCase();
  if (ext === '.html') return 'text/html; charset=utf-8';
  if (ext === '.css') return 'text/css; charset=utf-8';
  if (ext === '.js' || ext === '.mjs') return 'application/javascript; charset=utf-8';
  if (ext === '.json') return 'application/json; charset=utf-8';
  if (ext === '.png') return 'image/png';
  if (ext === '.jpg' || ext === '.jpeg') return 'image/jpeg';
  if (ext === '.svg') return 'image/svg+xml; charset=utf-8';
  if (ext === '.ico') return 'image/x-icon';
  return 'application/octet-stream';
}

function safePathname(urlStr) {
  try {
    const u = new URL(urlStr, 'http://localhost');
    return decodeURIComponent(u.pathname);
  } catch {
    return '/';
  }
}

function attachQuery(req) {
  try {
    const u = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
    const query = Object.fromEntries(u.searchParams.entries());
    if (u.pathname && u.pathname.startsWith('/api/')) {
      const rest = u.pathname.slice('/api/'.length);
      query.path = rest.split('/').filter(Boolean);
    }
    req.query = query;
  } catch {
    req.query = {};
  }
}

function serveFile(filePath, res) {
  fs.readFile(filePath, (err, buf) => {
    if (err) {
      res.statusCode = 404;
      res.setHeader('content-type', 'text/plain; charset=utf-8');
      res.end('Not found');
      return;
    }
    res.statusCode = 200;
    res.setHeader('content-type', mimeType(filePath));
    res.end(buf);
  });
}

function tryServeStatic(req, res) {
  const pathname = safePathname(req.url);
  let rel = pathname;
  if (rel === '/') rel = '/index.html';
  const abs = path.resolve(root, '.' + rel);
  if (!abs.startsWith(root)) return false;
  try {
    const st = fs.statSync(abs);
    if (st.isDirectory()) {
      const index = path.join(abs, 'index.html');
      if (fs.existsSync(index)) {
        serveFile(index, res);
        return true;
      }
      return false;
    }
    if (st.isFile()) {
      serveFile(abs, res);
      return true;
    }
  } catch {
    return false;
  }
  return false;
}

const server = http.createServer(async (req, res) => {
  if (!req || !res) return;

  attachQuery(req);

  if (req.url && req.url.startsWith('/api/')) {
    try {
      await apiHandler(req, res);
    } catch (e) {
      res.statusCode = 500;
      res.setHeader('content-type', 'application/json; charset=utf-8');
      res.end(JSON.stringify({ error: e && e.message ? e.message : 'API error' }));
    }
    return;
  }

  const served = tryServeStatic(req, res);
  if (served) return;

  const fallback = path.join(root, 'index.html');
  serveFile(fallback, res);
});

server.listen(port, () => {
  process.stdout.write(`Dev server running at http://localhost:${port}\n`);
});
