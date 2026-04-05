const fs = require('fs');
const path = require('path');

const TEMPLATE_ROOT = path.resolve(process.cwd(), 'legacy', 'templates');

const ROUTE_ALIASES = new Map([
  ['/', 'index.html'],
  ['/flight', 'flight/dashboard.html'],
  ['/logistics', 'logistics/dashboard.html'],
  ['/admin', null],
  ['/admin/login', 'management.html'],
  ['/management', null],
  ['/scan', 'scan.html'],
]);

function normalizePathname(p) {
  let out = String(p || '').trim();
  if (!out.startsWith('/')) out = `/${out}`;
  out = out.replace(/\/{2,}/g, '/');
  if (out.length > 1 && out.endsWith('/')) out = out.slice(0, -1);
  return out;
}

function tryResolveTemplate(relativeHtmlPath) {
  const rel = String(relativeHtmlPath || '').replace(/^\/+/, '');
  if (!rel) return null;
  const tmplPath = path.join(TEMPLATE_ROOT, `${rel}.tmpl`);
  if (fs.existsSync(tmplPath)) return tmplPath;
  return null;
}

function resolveTemplateForPathname(pathname) {
  const p = normalizePathname(pathname);
  if (ROUTE_ALIASES.has(p)) {
    const dest = ROUTE_ALIASES.get(p);
    return dest ? tryResolveTemplate(dest) : { redirect: '/admin/login' };
  }

  const base = p.replace(/^\/+/, '');
  if (!base) return tryResolveTemplate('index.html');

  const ext = path.extname(base).toLowerCase();
  if (!ext) {
    return (
      tryResolveTemplate(`${base}.html`) ||
      tryResolveTemplate(`${base}/index.html`) ||
      null
    );
  }

  if (ext === '.html') {
    return tryResolveTemplate(base);
  }

  return null;
}

async function sendHtml(res, html) {
  res.statusCode = 200;
  res.setHeader('content-type', 'text/html; charset=utf-8');
  res.end(html);
}

export async function getServerSideProps(ctx) {
  const req = ctx.req;
  const res = ctx.res;

  const url = new URL(req.url || '/', `http://${req.headers.host || 'localhost'}`);
  const pathname = url.pathname || '/';

  const resolved = resolveTemplateForPathname(pathname);
  if (resolved && typeof resolved === 'object' && resolved.redirect) {
    return {
      redirect: { destination: resolved.redirect, permanent: false },
    };
  }

  if (!resolved) {
    res.statusCode = 404;
    res.setHeader('content-type', 'text/plain; charset=utf-8');
    res.end('Not found');
    return { props: {} };
  }

  try {
    const html = fs.readFileSync(resolved, 'utf8');
    await sendHtml(res, html);
  } catch {
    res.statusCode = 500;
    res.setHeader('content-type', 'text/plain; charset=utf-8');
    res.end('Failed to load template');
  }

  return { props: {} };
}

export default function LegacyPage() {
  return null;
}

