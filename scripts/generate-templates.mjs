import fs from 'fs';
import path from 'path';

const ROOT = process.cwd();
const TEMPLATE_ROOT = path.resolve(ROOT, 'legacy', 'templates');

const EXCLUDE_DIRS = new Set([
  'node_modules',
  '.git',
  '.next',
  'pages',
  'public',
  'legacy',
  'server_lib',
  'api',
  'scripts',
  'data',
]);

function walk(dir, out = []) {
  const entries = fs.readdirSync(dir, { withFileTypes: true });
  for (const e of entries) {
    const abs = path.join(dir, e.name);
    const rel = path.relative(ROOT, abs);
    const top = rel.split(path.sep)[0] || '';
    if (top && EXCLUDE_DIRS.has(top)) continue;
    if (e.isDirectory()) {
      walk(abs, out);
      continue;
    }
    if (e.isFile() && e.name.toLowerCase().endsWith('.html')) out.push(abs);
  }
  return out;
}

function ensureDir(p) {
  fs.mkdirSync(p, { recursive: true });
}

function writeTemplateFor(htmlAbs) {
  const rel = path.relative(ROOT, htmlAbs);
  const destAbs = path.join(TEMPLATE_ROOT, `${rel}.tmpl`);
  ensureDir(path.dirname(destAbs));
  const src = fs.readFileSync(htmlAbs, 'utf8');
  fs.writeFileSync(destAbs, src, 'utf8');
}

function main() {
  ensureDir(TEMPLATE_ROOT);
  const htmlFiles = walk(ROOT);
  htmlFiles.forEach(writeTemplateFor);
  process.stdout.write(`Generated ${htmlFiles.length} templates into ${path.relative(ROOT, TEMPLATE_ROOT)}\n`);
}

main();

