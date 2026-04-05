import fs from 'fs';
import path from 'path';

const ROOT = process.cwd();
const PUBLIC_DIR = path.resolve(ROOT, 'public');

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

const EXCLUDE_FILES = new Set([
  'package.json',
  'package-lock.json',
  'pnpm-lock.yaml',
  'yarn.lock',
  'vercel.json',
  'next.config.js',
  'README.md',
]);

function ensureDir(p) {
  fs.mkdirSync(p, { recursive: true });
}

function rmDir(p) {
  if (!fs.existsSync(p)) return;
  fs.rmSync(p, { recursive: true, force: true });
}

function shouldCopyFile(abs) {
  const name = path.basename(abs);
  if (EXCLUDE_FILES.has(name)) return false;
  const ext = path.extname(name).toLowerCase();
  if (ext === '.html') return false;
  return true;
}

function copyTree(srcDir, destDir) {
  const entries = fs.readdirSync(srcDir, { withFileTypes: true });
  for (const e of entries) {
    const srcAbs = path.join(srcDir, e.name);
    const rel = path.relative(ROOT, srcAbs);
    const top = rel.split(path.sep)[0] || '';
    if (top && EXCLUDE_DIRS.has(top)) continue;

    const destAbs = path.join(destDir, e.name);
    if (e.isDirectory()) {
      ensureDir(destAbs);
      copyTree(srcAbs, destAbs);
      continue;
    }
    if (!e.isFile()) continue;
    if (!shouldCopyFile(srcAbs)) continue;
    ensureDir(path.dirname(destAbs));
    fs.copyFileSync(srcAbs, destAbs);
  }
}

function main() {
  rmDir(PUBLIC_DIR);
  ensureDir(PUBLIC_DIR);
  copyTree(ROOT, PUBLIC_DIR);
  process.stdout.write('Prepared public/ (copied static assets; excluded *.html)\n');
}

main();

