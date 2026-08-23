// Bundles the tether frontend into web/dist for go:embed.
import { cpSync, mkdirSync, copyFileSync } from 'node:fs';
import { join } from 'node:path';

const root = new URL('.', import.meta.url).pathname;
const dist = join(root, 'dist');
mkdirSync(dist, { recursive: true });

const res = await Bun.build({
  entrypoints: [join(root, 'src/main.js')],
  format: 'iife',
  target: 'browser',
  minify: true,
  outdir: dist,
});

// normalize names regardless of bun's entry naming
{
  const { renameSync, existsSync, rmSync } = await import('node:fs');
  if (existsSync(join(dist, 'main.js'))) renameSync(join(dist, 'main.js'), join(dist, 'bundle.js'));
  if (existsSync(join(dist, 'main.css'))) rmSync(join(dist, 'main.css')); // xterm css served via /xterm.css
}
if (!res.success) {
  for (const log of res.logs) console.error(log);
  process.exit(1);
}

copyFileSync(join(root, 'index.html'), join(dist, 'index.html'));
copyFileSync(join(root, 'style.css'), join(dist, 'style.css'));
copyFileSync(
  join(root, 'node_modules/@xterm/xterm/css/xterm.css'),
  join(dist, 'xterm.css'),
);
mkdirSync(join(dist, 'fonts'), { recursive: true });
cpSync(join(root, 'assets/fonts'), join(dist, 'fonts'), { recursive: true });

// content-hash bundle filename so updated binaries never serve stale caches
const { readFileSync, writeFileSync, renameSync } = await import('node:fs');
const { createHash } = await import('node:crypto');
const js = readFileSync(join(dist, 'bundle.js'));
const h = createHash('md5').update(js).digest('hex').slice(0, 10);
renameSync(join(dist, 'bundle.js'), join(dist, `bundle.${h}.js`));
const idx = join(dist, 'index.html');
writeFileSync(idx, readFileSync(idx, 'utf8').replace('/bundle.js', `/bundle.${h}.js`));
console.log('bundled to web/dist (bundle.' + h + '.js)');

