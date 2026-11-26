// SPDX-License-Identifier: GPL-3.0-only
// Copyright (C) 2026 Vishota — https://run.vishota.com · license text and the attribution term: LICENSE
// virun — expose a local port behind a public link.
//
//   virunx -p 3000              →  https://<project-name>.run.vishota.com
//   virun -p 5173 -s myapp     →  https://myapp.run.vishota.com
//
// Plain output, no colors, no spinners. Ctrl+C stops the tunnel.

import { existsSync, readFileSync, realpathSync } from 'node:fs';
import { join } from 'node:path';
import { defaultIdFromDir, sanitizeId, startTunnel } from '@virun/core';
import { readRuns, registerRun } from './runs';

const VERSION = '0.5.0';
const DEFAULT_RELAY = 'wss://relay.run.vishota.com';
/* «Appropriate Legal Notices» по GPL-3.0 (UD-051): автор, отсутствие гарантии,
 * право распространять на условиях лицензии и где читать её текст. Показываются
 * при каждом запуске — тогда и производная программа обязана показывать их же,
 * вместе с упоминанием автора (условие в конце LICENSE) */
const COPYRIGHT = 'Copyright (C) 2026 Vishota — https://run.vishota.com';
const LICENSE_LINE = 'GPL-3.0-only, no warranty: free software, free to redistribute under its conditions';

const USAGE = `virunx ${VERSION} — expose a local port behind a public link

usage
  virunx -p <port> [options]
  virunx list           what this device is serving right now

options
  -p, --port <n>      local port to tunnel (required)
  -s, --sub <name>    subdomain (default: project name)
  -r, --relay <url>   relay url (default: ${DEFAULT_RELAY})
  -t, --token <tok>   relay token (or set VIRUN_TOKEN)
  --no-rewrite-host   pass the public Host header as-is (default: the local
                      app sees localhost:<port>, which fixes vite/webpack
                      host checks without touching their configs)
  --no-p2p            relay all visitor traffic through the server (default:
                      browsers connect directly to this machine when possible)
  --hidden            do not list this tunnel on the relay's public page
                      (it is still counted in the public totals)
  --no-fallback       never serve visitors through the server: it carries only
                      page HTML and signalling, a visitor without a direct
                      connection gets nothing (default: the server is the
                      fallback when no direct connection is possible)
  -y, --yes           skip the security notice
  -h, --help          show this help

more
  virunx list          tunnels running on this device: links, ports, traffic
  virunx license       the license text: GPL-3.0 and the attribution term
  virunx help hosts    fix "Blocked request" / "Invalid Host header" errors

examples
  virunx -p 3000
  virunx -p 5173 -s myapp
  virunx -p 8000 -r ws://relay.example.com
`;

const HOSTS_HELP = `fixing "Blocked request" / "Invalid Host header" while tunneling

some dev servers only accept requests addressed to localhost. through virunx
the Host header is the public subdomain, so they reject it. allow virunx's
host in your dev server config and restart it:

  vite       vite.config.js
               export default defineConfig({
                 server: { allowedHosts: ['.run.vishota.com'] }
               })

  webpack    webpack.config.js -> devServer
               allowedHosts: 'all'          // dev only!

  angular    ng serve --disable-host-check
               (newer versions: "allowedHosts" in angular.json serve options)

  next.js    works out of the box with "next dev"

  other      search your dev server docs for "allowedHosts" or "host check"

virunx rewrites the Host header to localhost:<port> by default, so most dev
servers just work. if your app needs the real public Host header, restart
with --no-rewrite-host and use the fixes above.

if you use a different relay domain, replace ".run.vishota.com" accordingly.
`;

interface CliArgs {
  port?: number;
  sub?: string;
  relay?: string;
  token?: string;
  rewriteHost: boolean;
  p2p: boolean;
  hidden: boolean;
  fallback: boolean;
  yes: boolean;
  help: boolean;
}

class ArgError extends Error {}

function parseArgs(argv: string[]): CliArgs {
  const out: CliArgs = { rewriteHost: true, p2p: true, hidden: false, fallback: true, yes: false, help: false };
  for (let i = 0; i < argv.length; i++) {
    let a = argv[i];
    let inline: string | undefined;
    if (a.startsWith('--')) {
      const eq = a.indexOf('=');
      if (eq !== -1) {
        inline = a.slice(eq + 1);
        a = a.slice(0, eq);
      }
    }
    const value = (): string => {
      const v = inline !== undefined ? inline : argv[++i];
      if (v === undefined || v.startsWith('-')) throw new ArgError(`missing value for ${a}`);
      return v;
    };
    if (a === '-h' || a === '--help') out.help = true;
    else if (a === '-y' || a === '--yes') out.yes = true;
    else if (a === '--no-rewrite-host') out.rewriteHost = false;
    else if (a === '--no-p2p') out.p2p = false;
    else if (a === '--hidden') out.hidden = true;
    else if (a === '--no-fallback') out.fallback = false;
    else if (a === '-p' || a === '--port') out.port = Number(value());
    else if (a === '-s' || a === '--sub') out.sub = value();
    else if (a === '-r' || a === '--relay') out.relay = value();
    else if (a === '-t' || a === '--token') out.token = value();
    else throw new ArgError(`unknown option: ${a}`);
  }
  return out;
}

/** реальный путь node.exe: у nvm-windows «Program Files\nodejs» — символическая
 *  ссылка, а брандмауэр матчит процессы по реальному пути */
function nodePath(): string {
  try {
    return realpathSync(process.execPath);
  } catch {
    return process.execPath;
  }
}

function fmtBytes(n: number): string {
  if (n < 1024) return `${n}B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)}KB`;
  return `${(n / (1024 * 1024)).toFixed(1)}MB`;
}

/** human-readable tunnel host derived from the relay url */
function describeHost(id: string, relay: string): string {
  try {
    const h = new URL(relay).hostname;
    if (h.startsWith('relay.')) return `${id}.${h.slice('relay.'.length)}`;
    return `${id} via ${h}`;
  } catch {
    return id;
  }
}

/** публичная страница релея: relay.<suffix> → <suffix> (как describeHost) */
function publicPage(relay: string): string {
  try {
    const h = new URL(relay).hostname;
    return h.startsWith('relay.') ? h.slice('relay.'.length) : `the public page of ${h}`;
  } catch {
    return 'the public page';
  }
}

function fmtDuration(ms: number): string {
  const s = Math.max(0, Math.floor(ms / 1000));
  if (s < 60) return `${s}s`;
  const m = Math.floor(s / 60);
  if (m < 60) return `${m}m ${s % 60}s`;
  const h = Math.floor(m / 60);
  if (h < 24) return `${h}h ${m % 60}m`;
  return `${Math.floor(h / 24)}d ${h % 24}h`;
}

/** `virunx license`: текст лицензии из корня пакета (npm кладёт его туда всегда);
 *  файла нет — короткое уведомление и адрес текста */
function printLicense(): void {
  const file = join(__dirname, '..', 'LICENSE');
  if (existsSync(file)) {
    process.stdout.write(readFileSync(file, 'utf8'));
    return;
  }
  process.stdout.write(`${COPYRIGHT}\n${LICENSE_LINE}\nGNU GPL version 3: https://www.gnu.org/licenses/gpl-3.0.html\n`);
}

/** `virunx list`: что эта машина раздаёт прямо сейчас (UD-48) */
function listRuns(): void {
  const runs = readRuns();
  if (runs.length === 0) {
    console.log(`virunx ${VERSION} — no tunnels are running on this device`);
    console.log('\n  start one:  virunx -p <port>');
    return;
  }
  console.log(`virunx ${VERSION} — ${runs.length} tunnel${runs.length > 1 ? 's' : ''} running on this device`);
  for (const r of runs) {
    const relayHost = /^wss?:\/\/([^/]+)/i.exec(r.relay)?.[1] ?? r.relay;
    console.log();
    console.log(`  ${r.id}${r.hidden ? '  (hidden)' : ''}`);
    console.log(`     link     ${r.url ?? `via ${relayHost} (no public url)`}`);
    console.log(`     target   localhost:${r.port}`);
    console.log(`     up       ${fmtDuration(Date.now() - r.startedAt)}   pid ${r.pid}   virunx ${r.version}`);
    console.log(
      `     traffic  ${r.requests} request${r.requests === 1 ? '' : 's'}   ${fmtBytes(r.bytesIn)} in   ${fmtBytes(r.bytesOut)} out` +
        (r.lastRequestAt ? `   last ${fmtDuration(Date.now() - r.lastRequestAt)} ago` : '')
    );
    console.log(`     mode     p2p ${r.p2p ? 'on' : 'off'}   relay fallback ${r.fallback ? 'on' : 'off'}`);
  }
}

function main(): void {
  const argv = process.argv.slice(2);
  if (argv[0] === 'list') {
    listRuns();
    process.exit(0);
  }
  if (argv[0] === 'license') {
    printLicense();
    process.exit(0);
  }
  if (argv[0] === 'hosts' || (argv[0] === 'help' && argv[1] === 'hosts')) {
    process.stdout.write(HOSTS_HELP);
    process.exit(0);
  }

  let args: CliArgs;
  try {
    args = parseArgs(argv);
  } catch (e) {
    process.stderr.write(`error: ${(e as Error).message}\n\n${USAGE}`);
    process.exit(1);
  }
  if (args.help || argv[0] === 'help') {
    process.stdout.write(USAGE);
    process.exit(0);
  }
  if (args.port === undefined) {
    process.stderr.write(`error: -p/--port is required\n\n${USAGE}`);
    process.exit(1);
  }
  if (!Number.isInteger(args.port) || args.port < 1 || args.port > 65535) {
    process.stderr.write('error: -p must be a port number (1-65535)\n');
    process.exit(1);
  }
  const id = args.sub !== undefined ? sanitizeId(args.sub) : defaultIdFromDir(process.cwd());
  if (!id) {
    process.stderr.write(`error: "${args.sub}" is not a valid subdomain (use a-z, 0-9, -)\n`);
    process.exit(1);
  }
  const relay = args.relay ?? DEFAULT_RELAY;
  if (!/^wss?:\/\//i.test(relay)) {
    process.stderr.write('error: --relay must be a ws:// or wss:// url\n');
    process.exit(1);
  }
  const token = args.token ?? process.env.VIRUN_TOKEN;

  console.log(`virunx ${VERSION}`);
  console.log(`  legal    ${COPYRIGHT}`);
  console.log(`           ${LICENSE_LINE}; the text: virunx license`);
  console.log(`  relay    ${relay}`);
  console.log(`  target   ${describeHost(id, relay)} -> localhost:${args.port}`);
  console.log(
    `  host     ${args.rewriteHost ? `rewrite: the app sees localhost:${args.port}` : 'public: the app sees the real Host header'}${args.rewriteHost ? ' (off: --no-rewrite-host)' : ''}`
  );
  console.log(`  p2p      ${args.p2p ? 'on (browsers connect to this machine directly; off: --no-p2p)' : 'off (--no-p2p): all traffic via relay'}`);
  if (!args.fallback) {
    console.log('  relay    off (--no-fallback): visitors need a direct connection; the server carries only page HTML and signalling');
  }
  const page = publicPage(relay);
  console.log(
    `  list     ` +
      (args.hidden
        ? `hidden: not listed on ${page} (still counted in its totals)`
        : `public: listed on ${page} (--hidden to opt out)`)
  );
  console.log(`  token    ${token ? 'set' : 'not set (open relay)'}`);
  console.log();
  if (!args.yes) {
    console.log('  ! the link is public: anyone who has it can reach this app');
    console.log('  ! dev servers may block public hosts - fix: virunx help hosts');
    console.log("  ! don't tunnel apps with real secrets or admin panels");
    console.log();
  }

  const t = startTunnel({
    relay,
    id,
    port: args.port,
    hostRewrite: args.rewriteHost,
    p2p: args.p2p,
    hidden: args.hidden,
    fallback: args.fallback,
    version: VERSION,
    ...(token ? { token } : {}),
  });

  // реестр запущенных туннелей на этом устройстве — для `virunx list`
  const stats = { requests: 0, bytesIn: 0, bytesOut: 0, lastRequestAt: 0 };
  let registry: { touch(): void; remove(): void } | null = null;
  t.on('url', (url, tid) => {
    const shown = url ?? `tunnel "${tid}" online (direct relay, no public url)`;
    console.log(`  live     ${shown}`);
    if (!registry) {
      registry = registerRun(
        {
          version: VERSION,
          id: tid,
          port: args.port,
          url,
          relay,
          p2p: args.p2p,
          fallback: args.fallback,
          hidden: args.hidden,
          startedAt: Date.now(),
        },
        () => stats
      );
    }
    if (args.p2p && !t.p2pAvailable) {
      console.log('  ! p2p engine (node-datachannel) did not load: all traffic goes via the relay');
    }
    console.log('  press Ctrl+C to stop');
    console.log();
  });
  t.on('status', (s, info) => {
    if (s === 'reconnecting') {
      console.log(info?.message ? `  --       relay: ${info.message} — retrying` : '  --       relay connection lost, retrying');
    } else if (s === 'error') {
      console.error(`\n  error    ${info?.message ?? 'unknown error'}${info?.code ? ` (${info.code})` : ''}`);
      process.exit(1);
    }
  });
  t.on('request', (r) => {
    stats.requests += 1;
    stats.bytesIn += r.bytesIn;
    stats.bytesOut += r.bytesOut;
    stats.lastRequestAt = Date.now();
    registry?.touch();
    const time = new Date().toTimeString().slice(0, 8);
    const url = r.url.length > 40 ? `${r.url.slice(0, 39)}~` : r.url;
    const err = r.err ? `  !${r.err}` : '';
    console.log(
      `  ${time}  ${r.method.padEnd(7)} ${url.padEnd(40)} ${String(r.status).padStart(3)} ${String(r.ms).padStart(7)}ms ${fmtBytes(r.bytesOut).padStart(9)}${err}`
    );
  });

  // журнал прямого канала (UD-43): исход каждой попытки посетителя подключиться
  // напрямую; каждая из подсказок — один раз за запуск
  let hintedSame = false;
  let hintedOther = false;
  t.on('p2p', (e) => {
    const time = new Date().toTimeString().slice(0, 8);
    const who = `visitor ${e.vid.slice(0, 8)}`;
    if (e.kind === 'connected') {
      console.log(`  ${time}  P2P     ${who} direct${e.detail ? `: ${e.detail}` : ''}${e.sameIp ? ' (same network)' : ''}`);
      return;
    }
    console.log(
      `  ${time}  P2P     ${who} via relay: ${e.reason} · ${e.sameIp ? 'same network' : 'other network'} · ${e.detail}`
    );
    if (e.sameIp) {
      if (hintedSame) return;
      hintedSame = true;
      if (process.platform === 'win32') {
        console.log('  ! the visitor is on the same network as this machine, but no direct path was found.');
        console.log('    Usual cause on Windows: the firewall drops incoming UDP for node.exe on the current network profile.');
        console.log('    To allow it (PowerShell as administrator; with nvm-windows — again after a node version switch):');
        console.log(
          `      New-NetFirewallRule -DisplayName "virunx (node.exe)" -Direction Inbound -Action Allow -Program "${nodePath()}" -Protocol UDP -Profile Private,Domain`
        );
      } else {
        console.log('  ! the visitor is on the same network as this machine, but no direct path was found:');
        console.log('    allow incoming UDP for node in this machine\'s firewall, or check "AP/client isolation" on the router');
      }
    } else if (e.reason !== 'visitor-left' && e.reason !== 'no-report') {
      if (hintedOther) return;
      hintedOther = true;
      console.log('  ! a visitor on another network could not connect directly (their NAT or browser blocks WebRTC): served via the relay');
    }
  });

  let stopping = false;
  const stop = (): void => {
    if (stopping) process.exit(130);
    stopping = true;
    process.stdout.write('\n  stopping...\n');
    registry?.remove();
    void t.stop().then(() => process.exit(0));
  };
  process.on('SIGINT', stop);
  process.on('SIGTERM', stop);
}

main();
