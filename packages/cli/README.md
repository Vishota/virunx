# virunx

Expose a local port behind a public link. One command on your machine, one URL
to hand out:

```sh
npx virunx -p 3000
```

```
virunx 0.5.0
  legal    Copyright (C) 2026 Vishota — https://run.vishota.com
           GPL-3.0-only, no warranty: free software, free to redistribute under its
           conditions; the text: virunx license
  relay    wss://relay.run.vishota.com
  target   myapp.run.vishota.com -> localhost:3000
  host     rewrite: the app sees localhost:3000 (off: --no-rewrite-host)
  p2p      on (browsers connect to this machine directly; off: --no-p2p)
  list     public: listed on the public page of run.vishota.com (--hidden to opt out)
  token    not set (open relay)

  live     https://myapp.run.vishota.com
  press Ctrl+C to stop

  14:32:07  GET     /                        200      4ms      12B
  14:32:09  GET     /assets/index.js         200     31ms      41KB
```

Nothing is deployed and nothing is configured: the client dials out, so it
works behind any NAT or firewall that allows outgoing https. HTTP, SSE and
WebSockets are proxied, so dev-server hot reload keeps working through the
link.

Requires Node.js 18 or newer (20+ recommended). Run it with `npx` or install
it: `npm install -g virunx`.

## Where the traffic goes

Visitors' browsers connect to this machine **directly** when the network allows
it; the relay only introduces the two ends. What cannot go direct goes through
the relay:

| traffic | path |
| --- | --- |
| page assets, `fetch`/XHR, SSE responses | direct |
| websockets to the tunnel's own host (HMR, app sockets) | direct |
| HTML documents, non-browser clients, direct connection impossible | relay |

A direct connection is impossible when both sides sit behind symmetric NAT or
UDP is blocked; the page then falls back to the relay by itself. To keep the
relay out entirely, start with `--no-fallback`: the relay then carries only
what boots the direct channel, and a visitor who cannot connect directly gets
nothing.

## Options

```sh
virunx -p 3000                          # subdomain = project name (package.json / folder)
virunx -p 5173 -s myapp                 # pick the subdomain yourself
virunx -p 8000 -r wss://relay.example.com   # use your own relay
virunx -t <token> -p 3000               # when the relay requires a token
```

| flag | meaning |
| --- | --- |
| `-p, --port <n>` | local port to tunnel (required — no magic detection) |
| `-s, --sub <name>` | subdomain, default: project name |
| `-r, --relay <url>` | relay url, default: `wss://relay.run.vishota.com` |
| `-t, --token <tok>` | relay token (or the `VIRUN_TOKEN` environment variable) |
| `--no-rewrite-host` | pass the public `Host` header to the app as-is |
| `--no-p2p` | relay all visitor traffic; no direct connections |
| `--hidden` | do not list the tunnel on the relay's public page (it is still counted in the totals) |
| `--no-fallback` | never serve visitors through the relay |
| `-y, --yes` | skip the security notice |

Stop with `Ctrl+C`.

### `virunx list`

What this device is serving right now — every running tunnel with its link,
local port, uptime, traffic and mode:

```
$ virunx list
virunx 0.5.0 — 2 tunnels running on this device

  myapp
     link     https://myapp.run.vishota.com
     target   localhost:3000
     up       12m 4s   pid 41842   virunx 0.5.0
     traffic  213 requests   1.2MB in   38.4MB out   last 3s ago
     mode     p2p on   relay fallback on
```

### `virunx license`

Prints the license text — GPL-3.0 and the attribution term at its end. The
banner of every run shows the short notice: the author, that there is no
warranty, that the program is free to redistribute under its conditions, and
where to read the text.

### when the app rejects the public host

By default the app sees `Host: localhost:<port>`, so Vite, webpack-dev-server
and friends work without touching their configs. Apps that build absolute URLs
from `Host` need `--no-rewrite-host` — and then the dev server has to allow the
tunnel's host. `virunx help hosts` lists the fix per framework:

```js
// vite.config.js
export default defineConfig({ server: { allowedHosts: ['.run.vishota.com'] } })
```

## Before you share the link

- **The link is public.** Anyone who has the address can open the app. The
  relay's public page lists running tunnels by name; `--hidden` keeps a tunnel
  off that list, but the address still works for anyone who knows it.
- **The relay sees what crosses it.** That is the HTML pages and everything a
  visitor loads when the direct connection is impossible. Direct traffic never
  passes through the relay; the client only reports its byte totals for the
  public counters.
- **The URL never changes.** The tunnel adds no query parameters and rewrites
  no paths; its own metadata travels in `x-virun-*` headers, which never reach
  the app.
- **Sessions work.** Cookies, including `HttpOnly` ones, are set and sent on
  both paths, and the app sees `Origin` and `Referer` on the direct path too.

## For visitors

The first visit to a tunnel installs a small Service Worker and reloads the
page once; from then on the browser fetches from this machine directly. A
visitor needs a current browser with JavaScript on. `window.virunx` in the
visitor's console shows the state of the channel.

## Sources

The sources of this package: <https://github.com/Vishota/virunx>. They build the
same bundle that npm installs; the relay is a separate, private project.

## Your own relay

The public relay is `relay.run.vishota.com`. The relay itself is not published
for now; the client speaks to any relay that implements the same protocol:
`virunx -p 3000 -r wss://relay.example.com`.

- Instructions and live usage of the public relay: <https://run.vishota.com>

## License

GPL-3.0-only, © 2026 Vishota, <https://run.vishota.com>. The full text is in
`LICENSE` and prints with `virunx license`. Its end carries the additional term
that section 7 of the license allows: whoever conveys the program or a work
based on it keeps the attribution «virunx — Copyright (C) 2026 Vishota —
https://run.vishota.com» in the material and in the legal notices its
interfaces show.
