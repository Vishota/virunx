// SPDX-License-Identifier: GPL-3.0-only
// Copyright (C) 2026 Vishota — https://run.vishota.com · license text and the attribution term: LICENSE
// virun core — the tunnel client.
//
// One WebSocket connection to the relay, multiplexing any number of visitor
// HTTP requests and websocket connections down to one local port:
//
//   visitor -> relay -> [control WS] -> core -> http://127.0.0.1:<port>
//
// Normal HTTP goes through http.request (keep-alive agent). Websocket /
// upgraded connections are proxied as raw sockets: the handshake response is
// parsed manually, then bytes are piped both ways frame by frame.

import { EventEmitter } from 'node:events';
import crypto from 'node:crypto';
import fs from 'node:fs';
import http from 'node:http';
import net from 'node:net';
import path from 'node:path';
import WebSocket, { type RawData } from 'ws';
import {
  FrameKind,
  PROTOCOL_VERSION,
  decodeFrame,
  encodeFrame,
  parseControl,
  type ControlMsg,
  type ReqMsg,
} from '@virun/proto';
import { P2pBridge, type Channel } from './p2p';

export type TunnelStatus = 'connecting' | 'live' | 'reconnecting' | 'error' | 'stopped';

export interface StatusInfo {
  code?: string;
  message?: string;
}

export interface RequestStat {
  method: string;
  url: string;
  status: number;
  ms: number;
  /** request body bytes (visitor -> local app) */
  bytesIn: number;
  /** response body bytes (local app -> visitor) */
  bytesOut: number;
  err?: string;
}

export interface StartOptions {
  /** relay url, e.g. wss://relay.run.example.com or ws://127.0.0.1:8080/virun */
  relay: string;
  /** desired subdomain */
  id: string;
  /** local port to expose */
  port: number;
  /** relay token, if the relay requires one */
  token?: string;
  /** переписывать Host/Origin/Referer на localhost:<port> (UD-15, default on):
   *  дев-серверы с allowedHosts пропускают туннель без правки своих конфигов;
   *  публичный хост сохраняется в X-Forwarded-Host. Отключать для приложений,
   *  строящих абсолютные URL из Host, и multi-vhost */
  hostRewrite?: boolean;
  /** принимать прямые соединения посетителей (WebRTC), default on. off = весь
   *  трафик идёт только через релей */
  p2p?: boolean;
  /** пускать ли посетителей через релей, когда прямого канала нет (default on).
   *  off = через VPS идут только HTML-страницы, служебные скрипты и сигналинг;
   *  посетитель без прямого канала не получает ничего (UD-44) */
  fallback?: boolean;
  /** версия пакета (CLI передаёт свою): уходит в hello для админки и заголовком
   *  `x-virun-client` на каждом ответе — отладочная информация рядом с версией
   *  релея в `x-powered-by`; версии могут не совпадать (UD-47) */
  version?: string;
  /** не показывать туннель в публичном списке на апексе (в агрегатах он
   *  учитывается вместе со всеми) */
  hidden?: boolean;
}

export interface Tunnel extends EventEmitter {
  on(event: 'status', listener: (s: TunnelStatus, info?: StatusInfo) => void): this;
  on(event: 'url', listener: (url: string | null, id: string) => void): this;
  on(event: 'request', listener: (r: RequestStat) => void): this;
  on(event: 'p2p', listener: (e: P2pEvent) => void): this;
}

/** исход попытки посетителя подключиться напрямую (UD-43) — в журнал клиента */
export type P2pEvent =
  | { kind: 'connected'; vid: string; /** выбранная пара кандидатов */ detail: string; sameIp?: boolean }
  | {
      kind: 'failed';
      vid: string;
      /** причина: отчёт посетителя, иначе что видела эта машина */
      reason: string;
      /** посетитель из той же сети, что и клиент (судя по релею) */
      sameIp?: boolean;
      /** ice-состояние и кандидаты обеих сторон глазами клиента */
      detail: string;
    };

/** err codes after which retrying is pointless. `id-taken` и `server-full` —
 *  временные состояния релея: первый фатален только при первом подключении
 *  (имя занято — честная ошибка), при переподключении уходит в backoff */
const FATAL = new Set(['bad-version', 'bad-id', 'banned', 'bad-token']);

/** ключ моста кук из `ok`: 32 байта в base64; всё остальное — «моста нет» */
function openKey(b64: string | undefined): Buffer | null {
  if (typeof b64 !== 'string') return null;
  const key = Buffer.from(b64, 'base64');
  return key.length === 32 ? key : null;
}

/** распечатать cookie-заголовок, присланный страницей (UD-45): внутри
 *  base64(nonce[12] ‖ шифротекст ‖ тег[16]); null — не открылся */
function openSealed(key: Buffer, sealed: string): string | null {
  try {
    const raw = Buffer.from(sealed, 'base64');
    if (raw.length < 28) return null;
    const d = crypto.createDecipheriv('aes-256-gcm', key, raw.subarray(0, 12));
    d.setAuthTag(raw.subarray(raw.length - 16));
    return Buffer.concat([d.update(raw.subarray(12, raw.length - 16)), d.final()]).toString('utf8');
  } catch {
    return null;
  }
}

/** мост кук (UD-45): браузер не отдаёт странице ни HttpOnly-куки, ни Cookie
 *  запроса — она пересылает опечатанный релеем заголовок, здесь он распечатывается
 *  и встаёт настоящим Cookie. false — не открылся (ключ сменился при
 *  переподключении): странице нужен свежий; true — обработан или нечего делать */
function applySealedCookie(
  headers: Record<string, unknown>,
  sealed: string | undefined,
  relayChannel: boolean,
  key: Buffer | null
): boolean {
  if (relayChannel || !key || typeof sealed !== 'string') return true;
  const opened = openSealed(key, sealed);
  if (opened === null) return false;
  if (opened) headers.cookie = opened;
  else delete headers.cookie;
  return true;
}

/** переписать публичный URL в локальный вид (hostRewrite, UD-15);
 *  строки без публичного хоста не трогаем */
function localUrl(
  u: string | string[] | undefined,
  publicHost: string | undefined,
  port: number
): string | string[] | undefined {
  if (typeof u !== 'string' || !publicHost) return u;
  const re = new RegExp(`^https?://${publicHost.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}`, 'i');
  return re.test(u) ? u.replace(re, `http://localhost:${port}`) : u;
}

const BACKOFF_MS = [1000, 2000, 4000, 8000, 10000];

/** response-body window before the first relay credit (bytes; p2p-plan §5).
 *  Должно быть заведомо меньше очереди шейпера релея (QUEUE_CAP 512К): окно +
 *  перелёт одного BODY_CHUNK помещаются в очередь без overflow. */
const CREDIT_WINDOW = 256_000;
/** минимальный интервал между self-report'ами сверх таймера и событий канала */
const STATS_MIN_INTERVAL_MS = 5_000;
/** исходящие чанки тела дробим — перелёт окна ограничен одним BODY_CHUNK */
const BODY_CHUNK = 64_000;

/** hop-by-hop headers, never forwarded in either direction */
const HOP_HEADERS = [
  'connection',
  'keep-alive',
  'proxy-connection',
  'te',
  'trailer',
  'transfer-encoding',
  'upgrade',
];

interface Ctx {
  rid: string;
  kind: 'http' | 'ws';
  method: string;
  url: string;
  startedAt: number;
  bytesIn: number;
  bytesOut: number;
  status: number | null;
  /** resp-start has been sent to the relay */
  respStarted: boolean;
  /** exchange finished; context removed from the map */
  ended: boolean;
  /** куда уходят ответы на этот запрос: relay-WS или DC посетителя (p2p) */
  channel: Channel;
  /** credit-window balance for this rid (null = flow control off) */
  creditLeft?: number | null;
  /** paused by the shaper window; resume on 'credit' */
  pausedByCredit?: boolean;
  /** p2p: приостановлен из-за переполнения SCTP-буфера канала */
  pausedByBp?: boolean;
  /** chunks received while the window was empty (sent after a credit) */
  pendingBufs?: Buffer[];
  /** p2p: кадры, не принятые каналом (буфер полон), ждут здесь строго по порядку */
  bpQueue?: Array<{ kind: number; buf: Buffer }>;
  /** p2p: тело дочитано из приложения, resp-end уйдёт после опустошения очереди */
  bpEnd?: boolean;
  // http kind
  req?: http.ClientRequest;
  res?: http.IncomingMessage;
  // ws kind
  sock?: net.Socket;
  hsBuf?: Buffer;
  hsDone?: boolean;
  /** 101 received, raw piping active */
  pipe?: boolean;
  /** local socket paused due to relay backpressure */
  paused?: boolean;
}

function toBuf(data: RawData): Buffer {
  if (Buffer.isBuffer(data)) return data;
  if (Array.isArray(data)) return Buffer.concat(data);
  return Buffer.from(data);
}

export class Tunnel extends EventEmitter {
  readonly id: string;
  readonly port: number;

  private readonly relay: string;
  private readonly token?: string;
  private readonly hostRewrite: boolean;
  private ws: WebSocket | null = null;
  private readonly agent: http.Agent;
  private readonly ctxs = new Map<string, Ctx>();
  private state: TunnelStatus = 'connecting';
  private _publicUrl: string | null = null;
  private stopped = false;
  private flow = false;
  private retryCount = 0;
  /** туннель уже был зарегистрирован в этом процессе (идёт переподключение) */
  private everLive = false;
  /** последний отказ релея, не остановивший переподключение */
  private lastRefusal?: string;
  private readonly hidden: boolean;
  private readonly fallback: boolean;
  private retryTimer: NodeJS.Timeout | null = null;
  private pingTimer: NodeJS.Timeout | null = null;
  private readonly bpTimer: NodeJS.Timeout;
  private p2p: P2pBridge | null = null;
  /** отчёты релея об исходах попыток (p2p-result) до прихода signal-close.
   *  sameIp здесь — запасной источник: основным релей снабжает offer (signal) */
  private readonly p2pResults = new Map<string, Extract<ControlMsg, { t: 'p2p-result' }>>();
  private iceServers: string[] = [];
  /** ключ моста кук от релея (UD-45) */
  private cookieKey: Buffer | null = null;
  private readonly version: string | null;
  private statsTimer: NodeJS.Timeout | null = null;
  private lastStatsAt = 0;
  /** канал relay-WS: все ctxs с channel.relay=true умирают вместе с ним */
  private readonly relayChannel: Channel = {
    relay: true,
    ctrl: (msg) => {
      const ws = this.ws;
      if (ws && ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(msg));
    },
    frame: (kind, rid, payload) => {
      const ws = this.ws;
      if (ws && ws.readyState === WebSocket.OPEN) {
        ws.send(encodeFrame(kind, rid, payload));
        return true;
      }
      return false;
    },
    ready: () => {
      const ws = this.ws;
      return !!ws && ws.readyState === WebSocket.OPEN && ws.bufferedAmount <= 1024 * 1024;
    },
    close: () => {
      try {
        this.ws?.close();
      } catch {
        /* ignore */
      }
    },
    label: () => 'relay',
  };

  constructor(opts: StartOptions) {
    super();
    this.id = opts.id;
    this.port = opts.port;
    this.relay = opts.relay;
    this.token = opts.token;
    this.hostRewrite = opts.hostRewrite !== false;
    this.hidden = opts.hidden === true;
    this.fallback = opts.fallback !== false;
    this.version = typeof opts.version === 'string' && opts.version ? opts.version.slice(0, 32) : null;
    this.agent = new http.Agent({ keepAlive: true, maxSockets: 32, keepAliveMsecs: 15_000 });
    this.bpTimer = setInterval(() => this.resumeSockets(), 300);
    this.statsTimer = setInterval(() => this.reportStats(), 30_000);
    if (opts.p2p !== false) {
      this.p2p = new P2pBridge(
        {
          signal: (vid, data) => this.relayChannel.ctrl({ t: 'signal', vid, data }),
          channelClosed: (label) => {
            for (const ctx of [...this.ctxs.values()]) {
              if (ctx.channel.label() === label) this.closeCtx(ctx, 'p2p channel closed');
            }
          },
          control: (msg, chan) => this.onControl(msg, chan),
          frame: (kind, rid, payload) => this.onDecodedFrame(kind, rid, payload),
          stats: () => this.reportStats(),
          outcome: (vid, ok, reason, detail, sameIp) => this.onP2pOutcome(vid, ok, reason, detail, sameIp),
        },
        []
      );
    }
    this.statsTimer.unref();
    // deferred so that callers can attach listeners before the first event
    setImmediate(() => this.connect());
  }

  /** self-report прямого трафика в релей (UD-11): дельты с прошлого отчёта */
  private reportStats(): void {
    this.lastStatsAt = Date.now();
    if (!this.p2p) return;
    const st = this.p2p.takeStats();
    if (st.bytesIn + st.bytesOut + st.reqs + st.conns > 0) {
      this.relayChannel.ctrl({
        t: 'stats',
        p2pBytesIn: st.bytesIn,
        p2pBytesOut: st.bytesOut,
        p2pReqs: st.reqs,
        p2pConns: st.conns,
      });
    }
  }

  /** дашборд не ждёт таймера: после завершённого p2p-запроса — не чаще раза в 5 с */
  private reportStatsSoon(): void {
    if (Date.now() - this.lastStatsAt >= STATS_MIN_INTERVAL_MS) this.reportStats();
  }

  get status(): TunnelStatus {
    return this.state;
  }

  /** движок WebRTC загрузился (иначе p2p не анонсируется, трафик идёт через релей) */
  get p2pAvailable(): boolean {
    return this.p2p?.available ?? false;
  }

  get publicUrl(): string | null {
    return this._publicUrl;
  }

  async stop(): Promise<void> {
    if (this.stopped && this.state === 'stopped') return;
    this.stopped = true;
    this.clearTimers();
    this.p2p?.closeAll();
    this.failCtxs();
    const ws = this.ws;
    if (ws) {
      await new Promise<void>((resolve) => {
        const kill = setTimeout(() => {
          try {
            ws.terminate();
          } catch {
            /* already dead */
          }
          resolve();
        }, 250);
        ws.once('close', () => {
          clearTimeout(kill);
          resolve();
        });
        try {
          ws.close(1000, 'shutdown');
        } catch {
          clearTimeout(kill);
          resolve();
        }
      });
    }
    if (this.state !== 'error') this.setState('stopped');
  }

  // -- connection lifecycle ------------------------------------------------

  private connect(): void {
    if (this.stopped) return;
    this.setState(this.retryCount === 0 ? 'connecting' : 'reconnecting');
    const ws = new WebSocket(this.relay, { maxPayload: 8 * 1024 * 1024 });
    this.ws = ws;
    let lastPong = Date.now();
    ws.on('open', () => {
      const hello: ControlMsg = {
        t: 'hello',
        v: PROTOCOL_VERSION,
        id: this.id,
        ...(this.token ? { token: this.token } : {}),
        ...(this.p2p?.available ? { p2p: true, p2pQueue: true, cookieBridge: true } : {}),
        ...(this.hidden ? { hidden: true } : {}),
        ...(this.fallback ? {} : { fallback: false }),
        ...(this.version ? { version: this.version } : {}),
      };
      this.sendCtrl(hello);
    });
    ws.on('pong', () => {
      lastPong = Date.now();
    });
    ws.on('message', (data: RawData, isBinary: boolean) => this.onMessage(data, isBinary));
    ws.on('error', () => {
      /* 'close' always follows; reconnect logic lives there */
    });
    ws.on('close', () => this.onClose());
    this.pingTimer = setInterval(() => {
      if (ws.readyState !== WebSocket.OPEN) return;
      ws.ping();
      if (Date.now() - lastPong > 45_000) ws.terminate();
    }, 15_000);
  }

  private onClose(): void {
    if (this.pingTimer) {
      clearInterval(this.pingTimer);
      this.pingTimer = null;
    }
    this.failCtxs();
    this.ws = null;
    if (this.stopped) {
      if (this.state !== 'error') this.setState('stopped');
      return;
    }
    if (this.state === 'error') return; // fatal: no retry
    this.retryCount += 1;
    const base = BACKOFF_MS[Math.min(this.retryCount - 1, BACKOFF_MS.length - 1)];
    // на потолке — джиттер ±50%: куча клиентов после сбоя релея не стучится хором
    const delay = this.retryCount >= BACKOFF_MS.length ? base / 2 + Math.random() * (base / 2) : base;
    const refused = this.lastRefusal;
    this.lastRefusal = undefined;
    // не через setState: состояние может уже быть 'reconnecting', а причину
    // отказа (например, id-taken) пользователь должен видеть у каждой попытки
    this.state = 'reconnecting';
    this.emit('status', 'reconnecting', refused ? { message: refused } : undefined);
    this.retryTimer = setTimeout(() => this.connect(), delay);
  }

  private clearTimers(): void {
    if (this.retryTimer) clearTimeout(this.retryTimer);
    if (this.pingTimer) clearInterval(this.pingTimer);
    if (this.statsTimer) clearInterval(this.statsTimer);
    clearInterval(this.bpTimer);
  }

  private setState(s: TunnelStatus, info?: StatusInfo): void {
    if (this.state === s) return;
    this.state = s;
    this.emit('status', s, info);
  }

  // -- relay / p2p visitor -> us ----------------------------------------------

  private onMessage(data: RawData, isBinary: boolean): void {
    const buf = toBuf(data);
    if (isBinary) {
      this.onRidFrame(buf);
      return;
    }
    const msg = parseControl(buf.toString('utf8'));
    if (msg) this.onControl(msg, this.relayChannel);
  }

  /** бинарный фрейм запроса: тело пишем в локальный диал по rid (канал важен
   *  только для ответов — они уходят через ctx.channel) */
  private onRidFrame(buf: Buffer): void {
    const f = decodeFrame(buf);
    if (f) this.onDecodedFrame(f.kind, f.rid, f.payload);
  }

  private onDecodedFrame(kind: number, rid: string, payload: Buffer): void {
    const ctx = this.ctxs.get(rid);
    if (!ctx || ctx.ended) return;
    if (kind === FrameKind.ReqBody) {
      ctx.bytesIn += payload.length;
      ctx.req?.write(payload);
    } else if (kind === FrameKind.WsUp) {
      if (ctx.hsDone) ctx.sock?.write(payload);
    }
  }

  private onControl(msg: ControlMsg, channel: Channel): void {
    switch (msg.t) {
      case 'ok':
        this.retryCount = 0;
        this.everLive = true;
        this.lastRefusal = undefined;
        this.flow = msg.flow === 1;
        this._publicUrl = msg.publicUrl;
        this.iceServers = msg.iceServers ?? [];
        this.cookieKey = openKey(msg.cookieKey);
        this.p2p?.setIceServers(this.iceServers);
        this.setState('live');
        this.emit('url', msg.publicUrl, msg.id);
        break;
      case 'credit': {
        // шейпер релея освободил окно: сливаем отложенное (по BODY_CHUNK),
        // снимаем паузу, когда всё отложенное ушло
        const c = this.ctxs.get(msg.rid);
        if (!c || typeof c.creditLeft !== 'number' || c.ended) break;
        c.creditLeft += Math.max(0, msg.n);
        if (c.pausedByCredit && c.creditLeft > 0) {
          while (c.pendingBufs && c.pendingBufs.length > 0 && c.creditLeft > 0) {
            const chunk = c.pendingBufs[0];
            const take = Math.min(BODY_CHUNK, chunk.length);
            const part = chunk.subarray(0, take);
            c.bytesOut += take;
            this.sendFrame(FrameKind.RespBody, c.rid, part);
            c.creditLeft -= take;
            if (take < chunk.length) c.pendingBufs[0] = chunk.subarray(take);
            else c.pendingBufs.shift();
          }
          if (c.pendingBufs && c.pendingBufs.length === 0) {
            c.pausedByCredit = false;
            try {
              c.res?.resume();
            } catch {
              /* ignore */
            }
          }
        }
        break;
      }
      case 'err': {
        // id-taken при переподключении — временный: после смены сети релей
        // держит прежнее соединение до собственного таймаута (~45 с); клиент
        // ждёт в backoff, а не выходит — иначе туннель лежит до перезапуска
        const fatal = FATAL.has(msg.code) || (msg.code === 'id-taken' && !this.everLive);
        if (fatal) {
          this.stopped = true;
          this.setState('error', { code: msg.code, message: msg.message });
          try {
            this.ws?.close();
          } catch {
            /* ignore */
          }
        } else {
          this.lastRefusal = msg.message;
        }
        break;
      }
      case 'req':
        this.startReq(msg, channel);
        break;
      case 'req-end': {
        const c = this.ctxs.get(msg.rid);
        if (c) c.req?.end();
        break;
      }
      case 'abort':
      case 'close': {
        const c = this.ctxs.get(msg.rid);
        if (c) this.closeCtx(c, msg.t === 'abort' ? 'visitor aborted' : 'visitor closed');
        break;
      }
      case 'signal':
        this.p2p?.signal(msg);
        break;
      case 'signal-close':
        this.p2p?.signalClose(msg.vid);
        break;
      case 'p2p-result':
        // только с релея: по DC это прислал бы сам посетитель
        if (channel.relay) this.p2pResults.set(msg.vid, msg);
        break;
      default:
        break;
    }
  }

  /** журнал прямого канала: взгляд этой машины + отчёт релея (причина
   *  посетителя, та ли сеть). Приходит раньше signal-close, который закрывает сессию */
  private onP2pOutcome(
    vid: string,
    ok: boolean,
    reason: string,
    detail: string,
    sameIp: boolean
  ): void {
    const relay = this.p2pResults.get(vid);
    this.p2pResults.delete(vid);
    const net = relay?.sameIp ?? sameIp;
    if (ok) this.emit('p2p', { kind: 'connected', vid, detail, sameIp: net } satisfies P2pEvent);
    else {
      // «страница ушла молча» релею видно как no-report — своя формулировка точнее
      const why = relay?.reason && relay.reason !== 'no-report' ? relay.reason : reason;
      this.emit('p2p', { kind: 'failed', vid, reason: why, sameIp: net, detail } satisfies P2pEvent);
    }
  }

  // -- visitor request handling ----------------------------------------------

  private startReq(msg: ReqMsg, channel: Channel): void {
    // повтор rid (перезапуск SW посетителя сбрасывает его счётчик): старый
    // контекст закрываем, иначе его ответные кадры уйдут в новый запрос
    const stale = this.ctxs.get(msg.rid);
    if (stale && !stale.ended) this.closeCtx(stale, 'rid reuse');
    const ctx: Ctx = {
      rid: msg.rid,
      kind: msg.upgrade ? 'ws' : 'http',
      method: msg.method,
      url: msg.url,
      startedAt: Date.now(),
      bytesIn: 0,
      bytesOut: 0,
      status: null,
      respStarted: false,
      ended: false,
      channel,
      // credit-окна — только для relay-канала (шейпер релея); p2p-канал
      // ограничен backpressure'ом по bufferedAmount, credit'ов не шлёт
      creditLeft: msg.upgrade || !this.flow || !channel.relay ? null : CREDIT_WINDOW,
      pausedByCredit: false,
      pendingBufs: [],
    };
    this.ctxs.set(msg.rid, ctx);
    if (ctx.kind === 'ws') this.startUpgrade(ctx, msg);
    else this.startHttp(ctx, msg);
  }

  private startHttp(ctx: Ctx, msg: ReqMsg): void {
    const headers: http.OutgoingHttpHeaders = { ...msg.headers };
    for (const h of HOP_HEADERS) delete headers[h];
    let cr = false;
    if (!applySealedCookie(headers, msg.sc, ctx.channel.relay, this.cookieKey)) cr = true;
    if (this.hostRewrite) {
      const publicHost =
        typeof headers.host === 'string' ? headers.host : Array.isArray(headers.host) ? headers.host[0] : undefined;
      if (publicHost) {
        headers['x-forwarded-host'] = publicHost;
        const o = localUrl(headers.origin, publicHost, this.port);
        if (typeof o === 'string') headers.origin = o;
        const rf = localUrl(headers.referer, publicHost, this.port);
        if (typeof rf === 'string') headers.referer = rf;
        headers.host = `localhost:${this.port}`;
      }
    }
    // localhost + autoSelectFamily: дев-серверы биндятся и на ::1, и на
    // 127.0.0.1 (инцидент 2026-11-20: app на ::1 — ECONNREFUSED на 127.0.0.1);
    // Node 20+ перебирает семейства, до v20 опция просто игнорируется
    const opts: http.RequestOptions & { autoSelectFamily?: boolean } = {
      host: 'localhost',
      port: this.port,
      autoSelectFamily: true,
      method: msg.method,
      path: msg.url,
      headers,
      agent: this.agent,
    };
    const req = http.request(
      opts,
      (res) => {
        ctx.res = res;
        ctx.status = res.statusCode ?? 502;
        const rh: Record<string, string | string[]> = {};
        for (const [k, v] of Object.entries(res.headers)) {
          if (v !== undefined) rh[k] = v;
        }
        for (const h of HOP_HEADERS) delete rh[h];
        // версия пакета — на каждом ответе, на любом пути: по прямой релей ответа не видит (UD-47)
        if (this.version) rh['x-virun-client'] = this.version;
        // мост кук (UD-45): Set-Cookie прямого ответа не идёт в страницу открытым
        // видом — релей хранит их под id и применит браузеру родным ответом
        let cs: string | undefined;
        if (!ctx.channel.relay && this.cookieKey && Array.isArray(res.headers['set-cookie'])) {
          const list = res.headers['set-cookie']
            .filter((c): c is string => typeof c === 'string' && c.length <= 4096 && !/[\r\n]/.test(c))
            .slice(0, 20);
          if (list.length) {
            cs = crypto.randomBytes(12).toString('hex');
            this.relayChannel.ctrl({ t: 'cookie-put', id: cs, setCookie: list });
            delete rh['set-cookie'];
          }
        }
        this.sendCtrl({
          t: 'resp-start',
          rid: ctx.rid,
          status: ctx.status,
          statusText: res.statusMessage,
          headers: rh,
          ...(cs ? { cs } : {}),
          ...(cr ? { cr: true } : {}),
        });
        ctx.respStarted = true;
        // тело дробим на BODY_CHUNK и уважаем окно шейпера: окно пусто —
        // остаток в pendingBufs, сокет в паузу до 'credit' (пауза до отправки:
        // иначе 'end' не наступит и resp-end не уйдёт — дедлок с релеем)
        const sendBody = (buf: Buffer): void => {
          const win = ctx.creditLeft;
          if (typeof win !== 'number') {
            // flow control off (старый релей без flow:1, или p2p-канал) —
            // шлём как есть; p2p: очередь с backpressure (forward)
            this.forward(ctx, FrameKind.RespBody, buf);
            return;
          }
          let left = win;
          let off = 0;
          while (off < buf.length) {
            if (left <= 0) {
              ctx.creditLeft = left;
              ctx.pendingBufs!.push(buf.subarray(off));
              if (!ctx.pausedByCredit) {
                ctx.pausedByCredit = true;
                try {
                  res.pause();
                } catch {
                  /* ignore */
                }
              }
              return;
            }
            const take = Math.min(BODY_CHUNK, buf.length - off);
            const part = buf.subarray(off, off + take);
            ctx.bytesOut += take;
            this.sendFrame(FrameKind.RespBody, ctx.rid, part);
            left -= take;
            off += take;
          }
          ctx.creditLeft = left;
        };
        res.on('data', (chunk: Buffer) => sendBody(chunk));
        res.on('end', () => {
          // p2p: хвост тела ещё в очереди — resp-end уйдёт, когда канал его пропустит
          if (ctx.bpQueue?.length) {
            ctx.bpEnd = true;
            return;
          }
          this.sendCtrl({ t: 'resp-end', rid: ctx.rid });
          this.closeCtx(ctx);
        });
        const onResFail = (err: Error) => {
          if (ctx.ended) return;
          if (!ctx.respStarted) this.sendLocalError(ctx, err.message);
          else this.sendCtrl({ t: 'resp-end', rid: ctx.rid });
          this.closeCtx(ctx, err.message);
        };
        res.on('error', onResFail);
        res.on('aborted', () => onResFail(new Error('response aborted')));
      }
    );
    ctx.req = req;
    req.on('error', (err) => {
      if (ctx.ended) return;
      if (!ctx.respStarted) this.sendLocalError(ctx, err.message);
      else this.sendCtrl({ t: 'resp-end', rid: ctx.rid });
      this.closeCtx(ctx, err.message);
    });
  }

  private startUpgrade(ctx: Ctx, msg: ReqMsg): void {
    const headers: Record<string, string | string[]> = { ...msg.headers };
    applySealedCookie(headers, msg.sc, ctx.channel.relay, this.cookieKey);
    if (this.hostRewrite) {
      const publicHost = typeof headers.host === 'string' ? headers.host : undefined;
      if (publicHost) {
        headers['x-forwarded-host'] = publicHost;
        const o = localUrl(headers.origin, publicHost, this.port);
        if (o !== undefined) headers.origin = o;
        const rf = localUrl(headers.referer, publicHost, this.port);
        if (rf !== undefined) headers.referer = rf;
        headers.host = `localhost:${this.port}`;
      }
    }
    // dual-stack dial — как в startHttp (::1-only дев-серверы, инцидент 2026-11-20)
    const sock = net.connect({ host: 'localhost', port: this.port, autoSelectFamily: true });
    ctx.sock = sock;
    ctx.hsBuf = Buffer.alloc(0);
    let failed = false;

    const fail = (reason: string, status = 502): void => {
      if (failed || ctx.ended) return;
      failed = true;
      if (!ctx.respStarted) {
        const body = Buffer.from(`virun: upgrade failed: ${reason}\n`, 'utf8');
        this.sendCtrl({
          t: 'resp-start',
          rid: ctx.rid,
          status,
          statusText: 'Bad Gateway',
          headers: {
            'content-type': 'text/plain; charset=utf-8',
            'content-length': String(body.length),
          },
        });
        this.sendFrame(FrameKind.RespBody, ctx.rid, body);
        this.sendCtrl({ t: 'resp-end', rid: ctx.rid });
        ctx.respStarted = true;
        ctx.status = status;
      }
      this.closeCtx(ctx, reason);
    };

    sock.setTimeout(10_000, () => fail('local handshake timeout', 504));
    sock.on('error', (err) => fail(err.message));
    sock.on('close', () => {
      if (ctx.ended) return;
      if (ctx.pipe) {
        this.sendCtrl({ t: 'close', rid: ctx.rid });
        this.closeCtx(ctx, 'local socket closed');
      } else if (ctx.hsDone) {
        // non-101 response: body is complete once the local socket closes
        this.sendCtrl({ t: 'resp-end', rid: ctx.rid });
        this.closeCtx(ctx);
      } else {
        this.closeCtx(ctx, 'local socket closed before handshake');
      }
    });

    let head = `${msg.method} ${msg.url} HTTP/1.1\r\n`;
    for (const [k, v] of Object.entries(headers)) {
      head += `${k}: ${Array.isArray(v) ? v.join(', ') : v}\r\n`;
    }
    head += '\r\n';
    sock.write(head);

    sock.on('data', (chunk: Buffer) => {
      if (ctx.ended) return;
      if (!ctx.hsDone) {
        ctx.hsBuf = Buffer.concat([ctx.hsBuf as Buffer, chunk]);
        const idx = ctx.hsBuf.indexOf('\r\n\r\n');
        if (idx === -1) {
          if (ctx.hsBuf.length > 64 * 1024) fail('handshake too large');
          return;
        }
        const headStr = ctx.hsBuf.toString('latin1', 0, idx);
        const rest = ctx.hsBuf.subarray(idx + 4);
        ctx.hsBuf = Buffer.alloc(0);
        ctx.hsDone = true;
        sock.setTimeout(0);

        const lines = headStr.split('\r\n');
        const m = /^HTTP\/1\.[01] (\d{3})(?: (.*))?$/.exec(lines[0] ?? '');
        if (!m) {
          fail('malformed local handshake');
          return;
        }
        const status = parseInt(m[1], 10);
        const headers: Record<string, string | string[]> = {};
        for (let i = 1; i < lines.length; i++) {
          const ci = lines[i].indexOf(':');
          if (ci <= 0) continue;
          const k = lines[i].slice(0, ci).trim().toLowerCase();
          const v = lines[i].slice(ci + 1).trim();
          if (k in headers) {
            const prev = headers[k];
            headers[k] = Array.isArray(prev) ? [...prev, v] : [prev, v];
          } else {
            headers[k] = v;
          }
        }
        ctx.status = status;
        this.sendCtrl({
          t: 'resp-start',
          rid: ctx.rid,
          status,
          statusText: m[2] || undefined,
          headers,
        });
        ctx.respStarted = true;

        if (status === 101) {
          ctx.pipe = true;
          sock.setNoDelay(true);
          if (rest.length) this.forward(ctx, FrameKind.WsDown, rest);
        } else if (rest.length) this.forward(ctx, FrameKind.RespBody, rest);
        return;
      }
      // non-101 upgrade response body; cap the buffer defensively
      const kind = ctx.pipe ? FrameKind.WsDown : FrameKind.RespBody;
      if (ctx.channel.relay && ctx.pipe && this.ws && this.ws.bufferedAmount > 4 * 1024 * 1024) {
        sock.pause();
        ctx.paused = true;
      }
      this.forward(ctx, kind, chunk);
    });
  }

  private sendLocalError(ctx: Ctx, reason: string): void {
    const body = Buffer.from(
      `virun: cannot reach localhost:${this.port} (${reason})\n`,
      'utf8'
    );
    this.sendCtrl({
      t: 'resp-start',
      rid: ctx.rid,
      status: 502,
      statusText: 'Bad Gateway',
      headers: {
        'content-type': 'text/plain; charset=utf-8',
        'content-length': String(body.length),
      },
    });
    this.sendFrame(FrameKind.RespBody, ctx.rid, body);
    this.sendCtrl({ t: 'resp-end', rid: ctx.rid });
    ctx.respStarted = true;
    ctx.status = 502;
  }

  private closeCtx(ctx: Ctx, err?: string): void {
    if (ctx.ended) return;
    ctx.ended = true;
    this.ctxs.delete(ctx.rid);
    if (!ctx.channel.relay) this.reportStatsSoon();
    if (ctx.pipe) this.sendCtrl({ t: 'close', rid: ctx.rid });
    try {
      ctx.req?.destroy();
    } catch {
      /* ignore */
    }
    try {
      ctx.res?.destroy();
    } catch {
      /* ignore */
    }
    try {
      ctx.sock?.destroy();
    } catch {
      /* ignore */
    }
    this.emit('request', {
      method: ctx.method,
      url: ctx.url,
      status: ctx.status ?? 0,
      ms: Date.now() - ctx.startedAt,
      bytesIn: ctx.bytesIn,
      bytesOut: ctx.bytesOut,
      ...(err ? { err } : {}),
    });
  }

  /** relay-WS умер: рвём только его контексты — p2p-каналы живут своей жизнью */
  private failCtxs(): void {
    for (const ctx of [...this.ctxs.values()]) {
      if (ctx.channel.relay) this.closeCtx(ctx, 'relay connection lost');
    }
  }

  private resumeSockets(): void {
    const ws = this.ws;
    const relayOk = !!ws && ws.readyState === WebSocket.OPEN && ws.bufferedAmount <= 1024 * 1024;
    for (const c of this.ctxs.values()) {
      if (c.paused && c.sock && relayOk) {
        c.paused = false;
        c.sock.resume();
      }
      // p2p backpressure: буфер канала опустел — очередь уходит дальше
      if (c.bpQueue && c.bpQueue.length > 0 && c.channel.ready()) this.flushQueued(c);
    }
  }

  /** кадр вниз. Релей принимает всё; p2p — через очередь с backpressure */
  private forward(ctx: Ctx, kind: number, buf: Buffer): void {
    if (ctx.channel.relay) {
      ctx.bytesOut += buf.length;
      this.sendFrame(kind, ctx.rid, buf);
      return;
    }
    (ctx.bpQueue ??= []).push({ kind, buf });
    this.flushQueued(ctx);
  }

  /** p2p: очередь → канал, пока буфер канала принимает. frame() == false значит
   *  «не отправлено, буфер полон»: кадр остаётся в очереди, источник — на паузе,
   *  до ready() (resumeSockets). Потерянный кадр = обрезанный файл у посетителя
   *  без всякой ошибки — так и было до UD-43 */
  private flushQueued(ctx: Ctx): void {
    const q = ctx.bpQueue;
    if (!q) return;
    while (q.length > 0) {
      const { kind, buf } = q[0];
      if (!this.sendFrame(kind, ctx.rid, buf)) {
        if (!ctx.pausedByBp) {
          ctx.pausedByBp = true;
          this.pauseSource(ctx, true);
        }
        return;
      }
      ctx.bytesOut += buf.length;
      q.shift();
    }
    if (ctx.pausedByBp) {
      ctx.pausedByBp = false;
      this.pauseSource(ctx, false);
    }
    if (ctx.bpEnd) {
      ctx.bpEnd = false;
      this.sendCtrl({ t: 'resp-end', rid: ctx.rid });
      this.closeCtx(ctx);
    }
  }

  private pauseSource(ctx: Ctx, pause: boolean): void {
    try {
      if (pause) ctx.res?.pause();
      else ctx.res?.resume();
    } catch {
      /* ignore */
    }
    try {
      if (pause) ctx.sock?.pause();
      else ctx.sock?.resume();
    } catch {
      /* ignore */
    }
  }

  // -- us -> relay -----------------------------------------------------------

  /** маршрутизация по rid: ответ на запрос уходит в его источник
   *  (relay-WS или DC посетителя); сообщений без rid — в relay */
  private sendCtrl(msg: ControlMsg): void {
    const rid = (msg as { rid?: string }).rid;
    const chan = rid ? this.ctxs.get(rid)?.channel : undefined;
    (chan ?? this.relayChannel).ctrl(msg);
  }

  private sendFrame(kind: number, rid: string, payload: Buffer): boolean {
    const chan = this.ctxs.get(rid)?.channel ?? this.relayChannel;
    return chan.frame(kind, rid, payload);
  }
}

export function startTunnel(opts: StartOptions): Tunnel {
  return new Tunnel(opts);
}

// -- id helpers ---------------------------------------------------------------

/** normalize a raw string into a valid subdomain id, or null */
export function sanitizeId(raw: string): string | null {
  const s = raw
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9-]+/g, '-')
    .replace(/-+/g, '-')
    .replace(/^-+|-+$/g, '')
    .slice(0, 24)
    .replace(/-+$/g, '');
  return /^[a-z0-9][a-z0-9-]*$/.test(s) ? s : null;
}

/** pick a default id: package.json name, else directory name, else "app" */
export function defaultIdFromDir(dir: string): string {
  try {
    const pkg = JSON.parse(fs.readFileSync(path.join(dir, 'package.json'), 'utf8'));
    const name = typeof pkg?.name === 'string' ? (pkg.name.split('/').pop() ?? '') : '';
    const id = sanitizeId(name);
    if (id) return id;
  } catch {
    /* no package.json or not JSON — fall through */
  }
  return sanitizeId(path.basename(dir)) ?? 'app';
}
