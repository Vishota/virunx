// SPDX-License-Identifier: GPL-3.0-only
// Copyright (C) 2026 Vishota — https://run.vishota.com · license text and the attribution term: LICENSE
// virun wire protocol, shared by the tunnel client (@virun/core) and the relay.
//
// Transport: one WebSocket connection per tunnel client.
//  - text frames   JSON control messages: { t: 'hello' | 'ok' | ... }
//  - binary frames payload chunks: [kind:1][ridLen:1][rid ascii][payload]
//
// Request/response multiplexing: every visitor request gets a `rid` generated
// by the relay; frames and control messages carry it until the exchange ends.

export const PROTOCOL_VERSION = 1;

/** Binary frame kinds (first byte of a binary frame). */
export const FrameKind = {
  /** request body bytes, relay -> client */
  ReqBody: 1,
  /** response body bytes, client -> relay */
  RespBody: 2,
  /** bytes from the visitor on an upgraded (websocket) connection, relay -> client */
  WsUp: 3,
  /** bytes from the local app on an upgraded (websocket) connection, client -> relay */
  WsDown: 4,
} as const;

export type FrameKindValue = (typeof FrameKind)[keyof typeof FrameKind];

const MAX_RID_LEN = 255;

export function encodeFrame(kind: number, rid: string, payload: Buffer = Buffer.alloc(0)): Buffer {
  const ridBuf = Buffer.from(rid, 'ascii');
  if (ridBuf.length > MAX_RID_LEN) throw new Error(`rid too long: ${rid}`);
  const out = Buffer.allocUnsafe(2 + ridBuf.length + payload.length);
  out[0] = kind;
  out[1] = ridBuf.length;
  ridBuf.copy(out, 2);
  payload.copy(out, 2 + ridBuf.length);
  return out;
}

export interface DecodedFrame {
  kind: number;
  rid: string;
  payload: Buffer;
}

export function decodeFrame(data: Buffer): DecodedFrame | null {
  if (data.length < 2) return null;
  const ridLen = data[1];
  if (data.length < 2 + ridLen) return null;
  return {
    kind: data[0],
    rid: data.toString('ascii', 2, 2 + ridLen),
    payload: data.subarray(2 + ridLen),
  };
}

// ---------------------------------------------------------------------------
// control messages (JSON, sent as text frames)
// ---------------------------------------------------------------------------

/** client -> relay: register the tunnel id. `p2p` announces WebRTC capability
 *  (relay will serve visitors the bootstrap/SW and relay signaling) */
export type HelloMsg = {
  t: 'hello';
  v: number;
  id: string;
  token?: string;
  p2p?: boolean;
  /** кадр, не принятый прямым каналом, клиент ставит в очередь, а не теряет
   *  (0.4.1+). Только такому клиенту релей помечает ответы заголовком
   *  `x-virun-p2p-download`, по нему Service Worker ведёт скачивание напрямую */
  p2pQueue?: boolean;
  /** не ходить через релей (UD-44): через VPS остаются только HTML-страницы,
   *  служебные скрипты и сигналинг; остальное — напрямую или никак */
  fallback?: boolean;
  /** клиент умеет мост кук (0.5.0+): релей даёт ключ, им клиент распечатывает
   *  cookie-заголовок прямых запросов страницы (UD-45) */
  cookieBridge?: boolean;
  /** версия пакета клиента: версии релея и пакета на транслирующем устройстве
   *  могут не совпадать — в админке видно, кто на каком (UD-47) */
  version?: string;
  /** скрыть туннель из публичного списка (UD-34) */
  hidden?: boolean;
};

/** relay -> client: registration confirmed; publicUrl may be null when the
 *  relay cannot infer the public address (e.g. direct IP connections).
 *  `flow: 1` announces the credit-window protocol: the client must not send
 *  more than the granted window of response-body bytes per rid without a
 *  `credit` message (old relays omit it — client sends freely).
 *  `iceServers` — STUN/TURN urls for the client's WebRTC endpoints. */
export type OkMsg = {
  t: 'ok';
  id: string;
  publicUrl: string | null;
  flow?: number;
  iceServers?: string[];
  /** base64 ключа AES-256-GCM моста кук — на одну жизнь подключения (UD-45) */
  cookieKey?: string;
};

export type ErrCode =
  | 'bad-version'
  | 'bad-id'
  | 'id-taken'
  | 'banned'
  | 'bad-token'
  | 'server-full';

export type ErrMsg = { t: 'err'; code: ErrCode; message: string };

/** relay -> client: a visitor request arrived. `url` is path + query.
 *  `upgrade: true` marks a websocket/hijacked connection. */
export type ReqMsg = {
  t: 'req';
  rid: string;
  method: string;
  url: string;
  headers: Record<string, string | string[]>;
  upgrade?: boolean;
  /** опечатанный релеем cookie-заголовок: страница пересылает его, не читая (UD-45) */
  sc?: string;
};

/** relay -> client: request body fully sent (may be zero-length) */
export type ReqEndMsg = { t: 'req-end'; rid: string };

/** client -> relay: local app responded. For upgrades, status 101 switches
 *  the exchange into raw bidirectional piping. */
export type RespStartMsg = {
  t: 'resp-start';
  rid: string;
  status: number;
  statusText?: string;
  headers: Record<string, string | string[]>;
  /** идентификатор Set-Cookie ответа: сами куки остались у релея, браузер
   *  применяет их родным ответом (UD-45) */
  cs?: string;
  /** опечатанный cookie-заголовок запроса не открылся — странице нужен свежий */
  cr?: boolean;
};
/** client -> relay: Set-Cookie ответа по прямому каналу — сохранить под `id` на
 *  минуту и применить браузеру, когда страница попросит (UD-45) */
export type CookiePutMsg = { t: 'cookie-put'; id: string; setCookie: string[] };

/** client -> relay: response fully sent */
export type RespEndMsg = { t: 'resp-end'; rid: string };

/** relay -> client: the visitor went away; drop the local request */
export type AbortMsg = { t: 'abort'; rid: string };

/** either side: an upgraded connection ended; tear down your end */
export type CloseMsg = { t: 'close'; rid: string };

/** relay -> client: flow-control grant, response-body bytes the client may
 *  now send for this rid (fair-share shaper backpressure, see p2p-plan §5) */
export type CreditMsg = { t: 'credit'; rid: string; n: number };

// --- P2P signaling (visitor WebRTC endpoints, p2p-plan §2) ------------------

/** visitor <-> relay <-> client: SDP/ICE blob for a visitor session `vid`.
 *  data — opaque JSON string (offer/answer/ice), relay never inspects it.
 *  sameIp (relay -> client): посетитель пришёл с того же адреса, что и клиент */
export type SignalMsg = { t: 'signal'; vid: string; data: string; sameIp?: boolean };

/** relay -> client: the visitor signaling socket went away — drop its PCs */
export type SignalCloseMsg = { t: 'signal-close'; vid: string };

/** visitor -> relay (via signal socket): WebRTC result for the session.
 *  ok:false идёт с причиной (ice-failed, no-webrtc, signal-closed, …): без неё
 *  страница молча рвала сигналинг, и релей не отличал «не пыталась» от «не пробилась» */
export type P2pStatusMsg = { t: 'p2p-status'; ok: boolean; reason?: string };

/** relay -> client: исход одной попытки посетителя подключиться напрямую.
 *  sameIp — посетитель пришёл с того же адреса, что и клиент (та же сеть) */
export type P2pResultMsg = {
  t: 'p2p-result';
  vid: string;
  ok: boolean;
  reason?: string;
  sameIp?: boolean;
};

/** client -> relay: periodic self-report of direct-path traffic (UD-11:
 *  числа неконтролируемы сервером, в дашборде помечаются self-report) */
export type StatsMsg = {
  t: 'stats';
  /** байты и запросы с прошлого отчёта (дельты, не кумулятив) */
  p2pBytesIn: number;
  p2pBytesOut: number;
  p2pReqs?: number;
  p2pConns: number;
};

export type ControlMsg =
  | HelloMsg
  | OkMsg
  | ErrMsg
  | ReqMsg
  | ReqEndMsg
  | RespStartMsg
  | RespEndMsg
  | AbortMsg
  | CloseMsg
  | CreditMsg
  | SignalMsg
  | SignalCloseMsg
  | P2pStatusMsg
  | P2pResultMsg
  | CookiePutMsg
  | StatsMsg;

export function parseControl(data: string): ControlMsg | null {
  try {
    const msg = JSON.parse(data);
    if (msg && typeof msg === 'object' && typeof msg.t === 'string') return msg as ControlMsg;
    return null;
  } catch {
    return null;
  }
}
