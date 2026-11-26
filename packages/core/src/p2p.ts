// SPDX-License-Identifier: GPL-3.0-only
// Copyright (C) 2026 Vishota — https://run.vishota.com · license text and the attribution term: LICENSE
// P2P bridge: WebRTC DataChannels посетителей ↔ движок туннеля (p2p-plan §4).
//
// node-datachannel — опциональная зависимость: если не загрузился (нет prebuilt),
// туннель работает как раньше, p2p-возможность просто не анонсируется
// (hello без `p2p: true`). Клиент — answering side: посетитель (страница) шлёт
// offer через сигналинг релея, здесь поднимается PeerConnection и принимается
// DataChannel.
//
// Протокол по DC — тот же, что relay↔клиент: JSON-control текстовыми
// сообщениями, бинарные фреймы [kind:1][ridLen:1][rid][payload]. Запросы
// попадают в те же хендлеры, ответы уходят через ctx.channel (Channel ниже).
//
// Flow control: credit-окна — механизм шейпера релея, в p2p их нет. Здесь
// backpressure по SCTP-буферу канала (bufferedAmount): frame() возвращает false,
// туннель ставит локальный ответ на паузу до ready().

import { createRequire } from 'node:module';
import {
  type ControlMsg,
  type SignalCloseMsg,
  type SignalMsg,
  decodeFrame,
  encodeFrame,
  parseControl,
} from '@virun/proto';

interface NdcDataChannel {
  sendMessage(msg: string): boolean;
  sendMessageBinary(buffer: Buffer): boolean;
  onMessage(cb: (msg: string | Buffer | ArrayBuffer) => void): void;
  onClosed(cb: () => void): void;
  bufferedAmount(): number;
  close(): void;
}
interface NdcCandidate {
  address: string;
  port: number;
  type: string;
}

interface NdcPeerConnection {
  onLocalDescription(cb: (sdp: string, type: string) => void): void;
  onLocalCandidate(cb: (candidate: string, mid: string) => void): void;
  onDataChannel(cb: (dc: NdcDataChannel) => void): void;
  setRemoteDescription(sdp: string, type: string): void;
  addRemoteCandidate(candidate: string, mid: string): void;
  iceState(): string;
  /** null, пока ICE не выбрал пару */
  getSelectedCandidatePair(): { local: NdcCandidate; remote: NdcCandidate } | null;
  close(): void;
}
interface NdcModule {
  PeerConnection: new (label: string, config: { iceServers: string[] }) => NdcPeerConnection;
  cleanup(): void;
}

function loadWebRtc(): NdcModule | null {
  try {
    // динамический require: пакет опциональный, его может не быть
    const req = createRequire(__filename);
    return req('node-datachannel') as NdcModule;
  } catch {
    return null;
  }
}

function toBuf(data: Buffer | Uint8Array | ArrayBuffer): Buffer {
  if (Buffer.isBuffer(data)) return data;
  if (data instanceof Uint8Array) return Buffer.from(data);
  return Buffer.from(data as ArrayBuffer);
}

/** канал источника запроса: relay-WS или WebRTC DataChannel посетителя.
 *  Ответы на запрос уходят в ТУТ ЖЕ источник (resp-start/frame/close) */
export interface Channel {
  /** relay-канал умирает вместе с WS (failCtxs); p2p-каналы живут своей жизнью */
  relay: boolean;
  ctrl(msg: ControlMsg): void;
  /** false = буфер канала переполнен, вызвать позже (после ready()) */
  frame(kind: number, rid: string, payload: Buffer): boolean;
  ready(): boolean;
  close(): void;
  label(): string;
}

export interface P2pCallbacks {
  /** signaling blob клиент → релей → посетитель */
  signal(vid: string, data: string): void;
  /** DC закрылся — label вида `p2p:<vid>` */
  channelClosed(label: string): void;
  /** control-сообщение от посетителя (req/req-end/abort) */
  control(msg: ControlMsg, chan: Channel): void;
  /** бинарный фрейм от посетителя (тело запроса) */
  frame(kind: number, rid: string, payload: Buffer): void;
  /** пора отчитаться статами в релей (DC открылся/закрылся; дальше по таймеру) */
  stats(): void;
  /** попытка посетителя получить прямой канал завершилась: ok — канал открыт,
   *  detail — выбранная пара; иначе reason и что видела эта машина (ICE,
   *  кандидаты) — «почему не p2p» читается по журналу клиента (UD-43) */
  outcome(vid: string, ok: boolean, reason: string, detail: string, sameIp: boolean): void;
}

/** self-report прямого трафика: дельты с прошлого отчёта, не кумулятив */
export interface P2pStats {
  bytesIn: number;
  bytesOut: number;
  /** обработанные запросы с прошлого отчёта */
  reqs: number;
  conns: number;
}

interface Session {
  pc: NdcPeerConnection;
  dcs: Set<NdcDataChannel>;
  /** кандидаты по типам: свои и посетителя */
  local: Record<string, number>;
  remote: Record<string, number>;
  /** хоть один DC открылся — попытка удалась, закрытие сессии не неудача */
  opened: boolean;
  /** посетитель из той же сети, что и клиент (судя по релею) */
  sameIp: boolean;
  timer: NodeJS.Timeout;
}

/** кандидаты по типам: "2 host, 1 srflx" — видно, донёс ли сигналинг адреса
 *  до обеих сторон и был ли у ICE шанс */
function noteCandidates(into: Record<string, number>, text: string): void {
  for (const line of text.split(/\r?\n/)) {
    if (!line.startsWith('a=candidate:')) continue;
    const m = / typ (\w+)/.exec(line);
    const type = m ? m[1] : 'unknown';
    into[type] = (into[type] ?? 0) + 1;
  }
}

function summarize(types: Record<string, number>): string {
  const keys = Object.keys(types).sort();
  return keys.length ? keys.map((k) => `${types[k]} ${k}`).join(', ') : 'none';
}

const DC_BACKLOG_LIMIT = 4_000_000;
const DC_FRAME_MAX = 65_536;
/** сессия без единого DC за это время — ICE не пробился, убираем */
const HANDSHAKE_TIMEOUT_MS = 20_000;

export class P2pBridge {
  private readonly ndc: NdcModule | null;
  private readonly sessions = new Map<string, Session>();
  private bytesIn = 0;
  private bytesOut = 0;
  private reportedIn = 0;
  private reportedOut = 0;
  private reqs = 0;
  private reportedReqs = 0;
  private closed = false;

  constructor(
    private readonly cbs: P2pCallbacks,
    private iceServers: string[]
  ) {
    this.ndc = loadWebRtc();
  }

  get available(): boolean {
    return this.ndc !== null;
  }

  setIceServers(servers: string[]): void {
    this.iceServers = servers;
  }

  /** байты и запросы с момента прошлого вызова + сколько DC открыто прямо сейчас */
  takeStats(): P2pStats {
    const bytesIn = Math.max(0, this.bytesIn - this.reportedIn);
    const bytesOut = Math.max(0, this.bytesOut - this.reportedOut);
    const reqs = Math.max(0, this.reqs - this.reportedReqs);
    this.reportedIn = this.bytesIn;
    this.reportedOut = this.bytesOut;
    this.reportedReqs = this.reqs;
    let conns = 0;
    for (const s of this.sessions.values()) conns += s.dcs.size;
    return { bytesIn, bytesOut, reqs, conns };
  }

  /** всё закрыть (Tunnel.stop): нативные PC держат event loop — чистим явно */
  closeAll(): void {
    this.closed = true;
    for (const vid of [...this.sessions.keys()]) this.drop(vid, 'shutdown');
    try {
      this.ndc?.cleanup();
    } catch {
      /* ignore */
    }
  }

  private drop(vid: string, reason: string): void {
    const s = this.sessions.get(vid);
    if (!s) return;
    this.sessions.delete(vid);
    clearTimeout(s.timer);
    // канал так и не открылся — владелец видит, что видела эта машина (UD-43)
    const failed = !s.opened && !this.closed;
    let ice = '';
    if (failed) {
      try {
        ice = s.pc.iceState();
      } catch {
        /* pc is already closed */
      }
    }
    for (const dc of s.dcs) {
      try {
        dc.close();
      } catch {
        /* ignore */
      }
    }
    try {
      s.pc.close();
    } catch {
      /* ignore */
    }
    if (failed) {
      this.cbs.outcome(
        vid,
        false,
        reason,
        `ice ${ice || 'unknown'} · candidates here ${summarize(s.local)}, visitor ${summarize(s.remote)}`,
        s.sameIp
      );
    }
  }

  /** signaling-сокет посетителя закрылся — релей просит убрать сессию */
  signalClose(vid: string): void {
    this.drop(vid, 'visitor-left');
  }

  /** blob от посетителя через релей (desc/ice) */
  signal(msg: SignalMsg | SignalCloseMsg): void {
    if (msg.t === 'signal-close') {
      this.signalClose(msg.vid);
      return;
    }
    const ndc = this.ndc;
    if (!ndc || this.closed) return;
    let blob: { type?: string; sdp?: string; sdpType?: string; candidate?: string; mid?: string };
    try {
      blob = JSON.parse(msg.data);
    } catch {
      return;
    }

    let s = this.sessions.get(msg.vid);
    if (!s) {
      if (blob.type !== 'desc' || !blob.sdp) return; // сессию открывает только offer
      const pc = new ndc.PeerConnection(`virun-${msg.vid}`, { iceServers: this.iceServers });
      const created: Session = {
        pc,
        dcs: new Set(),
        local: {},
        remote: {},
        opened: false,
        sameIp: false,
        timer: setTimeout(() => {
          if (created.dcs.size === 0) this.drop(msg.vid, 'no-channel');
        }, HANDSHAKE_TIMEOUT_MS),
      };
      s = created;
      this.sessions.set(msg.vid, s);
      pc.onLocalDescription((sdp, type) => {
        this.cbs.signal(msg.vid, JSON.stringify({ type: 'desc', sdpType: type, sdp }));
      });
      pc.onLocalCandidate((candidate, mid) => {
        noteCandidates(created.local, candidate);
        // libdatachannel отдаёт с префиксом "a=" — браузерный addIceCandidate ждёт без него
        this.cbs.signal(msg.vid, JSON.stringify({ type: 'ice', candidate: candidate.replace(/^a=/, ''), mid }));
      });
      pc.onDataChannel((dc) => {
        const cur = this.sessions.get(msg.vid);
        if (!cur) return;
        cur.dcs.add(dc);
        clearTimeout(cur.timer);
        this.wireChannel(msg.vid, cur, dc);
      });
    }
    if (msg.sameIp === true) s.sameIp = true;
    try {
      if (blob.type === 'desc' && blob.sdp && blob.sdpType) {
        noteCandidates(s.remote, blob.sdp);
        s.pc.setRemoteDescription(blob.sdp, blob.sdpType);
      } else if (blob.type === 'ice' && blob.candidate) {
        // браузер шлёт без "a=", libdatachannel принимает с "a=" — нормализуем
        const cand = blob.candidate.startsWith('a=') ? blob.candidate : `a=${blob.candidate}`;
        noteCandidates(s.remote, cand);
        s.pc.addRemoteCandidate(cand, blob.mid ?? '0');
      }
    } catch {
      /* некорректный blob от посетителя — игнор */
    }
  }

  /** выбранная пара кандидатов: "host 192.168.1.5:53114 ↔ host 192.168.1.7:53000" */
  private describePair(pc: NdcPeerConnection): string {
    try {
      const pair = pc.getSelectedCandidatePair();
      if (!pair) return '';
      return `${pair.local.type} ${pair.local.address}:${pair.local.port} ↔ ${pair.remote.type} ${pair.remote.address}:${pair.remote.port}`;
    } catch {
      return '';
    }
  }

  /** DC посетителя → Channel для движка туннеля */
  private wireChannel(vid: string, s: Session, dc: NdcDataChannel): void {
    if (!s.opened) {
      s.opened = true;
      this.cbs.outcome(vid, true, 'connected', this.describePair(s.pc), s.sameIp);
    }
    const chan: Channel = {
      relay: false,
      ctrl: (m) => {
        const text = JSON.stringify(m);
        this.bytesOut += Buffer.byteLength(text);
        try {
          dc.sendMessage(text);
        } catch {
          /* dc закрылся — SW посетителя уйдёт в fallback */
        }
      },
      frame: (kind, rid, payload) => {
        try {
          if (dc.bufferedAmount() > DC_BACKLOG_LIMIT) return false;
          for (let off = 0; off < payload.length; off += DC_FRAME_MAX) {
            const slice = payload.subarray(off, Math.min(off + DC_FRAME_MAX, payload.length));
            this.bytesOut += slice.length;
            dc.sendMessageBinary(encodeFrame(kind, rid, slice));
          }
          return true;
        } catch {
          return false;
        }
      },
      ready: () => {
        try {
          return dc.bufferedAmount() <= DC_BACKLOG_LIMIT / 2;
        } catch {
          return false;
        }
      },
      close: () => {
        try {
          dc.close();
        } catch {
          /* ignore */
        }
      },
      label: () => `p2p:${vid}`,
    };
    dc.onMessage((msg) => {
      if (typeof msg === 'string') {
        this.bytesIn += Buffer.byteLength(msg);
        const parsed = parseControl(msg);
        if (!parsed) return;
        if (parsed.t === 'req') this.reqs += 1;
        this.cbs.control(parsed, chan);
        return;
      }
      const buf = toBuf(msg as Buffer);
      this.bytesIn += buf.length;
      const f = decodeFrame(buf);
      if (f) this.cbs.frame(f.kind, f.rid, f.payload);
    });
    dc.onClosed(() => {
      s.dcs.delete(dc);
      this.cbs.channelClosed(chan.label());
      this.cbs.stats();
    });
    this.cbs.stats();
  }
}
