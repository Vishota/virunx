// SPDX-License-Identifier: GPL-3.0-only
// Copyright (C) 2026 Vishota — https://run.vishota.com · license text and the attribution term: LICENSE
// Реестр запущенных туннелей на этом устройстве: `virunx list` показывает, что
// и с какого порта раздаёт эта машина прямо сейчас (UD-48).
//
// Каждый virunx пишет ~/.virunx/run/<pid>.json и обновляет в нём счётчики;
// список читает файлы и выбрасывает записи мёртвых процессов. Реестр
// best-effort: если домашний каталог недоступен для записи — просто пуст.

import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

export interface RunStats {
  requests: number;
  bytesIn: number;
  bytesOut: number;
  lastRequestAt: number;
}

export interface RunRecord extends RunStats {
  pid: number;
  version: string;
  id: string;
  port: number;
  url: string | null;
  relay: string;
  p2p: boolean;
  fallback: boolean;
  hidden: boolean;
  startedAt: number;
}

const STATS_REFRESH_MS = 5000;
const TOUCH_DELAY_MS = 200;

export function runDir(): string {
  return path.join(os.homedir(), '.virunx', 'run');
}

function alive(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch (e) {
    // чужой процесс: он есть, просто сигнал нельзя — считаем живым
    return (e as NodeJS.ErrnoException).code === 'EPERM';
  }
}

/** туннели, работающие прямо сейчас; записи мёртвых процессов убираются */
export function readRuns(): RunRecord[] {
  const dir = runDir();
  let names: string[] = [];
  try {
    names = fs.readdirSync(dir);
  } catch {
    return [];
  }
  const out: RunRecord[] = [];
  for (const name of names) {
    if (!/^\d+\.json$/.test(name)) continue;
    const file = path.join(dir, name);
    let rec: RunRecord | null = null;
    try {
      rec = JSON.parse(fs.readFileSync(file, 'utf8'));
    } catch {
      rec = null;
    }
    if (!rec || typeof rec !== 'object' || typeof rec.id !== 'string' || !alive(Number(name.slice(0, -5)))) {
      try {
        fs.unlinkSync(file);
      } catch {
        /* уже убрал другой virunx */
      }
      continue;
    }
    out.push(rec);
  }
  return out.sort((a, b) => a.startedAt - b.startedAt);
}

/** записаться в реестр; возвращает touch (обновить счётчики) и remove (выйти) */
export function registerRun(
  base: Omit<RunRecord, keyof RunStats | 'pid'>,
  stats: () => RunStats
): { touch(): void; remove(): void } {
  const file = path.join(runDir(), `${process.pid}.json`);
  const tmp = `${file}.${process.pid}.tmp`;
  let removed = false;
  let timer: NodeJS.Timeout | null = null;
  const write = (): void => {
    if (removed) return;
    try {
      fs.mkdirSync(path.dirname(file), { recursive: true });
      // через временный файл: список не должен читать недописанную запись
      fs.writeFileSync(tmp, JSON.stringify({ ...base, pid: process.pid, ...stats() }));
      fs.renameSync(tmp, file);
    } catch {
      /* реестр best-effort */
    }
  };
  write();
  const refresh = setInterval(write, STATS_REFRESH_MS);
  refresh.unref();
  const touch = (): void => {
    if (timer) return;
    timer = setTimeout(() => {
      timer = null;
      write();
    }, TOUCH_DELAY_MS);
    timer.unref?.();
  };
  const remove = (): void => {
    if (removed) return;
    removed = true;
    clearInterval(refresh);
    if (timer) clearTimeout(timer);
    try {
      fs.unlinkSync(file);
    } catch {
      /* already gone */
    }
  };
  process.on('exit', remove);
  return { touch, remove };
}
