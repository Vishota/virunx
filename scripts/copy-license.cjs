// SPDX-License-Identifier: GPL-3.0-only
// Copyright (C) 2026 Vishota — https://run.vishota.com · license text and the attribution term: LICENSE
// Ставит лицензию репозитория рядом с CLI: npm всегда кладёт LICENSE из корня
// пакета в архив. Концы строк — LF, чтобы опубликованный текст совпадал с
// каноническим байт в байт. Запускается из сборки packages/cli (UD-051).
const fs = require('node:fs');
const path = require('node:path');

const source = path.join(__dirname, '..', 'LICENSE');
const target = path.join(__dirname, '..', 'packages', 'cli', 'LICENSE');
fs.writeFileSync(target, fs.readFileSync(source, 'utf8').replace(/\r\n/g, '\n'));
