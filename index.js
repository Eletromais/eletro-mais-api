const express = require('express');
const cors = require('cors');
const fs = require('fs');
const path = require('path');
const http = require('http');
const WebSocket = require('ws');
const Database = require('better-sqlite3');
const PDFDocument = require('pdfkit');
const admin = require('firebase-admin');

const PORT = process.env.PORT || 8080;

// ---------- Firebase Admin (push notifications) ----------
let firebaseReady = false;
try {
  if (process.env.FIREBASE_SERVICE_ACCOUNT) {
    const serviceAccount = JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT);
    admin.initializeApp({
      credential: admin.credential.cert(serviceAccount),
    });
    firebaseReady = true;
    console.log('Firebase Admin inicializado com sucesso.');
  } else {
    console.warn('FIREBASE_SERVICE_ACCOUNT não definida — notificações push desativadas.');
  }
} catch (err) {
  console.error('Erro ao inicializar Firebase Admin:', err);
}

const baseDir = process.env.NODE_ENV === 'production' ? '/tmp' : __dirname;
if (!fs.existsSync(baseDir)) fs.mkdirSync(baseDir, { recursive: true });

const dbPath = path.join(baseDir, 'database.db');
const DATA_FILE = path.join(baseDir, 'devices.json');

// ---------- SQLite ----------
let db;
try {
  db = new Database(dbPath);
  console.log(`Banco de dados SQLite carregado em: ${dbPath}`);
} catch (error) {
  console.error('Erro ao inicializar o banco SQLite:', error);
}

db.exec(`
  CREATE TABLE IF NOT EXISTS history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    deviceId TEXT NOT NULL,
    temperature REAL,
    vibration REAL,
    humidity INTEGER,
    compressorOn INTEGER,
    defrostOn INTEGER,
    createdAt TEXT NOT NULL
  );

  CREATE TABLE IF NOT EXISTS operations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    deviceId TEXT NOT NULL,
    type TEXT NOT NULL,
    status TEXT NOT NULL,
    startedAt TEXT NOT NULL,
    endedAt TEXT,
    durationSec INTEGER
  );

  CREATE TABLE IF NOT EXISTS alarms (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    deviceId TEXT NOT NULL,
    type TEXT NOT NULL,
    status TEXT NOT NULL,
    startedAt TEXT NOT NULL,
    endedAt TEXT,
    startValue REAL,
    endValue REAL,
    lastValue REAL,
    thresholdValue REAL
  );

  CREATE TABLE IF NOT EXISTS push_tokens (
    token TEXT PRIMARY KEY,
    platform TEXT,
    deviceIds TEXT,
    updatedAt TEXT
  );

  CREATE INDEX IF NOT EXISTS idx_history_device ON history(deviceId, createdAt);
  CREATE INDEX IF NOT EXISTS idx_operations_device ON operations(deviceId, startedAt);
  CREATE INDEX IF NOT EXISTS idx_alarms_device ON alarms(deviceId, startedAt);
`);

// ---------- devices.json (metadados + estado atual) ----------
function loadDevices() {
  try {
    if (!fs.existsSync(DATA_FILE)) {
      fs.writeFileSync(DATA_FILE, JSON.stringify({}), 'utf8');
      return {};
    }
    const data = fs.readFileSync(DATA_FILE, 'utf8');
    return JSON.parse(data || '{}');
  } catch (err) {
    console.error('Erro ao carregar devices.json:', err);
    return {};
  }
}

let devices = loadDevices();

function saveDevices() {
  try {
    fs.writeFileSync(DATA_FILE, JSON.stringify(devices, null, 2), 'utf8');
  } catch (err) {
    console.error('Erro ao salvar devices.json:', err);
  }
}

function defaultDevice(id, extra = {}) {
  return {
    id,
    name: extra.name || id,
    client: extra.client || '',
    location: extra.location || '',
    temperature: 0,
    vibration: 0,
    humidity: 0,
    compressorOn: false,
    defrostOn: false,
    sensorOk: true,
    online: false,
    updatedAt: null,
    tempMin: extra.tempMin ?? null,
    tempMax: extra.tempMax ?? null,
    alarmDelaySec: extra.alarmDelaySec ?? 0,
    offlineDelaySec: extra.offlineDelaySec ?? 120,
  };
}

// ---------- Express + WebSocket ----------
const app = express();
app.use(cors());
app.use(express.json());

const server = http.createServer(app);
const wss = new WebSocket.Server({ server, path: '/ws' });

wss.on('connection', (ws) => {
  console.log('Novo cliente WebSocket conectado.');
  ws.on('close', () => console.log('Cliente WebSocket desconectado.'));
});

function broadcast(data) {
  wss.clients.forEach((client) => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(JSON.stringify(data));
    }
  });
}

// ---------- Helpers de alarme/operação ----------
const insertAlarmStmt = db.prepare(`
  INSERT INTO alarms (deviceId, type, status, startedAt, startValue, lastValue, thresholdValue)
  VALUES (@deviceId, @type, 'ACTIVE', @startedAt, @startValue, @lastValue, @thresholdValue)
`);
const updateAlarmLastValueStmt = db.prepare(`
  UPDATE alarms SET lastValue = @lastValue WHERE id = @id
`);
const closeAlarmStmt = db.prepare(`
  UPDATE alarms SET status = 'RESOLVED', endedAt = @endedAt, endValue = @endValue WHERE id = @id
`);
const activeAlarmStmt = db.prepare(`
  SELECT * FROM alarms WHERE deviceId = ? AND type = ? AND status = 'ACTIVE' LIMIT 1
`);

const ALARM_TITLES = {
  TEMP_HIGH: 'Temperatura alta',
  TEMP_LOW: 'Temperatura baixa',
  COMM_OFFLINE: 'Perda de comunicação',
  SENSOR_TEMP_FAIL: 'Falha no sensor de temperatura',
};

async function sendPushForAlarm(deviceId, type, value) {
  if (!firebaseReady) return;

  try {
    const tokenRows = db.prepare(`SELECT token, deviceIds FROM push_tokens`).all();
    const tokens = tokenRows
      .filter((row) => {
        try {
          const ids = JSON.parse(row.deviceIds || '[]');
          return ids.includes(deviceId);
        } catch {
          return false;
        }
      })
      .map((row) => row.token);

    if (!tokens.length) return;

    const device = devices[deviceId];
    const deviceName = device ? device.name : deviceId;
    const title = ALARM_TITLES[type] || 'Alarme';
    let body = `${deviceName}`;
    if (type === 'TEMP_HIGH' || type === 'TEMP_LOW') {
      body += ` - ${value.toFixed(1)} °C`;
    } else {
      body += ' - verifique o equipamento';
    }

    const message = {
      notification: { title: `ELETRO MAIS: ${title}`, body },
      data: { deviceId, alarmType: type },
      tokens,
    };

    const result = await admin.messaging().sendEachForMulticast(message);
    console.log(`Push enviado: ${result.successCount} ok, ${result.failureCount} falhas.`);
  } catch (err) {
    console.error('Erro ao enviar push:', err);
  }
}

function openOrUpdateAlarm(deviceId, type, value, threshold) {
  const existing = activeAlarmStmt.get(deviceId, type);
  if (existing) {
    updateAlarmLastValueStmt.run({ id: existing.id, lastValue: value });
    return;
  }
  insertAlarmStmt.run({
    deviceId,
    type,
    startedAt: new Date().toISOString(),
    startValue: value,
    lastValue: value,
    thresholdValue: threshold ?? null,
  });
  broadcast({ type: 'alarm', deviceId, alarmType: type });
  sendPushForAlarm(deviceId, type, value);
}

function closeAlarmIfActive(deviceId, type, value) {
  const existing = activeAlarmStmt.get(deviceId, type);
  if (!existing) return;
  closeAlarmStmt.run({
    id: existing.id,
    endedAt: new Date().toISOString(),
    endValue: value,
  });
  broadcast({ type: 'alarm', deviceId, alarmType: type, resolved: true });
}

const openOperationStmt = db.prepare(`
  SELECT * FROM operations WHERE deviceId = ? AND type = ? AND status = 'ACTIVE' LIMIT 1
`);
const insertOperationStmt = db.prepare(`
  INSERT INTO operations (deviceId, type, status, startedAt) VALUES (?, ?, 'ACTIVE', ?)
`);
const closeOperationStmt = db.prepare(`
  UPDATE operations SET status = 'COMPLETED', endedAt = @endedAt, durationSec = @durationSec WHERE id = @id
`);

function handleCycle(deviceId, type, isOn) {
  const active = openOperationStmt.get(deviceId, type);
  if (isOn && !active) {
    insertOperationStmt.run(deviceId, type, new Date().toISOString());
  } else if (!isOn && active) {
    const startedAt = new Date(active.startedAt).getTime();
    const endedAt = Date.now();
    closeOperationStmt.run({
      id: active.id,
      endedAt: new Date(endedAt).toISOString(),
      durationSec: Math.max(0, Math.round((endedAt - startedAt) / 1000)),
    });
  }
}

const insertHistoryStmt = db.prepare(`
  INSERT INTO history (deviceId, temperature, vibration, humidity, compressorOn, defrostOn, createdAt)
  VALUES (@deviceId, @temperature, @vibration, @humidity, @compressorOn, @defrostOn, @createdAt)
`);

// Verifica dispositivos offline periodicamente
setInterval(() => {
  const now = Date.now();
  for (const id of Object.keys(devices)) {
    const d = devices[id];
    if (!d.updatedAt || !d.offlineDelaySec) continue;
    const last = new Date(d.updatedAt).getTime();
    const offline = (now - last) / 1000 > d.offlineDelaySec;
    if (offline && d.online) {
      d.online = false;
      saveDevices();
      openOrUpdateAlarm(id, 'COMM_OFFLINE', 0, null);
      broadcast({ type: 'telemetry', data: d });
    }
  }
}, 15000);

// ---------- Rotas ----------

app.get('/api/devices', (req, res) => {
  res.json(Object.values(devices));
});

app.post('/api/devices/register', (req, res) => {
  const { id, name, client, location } = req.body;
  if (!id || !name) {
    return res.status(400).json({ ok: false, message: 'ID e nome são obrigatórios.' });
  }
  devices[id] = defaultDevice(id, { name, client, location });
  saveDevices();
  res.json({ ok: true, device: devices[id] });
});

app.patch('/api/devices/:id/settings', (req, res) => {
  const { id } = req.params;
  const { tempMin, tempMax, alarmDelaySec, offlineDelaySec } = req.body;

  if (!devices[id]) {
    return res.status(404).json({ ok: false, message: 'Dispositivo não encontrado.' });
  }

  devices[id].tempMin = tempMin ?? null;
  devices[id].tempMax = tempMax ?? null;
  devices[id].alarmDelaySec = alarmDelaySec ?? 0;
  devices[id].offlineDelaySec = offlineDelaySec ?? 120;
  saveDevices();

  res.json({ ok: true, device: devices[id] });
});

app.post('/api/telemetry', (req, res) => {
  const {
    id, temperature, humidity, vibration,
    compressorOn, defrostOn, sensorOk,
  } = req.body;

  if (!id) {
    return res.status(400).json({ ok: false, message: 'ID do dispositivo é obrigatório.' });
  }

  const timestamp = new Date().toISOString();
  const existing = devices[id] || defaultDevice(id);

  const wasCompressorOn = existing.compressorOn;
  const wasDefrostOn = existing.defrostOn;

  devices[id] = {
    ...existing,
    id,
    temperature: temperature ?? existing.temperature ?? 0,
    humidity: humidity ?? existing.humidity ?? 0,
    vibration: vibration ?? existing.vibration ?? 0,
    compressorOn: compressorOn ?? existing.compressorOn ?? false,
    defrostOn: defrostOn ?? existing.defrostOn ?? false,
    sensorOk: sensorOk !== undefined ? sensorOk : (existing.sensorOk ?? true),
    online: true,
    updatedAt: timestamp,
  };
  saveDevices();

  const d = devices[id];

  // histórico
  insertHistoryStmt.run({
    deviceId: id,
    temperature: d.temperature,
    vibration: d.vibration,
    humidity: d.humidity,
    compressorOn: d.compressorOn ? 1 : 0,
    defrostOn: d.defrostOn ? 1 : 0,
    createdAt: timestamp,
  });

  // ciclos
  if (d.compressorOn !== wasCompressorOn) handleCycle(id, 'COMPRESSOR', d.compressorOn);
  if (d.defrostOn !== wasDefrostOn) handleCycle(id, 'DEFROST', d.defrostOn);

  // alarmes de temperatura
  if (d.tempMax != null && d.temperature > d.tempMax) {
    openOrUpdateAlarm(id, 'TEMP_HIGH', d.temperature, d.tempMax);
  } else {
    closeAlarmIfActive(id, 'TEMP_HIGH', d.temperature);
  }

  if (d.tempMin != null && d.temperature < d.tempMin) {
    openOrUpdateAlarm(id, 'TEMP_LOW', d.temperature, d.tempMin);
  } else {
    closeAlarmIfActive(id, 'TEMP_LOW', d.temperature);
  }

  // sensor
  if (d.sensorOk === false) {
    openOrUpdateAlarm(id, 'SENSOR_TEMP_FAIL', 0, null);
  } else {
    closeAlarmIfActive(id, 'SENSOR_TEMP_FAIL', 0);
  }

  // estava offline, voltou
  closeAlarmIfActive(id, 'COMM_OFFLINE', 0);

  broadcast({ type: 'telemetry', data: d });

  res.json({ ok: true, device: d });
});

// ---------- Histórico ----------
app.get('/api/history/:id/summary', (req, res) => {
  const { id } = req.params;
  const hours = parseInt(req.query.hours || '24', 10);
  const bucketMinutes = parseInt(req.query.bucketMinutes || '5', 10);
  const recentLimit = parseInt(req.query.recentLimit || '100', 10);

  const sinceIso = new Date(Date.now() - hours * 3600 * 1000).toISOString();

  const rows = db.prepare(`
    SELECT * FROM history WHERE deviceId = ? AND createdAt >= ? ORDER BY createdAt ASC
  `).all(id, sinceIso);

  const stats = {
    totalRecords: rows.length,
    minTemperature: null,
    avgTemperature: null,
    maxTemperature: null,
    firstAt: rows.length ? rows[0].createdAt : null,
    lastAt: rows.length ? rows[rows.length - 1].createdAt : null,
  };

  if (rows.length) {
    let min = rows[0].temperature, max = rows[0].temperature, sum = 0;
    for (const r of rows) {
      if (r.temperature < min) min = r.temperature;
      if (r.temperature > max) max = r.temperature;
      sum += r.temperature;
    }
    stats.minTemperature = min;
    stats.maxTemperature = max;
    stats.avgTemperature = sum / rows.length;
  }

  // agrupa em buckets
  const bucketMs = bucketMinutes * 60 * 1000;
  const buckets = new Map();
  for (const r of rows) {
    const t = new Date(r.createdAt).getTime();
    const bucketKey = Math.floor(t / bucketMs) * bucketMs;
    if (!buckets.has(bucketKey)) {
      buckets.set(bucketKey, { sum: 0, min: r.temperature, max: r.temperature, count: 0 });
    }
    const b = buckets.get(bucketKey);
    b.sum += r.temperature;
    b.count += 1;
    if (r.temperature < b.min) b.min = r.temperature;
    if (r.temperature > b.max) b.max = r.temperature;
  }

  const series = Array.from(buckets.entries())
    .sort((a, b) => a[0] - b[0])
    .map(([key, b]) => ({
      createdAt: new Date(key).toISOString(),
      temperature: b.sum / b.count,
      minTemperature: b.min,
      maxTemperature: b.max,
      samples: b.count,
    }));

  const recent = rows.slice(-recentLimit).reverse().map((r) => ({
    id: r.id,
    deviceId: r.deviceId,
    temperature: r.temperature,
    vibration: r.vibration,
    humidity: r.humidity,
    compressorOn: !!r.compressorOn,
    defrostOn: !!r.defrostOn,
    createdAt: r.createdAt,
  }));

  res.json({ deviceId: id, hours, bucketMinutes, stats, series, recent });
});

// ---------- Operações (ciclos) ----------
app.get('/api/operations/:id', (req, res) => {
  const { id } = req.params;
  const hours = parseInt(req.query.hours || '24', 10);
  const limit = parseInt(req.query.limit || '500', 10);
  const sinceIso = new Date(Date.now() - hours * 3600 * 1000).toISOString();

  const rows = db.prepare(`
    SELECT * FROM operations WHERE deviceId = ? AND startedAt >= ?
    ORDER BY startedAt DESC LIMIT ?
  `).all(id, sinceIso, limit);

  res.json(rows);
});

app.get('/api/operations/:id/summary', (req, res) => {
  const { id } = req.params;
  const hours = parseInt(req.query.hours || '24', 10);
  const sinceIso = new Date(Date.now() - hours * 3600 * 1000).toISOString();

  function summarizeType(type) {
    const rows = db.prepare(`
      SELECT * FROM operations WHERE deviceId = ? AND type = ? AND startedAt >= ?
    `).all(id, type, sinceIso);

    const completed = rows.filter((r) => r.status === 'COMPLETED' && r.durationSec != null);
    const activeRow = rows.find((r) => r.status === 'ACTIVE');

    const totalSec = completed.reduce((acc, r) => acc + r.durationSec, 0);
    const avgSec = completed.length ? Math.round(totalSec / completed.length) : 0;
    const maxSec = completed.length ? Math.max(...completed.map((r) => r.durationSec)) : 0;

    let activeSec = 0;
    if (activeRow) {
      activeSec = Math.round((Date.now() - new Date(activeRow.startedAt).getTime()) / 1000);
    }

    return {
      cycles: completed.length,
      totalSec,
      avgSec,
      maxSec,
      active: !!activeRow,
      activeSince: activeRow ? activeRow.startedAt : null,
      activeSec,
    };
  }

  res.json({
    deviceId: id,
    hours,
    compressor: summarizeType('COMPRESSOR'),
    defrost: summarizeType('DEFROST'),
  });
});

// ---------- Alarmes ----------
app.get('/api/alarms/:id', (req, res) => {
  const { id } = req.params;
  const status = (req.query.status || 'all').toLowerCase();
  const limit = parseInt(req.query.limit || '500', 10);

  let rows;
  if (status === 'active') {
    rows = db.prepare(`
      SELECT * FROM alarms WHERE deviceId = ? AND status = 'ACTIVE'
      ORDER BY startedAt DESC LIMIT ?
    `).all(id, limit);
  } else if (status === 'resolved') {
    rows = db.prepare(`
      SELECT * FROM alarms WHERE deviceId = ? AND status = 'RESOLVED'
      ORDER BY startedAt DESC LIMIT ?
    `).all(id, limit);
  } else {
    rows = db.prepare(`
      SELECT * FROM alarms WHERE deviceId = ?
      ORDER BY startedAt DESC LIMIT ?
    `).all(id, limit);
  }

  res.json(rows);
});

// ---------- Push (registro simples; envio real requer credenciais do Firebase) ----------
const upsertPushTokenStmt = db.prepare(`
  INSERT INTO push_tokens (token, platform, deviceIds, updatedAt)
  VALUES (@token, @platform, @deviceIds, @updatedAt)
  ON CONFLICT(token) DO UPDATE SET
    platform = excluded.platform,
    deviceIds = excluded.deviceIds,
    updatedAt = excluded.updatedAt
`);

app.post('/api/push/register', (req, res) => {
  const { token, platform, deviceIds } = req.body;
  if (!token) {
    return res.status(400).json({ ok: false, message: 'Token é obrigatório.' });
  }
  upsertPushTokenStmt.run({
    token,
    platform: platform || 'android',
    deviceIds: JSON.stringify(deviceIds || []),
    updatedAt: new Date().toISOString(),
  });
  res.json({ ok: true });
});

// ---------- Relatório PDF ----------
app.get('/api/reports/:id/pdf', (req, res) => {
  const { id } = req.params;
  const hours = parseInt(req.query.hours || '24', 10);
  const device = devices[id];

  if (!device) {
    return res.status(404).json({ ok: false, message: 'Dispositivo não encontrado.' });
  }

  const sinceIso = new Date(Date.now() - hours * 3600 * 1000).toISOString();
  const history = db.prepare(`
    SELECT * FROM history WHERE deviceId = ? AND createdAt >= ? ORDER BY createdAt ASC
  `).all(id, sinceIso);
  const alarms = db.prepare(`
    SELECT * FROM alarms WHERE deviceId = ? AND startedAt >= ? ORDER BY startedAt DESC
  `).all(id, sinceIso);

  let minT = null, maxT = null, avgT = null;
  if (history.length) {
    minT = Math.min(...history.map((h) => h.temperature));
    maxT = Math.max(...history.map((h) => h.temperature));
    avgT = history.reduce((a, h) => a + h.temperature, 0) / history.length;
  }

  res.setHeader('Content-Type', 'application/pdf');
  res.setHeader('Content-Disposition', `attachment; filename="relatorio_${id}.pdf"`);

  const doc = new PDFDocument({ margin: 40 });
  doc.pipe(res);

  doc.fontSize(20).text('ELETRO MAIS - Relatório de Monitoramento', { align: 'center' });
  doc.moveDown();
  doc.fontSize(12).text(`Equipamento: ${device.name} (${device.id})`);
  doc.text(`Cliente: ${device.client || '-'}`);
  doc.text(`Local: ${device.location || '-'}`);
  doc.text(`Período: últimas ${hours} horas`);
  doc.text(`Gerado em: ${new Date().toLocaleString('pt-BR')}`);
  doc.moveDown();

  doc.fontSize(14).text('Resumo de temperatura', { underline: true });
  doc.fontSize(12);
  doc.text(`Mínima: ${minT !== null ? minT.toFixed(1) + ' °C' : '-'}`);
  doc.text(`Média: ${avgT !== null ? avgT.toFixed(1) + ' °C' : '-'}`);
  doc.text(`Máxima: ${maxT !== null ? maxT.toFixed(1) + ' °C' : '-'}`);
  doc.text(`Total de registros: ${history.length}`);
  doc.moveDown();

  doc.fontSize(14).text('Alarmes no período', { underline: true });
  doc.fontSize(10);
  if (!alarms.length) {
    doc.text('Nenhum alarme registrado.');
  } else {
    alarms.slice(0, 40).forEach((a) => {
      const start = new Date(a.startedAt).toLocaleString('pt-BR');
      const end = a.endedAt ? new Date(a.endedAt).toLocaleString('pt-BR') : 'em aberto';
      doc.text(`${a.type} | início: ${start} | fim: ${end} | status: ${a.status}`);
    });
  }
  doc.moveDown();

  doc.fontSize(14).text('Leituras recentes (até 60)', { underline: true });
  doc.fontSize(9);
  history.slice(-60).reverse().forEach((h) => {
    const when = new Date(h.createdAt).toLocaleString('pt-BR');
    doc.text(
      `${when} - Temp: ${h.temperature.toFixed(1)}°C | Compressor: ${h.compressorOn ? 'LIGADO' : 'DESLIGADO'} | Degelo: ${h.defrostOn ? 'ATIVO' : 'DESLIGADO'}`
    );
  });

  doc.end();
});

// ---------- Status ----------
app.get('/', (req, res) => {
  res.send('API Eletro Mais em execução com sucesso!');
});

server.listen(PORT, '0.0.0.0', () => {
  console.log(`Servidor rodando na porta ${PORT}`);
});