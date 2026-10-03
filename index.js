const express = require('express');
const cors = require('cors');
const path = require('path');
const fs = require('fs');
const http = require('http');
const WebSocket = require('ws');
const Database = require('better-sqlite3');
const { initializeApp, cert } = require('firebase-admin/app');
const { getMessaging } = require('firebase-admin/messaging');
const PDFDocument = require('pdfkit');

const PORT = process.env.PORT || 8080;

const VOLUME_MOUNT_PATH = process.env.RAILWAY_VOLUME_MOUNT_PATH || '';
const DB_FILE =
  process.env.DB_FILE ||
  (VOLUME_MOUNT_PATH
    ? path.join(VOLUME_MOUNT_PATH, 'eletro_mais.db')
    : path.join(__dirname, 'eletro_mais.db'));

const LEGACY_DATA_FILE = path.join(__dirname, 'devices.json');

const FIREBASE_PROJECT_ID =
  process.env.FIREBASE_PROJECT_ID ||
  'eletro-mais-refrigeracao';

const FIREBASE_SERVICE_ACCOUNT_JSON =
  process.env.FIREBASE_SERVICE_ACCOUNT_JSON || '';

const FIREBASE_SERVICE_ACCOUNT_FILE =
  process.env.FIREBASE_SERVICE_ACCOUNT_FILE ||
  path.join(__dirname, 'firebase-service-account.json');

const REPORT_TIME_ZONE =
  process.env.REPORT_TIME_ZONE ||
  'America/Sao_Paulo';

let firebasePushReady = false;
const OFFLINE_CHECK_INTERVAL_MS = 15000;

const app = express();
app.use(cors());
app.use(express.json({ limit: '256kb' }));

const db = new Database(DB_FILE);
db.pragma('journal_mode = WAL');
db.pragma('foreign_keys = ON');

db.exec(`
CREATE TABLE IF NOT EXISTS devices (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  client TEXT NOT NULL DEFAULT '',
  location TEXT NOT NULL DEFAULT '',
  temperature REAL NOT NULL DEFAULT 0,
  vibration REAL NOT NULL DEFAULT 0,
  humidity REAL NOT NULL DEFAULT 0,
  compressor_on INTEGER NOT NULL DEFAULT 0,
  defrost_on INTEGER NOT NULL DEFAULT 0,
  updated_at TEXT,
  temp_min REAL,
  temp_max REAL,
  alarm_delay_sec INTEGER NOT NULL DEFAULT 0,
  high_since TEXT,
  low_since TEXT
);

CREATE TABLE IF NOT EXISTS telemetry_history (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  device_id TEXT NOT NULL,
  temperature REAL NOT NULL,
  vibration REAL NOT NULL DEFAULT 0,
  humidity REAL NOT NULL DEFAULT 0,
  compressor_on INTEGER NOT NULL DEFAULT 0,
  defrost_on INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL,
  FOREIGN KEY (device_id) REFERENCES devices(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_history_device_time
ON telemetry_history(device_id, created_at);

CREATE TABLE IF NOT EXISTS alarms (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  device_id TEXT NOT NULL,
  type TEXT NOT NULL,
  status TEXT NOT NULL,
  started_at TEXT NOT NULL,
  ended_at TEXT,
  start_value REAL NOT NULL,
  end_value REAL,
  last_value REAL NOT NULL,
  threshold_value REAL NOT NULL,
  FOREIGN KEY (device_id) REFERENCES devices(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_alarms_device_status
ON alarms(device_id, status, started_at);

CREATE TABLE IF NOT EXISTS push_tokens (
  token TEXT PRIMARY KEY,
  platform TEXT NOT NULL DEFAULT 'android',
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS push_subscriptions (
  token TEXT NOT NULL,
  device_id TEXT NOT NULL,
  PRIMARY KEY (token, device_id),
  FOREIGN KEY (token) REFERENCES push_tokens(token) ON DELETE CASCADE,
  FOREIGN KEY (device_id) REFERENCES devices(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_push_subscriptions_device
ON push_subscriptions(device_id);

CREATE TABLE IF NOT EXISTS operation_cycles (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  device_id TEXT NOT NULL,
  type TEXT NOT NULL,
  status TEXT NOT NULL,
  started_at TEXT NOT NULL,
  ended_at TEXT,
  duration_sec INTEGER,
  FOREIGN KEY (device_id) REFERENCES devices(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_operation_cycles_device_type_time
ON operation_cycles(device_id, type, started_at);

CREATE INDEX IF NOT EXISTS idx_operation_cycles_active
ON operation_cycles(device_id, type, status);
`);

// Migracao segura para bancos criados nas versoes anteriores.
function ensureDeviceColumn(columnName, definition) {
  const cols = db.prepare('PRAGMA table_info(devices)').all();
  if (cols.some(c => c.name === columnName)) return;
  db.exec(`ALTER TABLE devices ADD COLUMN ${definition}`);
  console.log(`Banco atualizado: coluna ${columnName} adicionada.`);
}

ensureDeviceColumn(
  'offline_delay_sec',
  'offline_delay_sec INTEGER NOT NULL DEFAULT 120'
);

ensureDeviceColumn(
  'sensor_ok',
  'sensor_ok INTEGER NOT NULL DEFAULT 1'
);

ensureDeviceColumn(
  'defrost_on',
  'defrost_on INTEGER NOT NULL DEFAULT 0'
);

ensureDeviceColumn(
  'compressor_max_on_sec',
  'compressor_max_on_sec INTEGER NOT NULL DEFAULT 7200'
);

ensureDeviceColumn(
  'defrost_max_sec',
  'defrost_max_sec INTEGER NOT NULL DEFAULT 2700'
);

function ensureHistoryColumn(columnName, definition) {
  const cols = db.prepare('PRAGMA table_info(telemetry_history)').all();
  if (cols.some(c => c.name === columnName)) return;
  db.exec(`ALTER TABLE telemetry_history ADD COLUMN ${definition}`);
  console.log(`Banco atualizado: historico ganhou coluna ${columnName}.`);
}

ensureHistoryColumn(
  'defrost_on',
  'defrost_on INTEGER NOT NULL DEFAULT 0'
);


function inicializarFirebasePush() {
  try {
    let serviceAccount = null;

    // CLOUD: segredo salvo como variavel de ambiente na Railway.
    if (FIREBASE_SERVICE_ACCOUNT_JSON.trim()) {
      serviceAccount = JSON.parse(FIREBASE_SERVICE_ACCOUNT_JSON);
      console.log('Firebase Push: usando credencial segura da variavel de ambiente.');
    }
    // LOCAL: continua aceitando o arquivo existente no PC.
    else if (fs.existsSync(FIREBASE_SERVICE_ACCOUNT_FILE)) {
      serviceAccount = JSON.parse(
        fs.readFileSync(FIREBASE_SERVICE_ACCOUNT_FILE, 'utf8')
      );
      console.log('Firebase Push: usando credencial local por arquivo.');
    }

    if (!serviceAccount) {
      console.log('Firebase Push: credencial ainda nao configurada.');
      return;
    }

    initializeApp({
      credential: cert(serviceAccount),
      projectId: FIREBASE_PROJECT_ID
    });

    firebasePushReady = true;
    console.log('Firebase Push: PRONTO.');
  } catch (err) {
    firebasePushReady = false;
    console.error('Firebase Push: falha ao iniciar:', err.message);
  }
}

inicializarFirebasePush();

function tokensDoEquipamento(deviceId) {
  return db.prepare(`
    SELECT DISTINCT pt.token
    FROM push_tokens pt
    INNER JOIN push_subscriptions ps ON ps.token = pt.token
    WHERE ps.device_id = ?
  `).all(deviceId).map(r => r.token);
}

function removerTokenPush(token) {
  const tx = db.transaction(() => {
    db.prepare('DELETE FROM push_subscriptions WHERE token = ?').run(token);
    db.prepare('DELETE FROM push_tokens WHERE token = ?').run(token);
  });
  tx();
}

function erroTokenInvalido(err) {
  const code = String(err?.code || '');
  return (
    code.includes('registration-token-not-registered') ||
    code.includes('invalid-registration-token') ||
    code.includes('invalid-argument')
  );
}

async function enviarPushParaEquipamento(deviceId, title, body, data = {}) {
  if (!firebasePushReady) {
    console.log('PUSH ignorado: Firebase Admin ainda nao configurado.');
    return { sent: 0, failed: 0 };
  }

  const tokens = tokensDoEquipamento(deviceId);

  if (tokens.length === 0) {
    console.log(`PUSH: nenhum celular inscrito em ${deviceId}.`);
    return { sent: 0, failed: 0 };
  }

  let sent = 0;
  let failed = 0;

  for (const token of tokens) {
    try {
      await getMessaging().send({
        token,
        notification: {
          title,
          body
        },
        data: Object.fromEntries(
          Object.entries(data).map(([k, v]) => [k, String(v ?? '')])
        ),
        android: {
          priority: 'high',
          notification: {
            sound: 'default'
          }
        }
      });
      sent++;
    } catch (err) {
      failed++;
      console.error('PUSH falhou:', err.code || err.message);

      if (erroTokenInvalido(err)) {
        removerTokenPush(token);
        console.log('Token FCM invalido removido automaticamente.');
      }
    }
  }

  console.log(`PUSH ${deviceId}: ${sent} enviado(s), ${failed} falha(s).`);
  return { sent, failed };
}

function tituloTipoAlarme(type) {
  if (type === 'TEMP_HIGH') return 'temperatura alta';
  if (type === 'TEMP_LOW') return 'temperatura baixa';
  if (type === 'COMM_OFFLINE') return 'perda de comunicacao';
  if (type === 'SENSOR_TEMP_FAIL') return 'falha no sensor de temperatura';
  if (type === 'COMPRESSOR_LONG_ON') return 'compressor ligado por tempo excessivo';
  if (type === 'DEFROST_LONG') return 'degelo com duracao excessiva';
  return 'alarme';
}

function formatarDuracao(segundos) {
  const total = Math.max(0, Math.round(Number(segundos) || 0));
  if (total < 60) return `${total} s`;
  const min = Math.floor(total / 60);
  const sec = total % 60;
  if (min < 60) return sec ? `${min} min ${sec} s` : `${min} min`;
  const h = Math.floor(min / 60);
  const remMin = min % 60;
  return remMin ? `${h} h ${remMin} min` : `${h} h`;
}

function enviarPushAlarmeIniciado(device, alarm) {
  let body;

  if (alarm.type === 'COMM_OFFLINE') {
    body =
      `${device.name}: equipamento OFFLINE. ` +
      `Sem comunicacao ha ${formatarDuracao(alarm.start_value)}.`;
  } else if (alarm.type === 'SENSOR_TEMP_FAIL') {
    body =
      `${device.name}: FALHA NO SENSOR DE TEMPERATURA. ` +
      `Verifique o SB70 e a fiacao.`;
  } else if (alarm.type === 'COMPRESSOR_LONG_ON') {
    body =
      `${device.name}: COMPRESSOR LIGADO HA ` +
      `${formatarDuracao(alarm.start_value)}. ` +
      `Limite: ${formatarDuracao(alarm.threshold_value)}.`;
  } else if (alarm.type === 'DEFROST_LONG') {
    body =
      `${device.name}: DEGELO EM ANDAMENTO HA ` +
      `${formatarDuracao(alarm.start_value)}. ` +
      `Limite: ${formatarDuracao(alarm.threshold_value)}.`;
  } else {
    const textoTipo = tituloTipoAlarme(alarm.type);
    body =
      `${device.name}: ${textoTipo} ${Number(alarm.start_value).toFixed(1)} °C ` +
      `(limite ${Number(alarm.threshold_value).toFixed(1)} °C)`;
  }

  enviarPushParaEquipamento(
    device.id,
    '⚠ ELETRO MAIS',
    body,
    {
      event: 'alarm_started',
      deviceId: device.id,
      alarmId: alarm.id,
      alarmType: alarm.type,
      alarmStatus: alarm.status
    }
  ).catch(err => console.error('Erro push alarme:', err.message));
}

function enviarPushAlarmeNormalizado(device, alarm) {
  let body;

  if (alarm.type === 'COMM_OFFLINE') {
    body =
      `${device.name}: comunicacao restabelecida. ` +
      `Tempo sem dados: ${formatarDuracao(alarm.end_value ?? alarm.last_value)}.`;
  } else if (alarm.type === 'SENSOR_TEMP_FAIL') {
    body =
      `${device.name}: sensor de temperatura restabelecido.`;
  } else if (alarm.type === 'COMPRESSOR_LONG_ON') {
    body =
      `${device.name}: compressor desligado. ` +
      `Tempo total ligado: ${formatarDuracao(alarm.end_value ?? alarm.last_value)}.`;
  } else if (alarm.type === 'DEFROST_LONG') {
    body =
      `${device.name}: degelo finalizado. ` +
      `Duracao total: ${formatarDuracao(alarm.end_value ?? alarm.last_value)}.`;
  } else {
    body =
      `${device.name}: temperatura normalizada em ` +
      `${Number(alarm.end_value ?? alarm.last_value).toFixed(1)} °C`;
  }

  enviarPushParaEquipamento(
    device.id,
    '✅ ELETRO MAIS',
    body,
    {
      event: 'alarm_resolved',
      deviceId: device.id,
      alarmId: alarm.id,
      alarmType: alarm.type,
      alarmStatus: alarm.status
    }
  ).catch(err => console.error('Erro push normalizacao:', err.message));
}

function importLegacyDevicesIfNeeded() {
  const count = db.prepare('SELECT COUNT(*) AS n FROM devices').get().n;
  if (count > 0 || !fs.existsSync(LEGACY_DATA_FILE)) return;

  try {
    const legacy = JSON.parse(fs.readFileSync(LEGACY_DATA_FILE, 'utf8'));
    const insert = db.prepare(`
      INSERT OR IGNORE INTO devices (
        id, name, client, location,
        temperature, vibration, humidity, compressor_on, updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    `);

    const tx = db.transaction(() => {
      for (const d of Object.values(legacy)) {
        if (!d || !d.id) continue;
        insert.run(
          String(d.id),
          String(d.name || d.id),
          String(d.client || ''),
          String(d.location || ''),
          Number(d.temperature) || 0,
          Number(d.vibration) || 0,
          Number(d.humidity) || 0,
          d.compressorOn ? 1 : 0,
          d.updatedAt || null
        );
      }
    });

    tx();
    console.log('devices.json antigo importado para o SQLite.');
  } catch (err) {
    console.error('Nao foi possivel importar devices.json:', err.message);
  }
}

importLegacyDevicesIfNeeded();

function isoNow() {
  return new Date().toISOString();
}

function rowToPublicDevice(d) {
  const last = d.updated_at ? new Date(d.updated_at).getTime() : 0;
  const offlineDelaySec = Math.max(0, Number(d.offline_delay_sec) || 0);
  const onlineWindowMs = Math.max(30, offlineDelaySec || 120) * 1000;

  return {
    id: d.id,
    name: d.name,
    client: d.client,
    location: d.location,
    temperature: d.temperature,
    vibration: d.vibration,
    humidity: d.humidity,
    compressorOn: !!d.compressor_on,
    defrostOn: !!d.defrost_on,
    sensorOk: d.sensor_ok !== 0,
    updatedAt: d.updated_at,
    online: last > 0 && Date.now() - last < onlineWindowMs,
    tempMin: d.temp_min,
    tempMax: d.temp_max,
    alarmDelaySec: d.alarm_delay_sec,
    offlineDelaySec,
    compressorMaxOnSec: Math.max(0, Number(d.compressor_max_on_sec) || 0),
    defrostMaxSec: Math.max(0, Number(d.defrost_max_sec) || 0)
  };
}

function publicAlarm(a) {
  return {
    id: a.id,
    deviceId: a.device_id,
    type: a.type,
    status: a.status,
    startedAt: a.started_at,
    endedAt: a.ended_at,
    startValue: a.start_value,
    endValue: a.end_value,
    lastValue: a.last_value,
    thresholdValue: a.threshold_value
  };
}

function publicOperationCycle(c) {
  return {
    id: c.id,
    deviceId: c.device_id,
    type: c.type,
    status: c.status,
    startedAt: c.started_at,
    endedAt: c.ended_at,
    durationSec: c.duration_sec == null ? null : Number(c.duration_sec)
  };
}

function getDevice(id) {
  return db.prepare('SELECT * FROM devices WHERE id = ?').get(id);
}

function getActiveOperationCycle(deviceId, type) {
  return db.prepare(`
    SELECT * FROM operation_cycles
    WHERE device_id = ? AND type = ? AND status = 'ACTIVE'
    ORDER BY id DESC
    LIMIT 1
  `).get(deviceId, type);
}

function startOperationCycle(deviceId, type, now) {
  if (getActiveOperationCycle(deviceId, type)) return;

  const r = db.prepare(`
    INSERT INTO operation_cycles (
      device_id, type, status, started_at
    ) VALUES (?, ?, 'ACTIVE', ?)
  `).run(deviceId, type, now);

  const cycle = db.prepare(
    'SELECT * FROM operation_cycles WHERE id = ?'
  ).get(r.lastInsertRowid);

  broadcast({
    type: 'operation',
    action: 'started',
    data: publicOperationCycle(cycle)
  });

  console.log(`OPERACAO INICIADA: ${deviceId} ${type}`);
}

function stopOperationCycle(deviceId, type, now) {
  const active = getActiveOperationCycle(deviceId, type);
  if (!active) return;

  const startMs = new Date(active.started_at).getTime();
  const endMs = new Date(now).getTime();
  const durationSec =
    Number.isFinite(startMs) && Number.isFinite(endMs)
      ? Math.max(0, Math.round((endMs - startMs) / 1000))
      : 0;

  db.prepare(`
    UPDATE operation_cycles
    SET status = 'COMPLETED',
        ended_at = ?,
        duration_sec = ?
    WHERE id = ?
  `).run(now, durationSec, active.id);

  const cycle = db.prepare(
    'SELECT * FROM operation_cycles WHERE id = ?'
  ).get(active.id);

  broadcast({
    type: 'operation',
    action: 'completed',
    data: publicOperationCycle(cycle)
  });

  console.log(
    `OPERACAO FINALIZADA: ${deviceId} ${type} ${formatarDuracao(durationSec)}`
  );
}

function syncOperationCycle(deviceId, type, isOn, now) {
  const active = getActiveOperationCycle(deviceId, type);

  if (isOn && !active) {
    startOperationCycle(deviceId, type, now);
  } else if (!isOn && active) {
    stopOperationCycle(deviceId, type, now);
  }
}

function ensureDevice(id) {
  let d = getDevice(id);
  if (d) return d;

  db.prepare(`
    INSERT INTO devices (
      id, name, client, location,
      temperature, vibration, humidity, compressor_on, updated_at
    ) VALUES (?, ?, '', '', 0, 0, 0, 0, NULL)
  `).run(id, id);

  return getDevice(id);
}

function getActiveAlarm(deviceId, type) {
  return db.prepare(`
    SELECT * FROM alarms
    WHERE device_id = ? AND type = ? AND status = 'ACTIVE'
    ORDER BY id DESC
    LIMIT 1
  `).get(deviceId, type);
}

function createAlarm(device, type, value, threshold, now) {
  const r = db.prepare(`
    INSERT INTO alarms (
      device_id, type, status, started_at,
      start_value, last_value, threshold_value
    ) VALUES (?, ?, 'ACTIVE', ?, ?, ?, ?)
  `).run(device.id, type, now, value, value, threshold);

  const alarm = db.prepare('SELECT * FROM alarms WHERE id = ?').get(r.lastInsertRowid);

  broadcast({ type: 'alarm', action: 'started', data: publicAlarm(alarm) });
  enviarPushAlarmeIniciado(device, alarm);
  console.log(`ALARME INICIADO: ${device.id} ${type} valor=${value} limite=${threshold}`);
}

function resolveAlarm(alarm, value, now) {
  db.prepare(`
    UPDATE alarms
    SET status = 'RESOLVED',
        ended_at = ?,
        end_value = ?,
        last_value = ?
    WHERE id = ?
  `).run(now, value, value, alarm.id);

  const updated = db.prepare('SELECT * FROM alarms WHERE id = ?').get(alarm.id);

  broadcast({ type: 'alarm', action: 'resolved', data: publicAlarm(updated) });

  const device = getDevice(alarm.device_id);
  if (device) {
    enviarPushAlarmeNormalizado(device, updated);
  }

  console.log(`ALARME NORMALIZADO: ${alarm.device_id} ${alarm.type} valor=${value}`);
}

function processTemperatureAlarms(deviceId, temperature, now) {
  let device = getDevice(deviceId);
  const delayMs = Math.max(0, Number(device.alarm_delay_sec) || 0) * 1000;

  let activeHigh = getActiveAlarm(deviceId, 'TEMP_HIGH');

  if (device.temp_max !== null && temperature > device.temp_max) {
    if (activeHigh) {
      db.prepare('UPDATE alarms SET last_value = ? WHERE id = ?')
        .run(temperature, activeHigh.id);
    } else if (delayMs === 0) {
      createAlarm(device, 'TEMP_HIGH', temperature, device.temp_max, now);
      db.prepare('UPDATE devices SET high_since = NULL WHERE id = ?').run(deviceId);
    } else if (!device.high_since) {
      db.prepare('UPDATE devices SET high_since = ? WHERE id = ?').run(now, deviceId);
    } else if (Date.now() - new Date(device.high_since).getTime() >= delayMs) {
      createAlarm(device, 'TEMP_HIGH', temperature, device.temp_max, now);
      db.prepare('UPDATE devices SET high_since = NULL WHERE id = ?').run(deviceId);
    }
  } else {
    if (device.high_since !== null) {
      db.prepare('UPDATE devices SET high_since = NULL WHERE id = ?').run(deviceId);
    }
    if (activeHigh) resolveAlarm(activeHigh, temperature, now);
  }

  device = getDevice(deviceId);
  let activeLow = getActiveAlarm(deviceId, 'TEMP_LOW');

  if (device.temp_min !== null && temperature < device.temp_min) {
    if (activeLow) {
      db.prepare('UPDATE alarms SET last_value = ? WHERE id = ?')
        .run(temperature, activeLow.id);
    } else if (delayMs === 0) {
      createAlarm(device, 'TEMP_LOW', temperature, device.temp_min, now);
      db.prepare('UPDATE devices SET low_since = NULL WHERE id = ?').run(deviceId);
    } else if (!device.low_since) {
      db.prepare('UPDATE devices SET low_since = ? WHERE id = ?').run(now, deviceId);
    } else if (Date.now() - new Date(device.low_since).getTime() >= delayMs) {
      createAlarm(device, 'TEMP_LOW', temperature, device.temp_min, now);
      db.prepare('UPDATE devices SET low_since = NULL WHERE id = ?').run(deviceId);
    }
  } else {
    if (device.low_since !== null) {
      db.prepare('UPDATE devices SET low_since = NULL WHERE id = ?').run(deviceId);
    }
    if (activeLow) resolveAlarm(activeLow, temperature, now);
  }
}




function getLatestOperationCycle(deviceId, type) {
  return db.prepare(`
    SELECT * FROM operation_cycles
    WHERE device_id = ? AND type = ?
    ORDER BY id DESC
    LIMIT 1
  `).get(deviceId, type);
}

function processOneOperationDurationAlarm(
  deviceId,
  operationType,
  isOn,
  thresholdSec,
  alarmType,
  now
) {
  const device = getDevice(deviceId);
  if (!device) return;

  const threshold = Math.max(0, Math.floor(Number(thresholdSec) || 0));
  const activeAlarm = getActiveAlarm(deviceId, alarmType);

  // 0 desativa este alarme.
  if (threshold <= 0) {
    if (activeAlarm) {
      resolveAlarm(activeAlarm, Number(activeAlarm.last_value) || 0, now);
    }
    return;
  }

  if (isOn) {
    const cycle = getActiveOperationCycle(deviceId, operationType);
    if (!cycle) return;

    const startMs = new Date(cycle.started_at).getTime();
    const elapsedSec = Number.isFinite(startMs)
      ? Math.max(0, Math.round((Date.now() - startMs) / 1000))
      : 0;

    if (elapsedSec >= threshold) {
      if (activeAlarm) {
        db.prepare('UPDATE alarms SET last_value = ? WHERE id = ?')
          .run(elapsedSec, activeAlarm.id);
      } else {
        const crossedAt = Number.isFinite(startMs)
          ? new Date(startMs + threshold * 1000).toISOString()
          : now;
        createAlarm(device, alarmType, elapsedSec, threshold, crossedAt);
      }
    } else if (activeAlarm) {
      // Ex.: usuario aumentou o limite enquanto o ciclo ainda estava ativo.
      resolveAlarm(activeAlarm, elapsedSec, now);
    }

    return;
  }

  // Estado OFF: se existia um alarme, encerra usando a duracao real
  // do ultimo ciclo finalizado.
  if (activeAlarm) {
    const lastCycle = getLatestOperationCycle(deviceId, operationType);
    const finalSec =
      lastCycle?.duration_sec == null
        ? Number(activeAlarm.last_value) || 0
        : Number(lastCycle.duration_sec) || 0;

    resolveAlarm(activeAlarm, finalSec, now);
  }
}

function processOperationDurationAlarms(
  deviceId,
  compressorOn,
  defrostOn,
  now
) {
  const device = getDevice(deviceId);
  if (!device) return;

  processOneOperationDurationAlarm(
    deviceId,
    'COMPRESSOR',
    compressorOn,
    device.compressor_max_on_sec,
    'COMPRESSOR_LONG_ON',
    now
  );

  processOneOperationDurationAlarm(
    deviceId,
    'DEFROST',
    defrostOn,
    device.defrost_max_sec,
    'DEFROST_LONG',
    now
  );
}


function processTemperatureSensorAlarm(deviceId, sensorOk, now) {
  const device = getDevice(deviceId);
  if (!device) return;

  const active = getActiveAlarm(deviceId, 'SENSOR_TEMP_FAIL');

  if (!sensorOk) {
    if (active) {
      db.prepare('UPDATE alarms SET last_value = ? WHERE id = ?')
        .run(1, active.id);
    } else {
      // alarms exige valores numericos. Para falha de sensor usamos:
      // 1 = falha detectada / 0 = normalizado.
      createAlarm(device, 'SENSOR_TEMP_FAIL', 1, 0, now);
    }
  } else if (active) {
    resolveAlarm(active, 0, now);
  }
}

function processCommunicationRecovery(deviceBefore, now) {
  if (!deviceBefore?.updated_at) return;

  const alarm = getActiveAlarm(deviceBefore.id, 'COMM_OFFLINE');
  if (!alarm) return;

  const lastMs = new Date(deviceBefore.updated_at).getTime();
  const downtimeSec = Number.isFinite(lastMs)
    ? Math.max(0, Math.round((Date.now() - lastMs) / 1000))
    : 0;

  resolveAlarm(alarm, downtimeSec, now);
}

function processOfflineDevices() {
  const nowMs = Date.now();
  const rows = db.prepare(`
    SELECT * FROM devices
    WHERE updated_at IS NOT NULL
  `).all();

  for (const device of rows) {
    const delaySec = Math.max(0, Number(device.offline_delay_sec) || 0);
    if (delaySec <= 0) continue; // 0 desativa o alarme offline.

    const lastMs = new Date(device.updated_at).getTime();
    if (!Number.isFinite(lastMs)) continue;

    const elapsedSec = Math.max(0, Math.floor((nowMs - lastMs) / 1000));
    const active = getActiveAlarm(device.id, 'COMM_OFFLINE');

    if (elapsedSec >= delaySec) {
      if (active) {
        db.prepare('UPDATE alarms SET last_value = ? WHERE id = ?')
          .run(elapsedSec, active.id);
      } else {
        const crossedAt = new Date(lastMs + delaySec * 1000).toISOString();
        createAlarm(device, 'COMM_OFFLINE', elapsedSec, delaySec, crossedAt);
        console.log(
          `OFFLINE: ${device.id} sem dados ha ${formatarDuracao(elapsedSec)}.`
        );
      }
    }
  }
}


// ============================================================
// V11 - RELATORIO PDF
// ============================================================

function formatarDataRelatorio(value) {
  if (!value) return '-';
  const d = new Date(value);
  if (!Number.isFinite(d.getTime())) return '-';

  try {
    return new Intl.DateTimeFormat('pt-BR', {
      timeZone: REPORT_TIME_ZONE,
      day: '2-digit',
      month: '2-digit',
      year: 'numeric',
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit'
    }).format(d);
  } catch (_) {
    return d.toISOString().replace('T', ' ').replace('Z', ' UTC');
  }
}

function formatarDataArquivo(value = new Date()) {
  const d = value instanceof Date ? value : new Date(value);
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, '0');
  const day = String(d.getUTCDate()).padStart(2, '0');
  return `${y}-${m}-${day}`;
}

function formatarTemperatura(value) {
  if (value === null || value === undefined || !Number.isFinite(Number(value))) {
    return '-';
  }
  return `${Number(value).toFixed(1)} °C`;
}

function nomeAlarmeRelatorio(type) {
  const map = {
    TEMP_HIGH: 'Temperatura alta',
    TEMP_LOW: 'Temperatura baixa',
    COMM_OFFLINE: 'Perda de comunicacao',
    SENSOR_TEMP_FAIL: 'Falha no sensor de temperatura',
    COMPRESSOR_LONG_ON: 'Compressor ligado por tempo excessivo',
    DEFROST_LONG: 'Degelo com duracao excessiva'
  };
  return map[type] || type || 'Alarme';
}

function valorAlarmeRelatorio(alarm) {
  if (!alarm) return '-';

  if (
    alarm.type === 'COMM_OFFLINE' ||
    alarm.type === 'COMPRESSOR_LONG_ON' ||
    alarm.type === 'DEFROST_LONG'
  ) {
    return formatarDuracao(alarm.last_value);
  }

  if (alarm.type === 'SENSOR_TEMP_FAIL') {
    return alarm.status === 'ACTIVE' ? 'Falha' : 'Restabelecido';
  }

  return formatarTemperatura(alarm.last_value);
}

function bucketRelatorioMinutos(hours) {
  if (hours <= 24) return 5;
  if (hours <= 24 * 7) return 30;
  return 60;
}

function resumoOperacaoRelatorio(deviceId, type, since) {
  const completed = db.prepare(`
    SELECT
      COUNT(*) AS cycles,
      COALESCE(SUM(duration_sec), 0) AS totalSec,
      COALESCE(AVG(duration_sec), 0) AS avgSec,
      COALESCE(MAX(duration_sec), 0) AS maxSec
    FROM operation_cycles
    WHERE device_id = ?
      AND type = ?
      AND status = 'COMPLETED'
      AND started_at >= ?
  `).get(deviceId, type, since);

  const active = getActiveOperationCycle(deviceId, type);
  let activeSec = 0;

  if (active) {
    const startMs = new Date(active.started_at).getTime();
    if (Number.isFinite(startMs)) {
      activeSec = Math.max(0, Math.round((Date.now() - startMs) / 1000));
    }
  }

  return {
    cycles: Number(completed.cycles) || 0,
    totalSec: Number(completed.totalSec) || 0,
    avgSec: Math.round(Number(completed.avgSec) || 0),
    maxSec: Number(completed.maxSec) || 0,
    active: !!active,
    activeSince: active?.started_at || null,
    activeSec
  };
}

function garantirEspacoPdf(doc, alturaNecessaria = 80) {
  // Reserva area fixa para o rodape sem deixar o fluxo de texto invadi-la.
  if (doc.y + alturaNecessaria <= doc.page.height - 78) return;
  doc.addPage();
}

function tituloSecaoPdf(doc, titulo) {
  garantirEspacoPdf(doc, 55);
  doc.moveDown(0.45);
  doc
    .font('Helvetica-Bold')
    .fontSize(13)
    .fillColor('#17365D')
    .text(titulo);
  doc
    .moveTo(40, doc.y + 3)
    .lineTo(doc.page.width - 40, doc.y + 3)
    .lineWidth(0.8)
    .strokeColor('#B8C6D9')
    .stroke();
  doc.moveDown(0.55);
}

function linhaInfoPdf(doc, label, value, x = 40, width = 515) {
  garantirEspacoPdf(doc, 30);
  const y = doc.y;

  doc
    .font('Helvetica-Bold')
    .fontSize(9.5)
    .fillColor('#334155')
    .text(`${label}:`, x, y, { width: 145, continued: false });
  const labelBottom = doc.y;

  doc
    .font('Helvetica')
    .fontSize(9.5)
    .fillColor('#111827')
    .text(String(value ?? '-'), x + 150, y, { width: width - 150 });
  const valueBottom = doc.y;

  // Respeita a maior altura entre label e valor, inclusive quando quebram linha.
  doc.y = Math.max(labelBottom, valueBottom, y + 15) + 2;
}

function caixaMetricaPdf(doc, x, y, w, h, titulo, valor, subtitulo = '') {
  doc
    .roundedRect(x, y, w, h, 7)
    .fillAndStroke('#F4F7FB', '#D7E0EC');
  doc
    .font('Helvetica-Bold')
    .fontSize(8.5)
    .fillColor('#64748B')
    .text(titulo, x + 10, y + 9, { width: w - 20 });
  doc
    .font('Helvetica-Bold')
    .fontSize(15)
    .fillColor('#0F172A')
    .text(valor, x + 10, y + 25, { width: w - 20 });
  if (subtitulo) {
    doc
      .font('Helvetica')
      .fontSize(7.5)
      .fillColor('#64748B')
      .text(subtitulo, x + 10, y + h - 15, { width: w - 20 });
  }
}

function desenharGraficoTemperaturaPdf(doc, series, device) {
  garantirEspacoPdf(doc, 225);

  const x = 52;
  const y = doc.y + 10;
  const w = doc.page.width - 104;
  const h = 150;

  doc
    .roundedRect(x - 12, y - 10, w + 24, h + 35, 7)
    .fillAndStroke('#FFFFFF', '#D7E0EC');

  if (!Array.isArray(series) || series.length < 2) {
    doc
      .font('Helvetica')
      .fontSize(10)
      .fillColor('#64748B')
      .text('Sem dados suficientes para gerar o grafico.', x, y + 60, {
        width: w,
        align: 'center'
      });
    doc.y = y + h + 35;
    return;
  }

  const values = series
    .map(p => Number(p.temperature))
    .filter(Number.isFinite);

  if (values.length < 2) {
    doc.y = y + h + 35;
    return;
  }

  const extras = [];
  if (device.temp_min !== null && Number.isFinite(Number(device.temp_min))) {
    extras.push(Number(device.temp_min));
  }
  if (device.temp_max !== null && Number.isFinite(Number(device.temp_max))) {
    extras.push(Number(device.temp_max));
  }

  let min = Math.min(...values, ...extras);
  let max = Math.max(...values, ...extras);

  if (Math.abs(max - min) < 1) {
    min -= 1;
    max += 1;
  }

  const pad = Math.max(0.5, (max - min) * 0.10);
  min -= pad;
  max += pad;

  const mapY = value =>
    y + h - ((Number(value) - min) / (max - min)) * h;

  // grade
  doc.lineWidth(0.5).strokeColor('#E2E8F0');
  for (let i = 0; i <= 4; i++) {
    const gy = y + (h / 4) * i;
    doc.moveTo(x, gy).lineTo(x + w, gy).stroke();
    const tempLabel = max - ((max - min) / 4) * i;
    doc
      .font('Helvetica')
      .fontSize(7)
      .fillColor('#64748B')
      .text(`${tempLabel.toFixed(1)}°`, x - 32, gy - 4, {
        width: 28,
        align: 'right'
      });
  }

  function limiteLinha(value, label) {
    if (value === null || !Number.isFinite(Number(value))) return;
    const ly = mapY(Number(value));
    doc
      .save()
      .dash(4, { space: 3 })
      .lineWidth(0.8)
      .strokeColor('#94A3B8')
      .moveTo(x, ly)
      .lineTo(x + w, ly)
      .stroke()
      .undash()
      .restore();
    doc
      .font('Helvetica')
      .fontSize(7)
      .fillColor('#64748B')
      .text(`${label} ${Number(value).toFixed(1)}°C`, x + w - 85, ly - 10, {
        width: 82,
        align: 'right'
      });
  }

  limiteLinha(device.temp_min, 'Min');
  limiteLinha(device.temp_max, 'Max');

  // serie principal
  doc.lineWidth(1.5).strokeColor('#2563EB');
  const n = series.length;
  series.forEach((p, i) => {
    const px = x + (i / (n - 1)) * w;
    const py = mapY(Number(p.temperature));
    if (i === 0) doc.moveTo(px, py);
    else doc.lineTo(px, py);
  });
  doc.stroke();

  const first = series[0]?.createdAt;
  const last = series[series.length - 1]?.createdAt;
  doc
    .font('Helvetica')
    .fontSize(7)
    .fillColor('#64748B')
    .text(formatarDataRelatorio(first), x, y + h + 7, {
      width: w / 2,
      align: 'left'
    })
    .text(formatarDataRelatorio(last), x + w / 2, y + h + 7, {
      width: w / 2,
      align: 'right'
    });

  doc.y = y + h + 35;
}

function addRodapePaginasPdf(doc, geradoEm) {
  const range = doc.bufferedPageRange();
  for (let i = range.start; i < range.start + range.count; i++) {
    doc.switchToPage(i);

    // IMPORTANTE: manter o rodape dentro da area util da pagina.
    // Texto abaixo da margem inferior fazia o PDFKit criar paginas extras.
    const y = doc.page.height - 70;

    doc
      .moveTo(40, y - 5)
      .lineTo(doc.page.width - 40, y - 5)
      .lineWidth(0.5)
      .strokeColor('#CBD5E1')
      .stroke();

    doc
      .font('Helvetica')
      .fontSize(7.5)
      .fillColor('#64748B')
      .text(
        `ELETRO MAIS - Relatorio gerado em ${formatarDataRelatorio(geradoEm)}`,
        40,
        y,
        { width: 390, lineBreak: false }
      );

    doc
      .font('Helvetica')
      .fontSize(7.5)
      .fillColor('#64748B')
      .text(
        `Pagina ${i - range.start + 1} de ${range.count}`,
        doc.page.width - 150,
        y,
        { width: 110, align: 'right', lineBreak: false }
      );
  }
}

function gerarRelatorioPdf(req, res, device, hours) {
  const now = new Date();
  const nowIso = now.toISOString();
  const since = new Date(Date.now() - hours * 3600 * 1000).toISOString();
  const bucketMinutes = bucketRelatorioMinutos(hours);
  const bucketSeconds = bucketMinutes * 60;

  const stats = db.prepare(`
    SELECT
      COUNT(*) AS totalRecords,
      MIN(temperature) AS minTemperature,
      AVG(temperature) AS avgTemperature,
      MAX(temperature) AS maxTemperature,
      MIN(created_at) AS firstAt,
      MAX(created_at) AS lastAt
    FROM telemetry_history
    WHERE device_id = ? AND created_at >= ?
      AND temperature BETWEEN -60.0 AND 80.0
  `).get(device.id, since);

  const series = db.prepare(`
    SELECT
      CAST(CAST(strftime('%s', created_at) AS INTEGER) / ? AS INTEGER) AS bucket,
      MIN(created_at) AS createdAt,
      AVG(temperature) AS temperature,
      MIN(temperature) AS minTemperature,
      MAX(temperature) AS maxTemperature,
      COUNT(*) AS samples
    FROM telemetry_history
    WHERE device_id = ? AND created_at >= ?
      AND temperature BETWEEN -60.0 AND 80.0
    GROUP BY bucket
    ORDER BY bucket ASC
  `).all(bucketSeconds, device.id, since);

  const compressor = resumoOperacaoRelatorio(device.id, 'COMPRESSOR', since);
  const defrost = resumoOperacaoRelatorio(device.id, 'DEFROST', since);

  const alarmSummary = db.prepare(`
    SELECT
      type,
      COUNT(*) AS total,
      SUM(CASE WHEN status = 'ACTIVE' THEN 1 ELSE 0 END) AS active
    FROM alarms
    WHERE device_id = ? AND started_at >= ?
    GROUP BY type
    ORDER BY total DESC, type ASC
  `).all(device.id, since);

  const recentAlarms = db.prepare(`
    SELECT *
    FROM alarms
    WHERE device_id = ? AND started_at >= ?
    ORDER BY started_at DESC
    LIMIT 30
  `).all(device.id, since);

  const recentOperations = db.prepare(`
    SELECT *
    FROM operation_cycles
    WHERE device_id = ?
      AND (started_at >= ? OR status = 'ACTIVE')
    ORDER BY started_at DESC
    LIMIT 30
  `).all(device.id, since);

  const recentHistory = db.prepare(`
    SELECT temperature, compressor_on, defrost_on, created_at
    FROM telemetry_history
    WHERE device_id = ? AND created_at >= ?
      AND temperature BETWEEN -60.0 AND 80.0
    ORDER BY created_at DESC
    LIMIT 15
  `).all(device.id, since);

  const publicDevice = rowToPublicDevice(device);
  const filename =
    `eletro-mais_${String(device.id).replace(/[^a-zA-Z0-9_-]/g, '_')}` +
    `_${hours}h_${formatarDataArquivo(now)}.pdf`;

  res.status(200);
  res.setHeader('Content-Type', 'application/pdf');
  res.setHeader(
    'Content-Disposition',
    `attachment; filename="${filename}"`
  );
  res.setHeader('Cache-Control', 'no-store');

  const doc = new PDFDocument({
    size: 'A4',
    margins: { top: 38, bottom: 52, left: 40, right: 40 },
    bufferPages: true,
    info: {
      Title: `ELETRO MAIS - ${device.name}`,
      Author: 'ELETRO MAIS',
      Subject: 'Relatorio de monitoramento de refrigeracao'
    }
  });

  doc.pipe(res);

  // Cabecalho
  doc
    .font('Helvetica-Bold')
    .fontSize(24)
    .fillColor('#2563EB')
    .text('ELETRO MAIS');
  doc
    .font('Helvetica-Bold')
    .fontSize(15)
    .fillColor('#0F172A')
    .text('Relatorio de Monitoramento de Refrigeracao');
  doc
    .font('Helvetica')
    .fontSize(9)
    .fillColor('#64748B')
    .text(
      `Periodo: ${formatarDataRelatorio(since)} ate ${formatarDataRelatorio(nowIso)}`
    );

  doc.moveDown(0.7);

  // Identificacao
  tituloSecaoPdf(doc, 'Identificacao');
  linhaInfoPdf(doc, 'Cliente', device.client || '-');
  linhaInfoPdf(doc, 'Equipamento', device.name);
  linhaInfoPdf(doc, 'ID', device.id);
  linhaInfoPdf(doc, 'Local', device.location || '-');
  linhaInfoPdf(doc, 'Comunicacao', publicDevice.online ? 'ONLINE' : 'OFFLINE');
  linhaInfoPdf(doc, 'Sensor SB70', publicDevice.sensorOk ? 'OK' : 'FALHA');

  // Temperatura
  tituloSecaoPdf(doc, 'Temperatura');

  const metricY = doc.y;
  const gap = 8;
  const totalW = doc.page.width - 80;
  const boxW = (totalW - gap * 3) / 4;

  caixaMetricaPdf(
    doc, 40, metricY, boxW, 58,
    'ATUAL',
    publicDevice.sensorOk ? formatarTemperatura(device.temperature) : 'FALHA'
  );
  caixaMetricaPdf(
    doc, 40 + (boxW + gap), metricY, boxW, 58,
    'MINIMA',
    formatarTemperatura(stats.minTemperature)
  );
  caixaMetricaPdf(
    doc, 40 + (boxW + gap) * 2, metricY, boxW, 58,
    'MEDIA',
    formatarTemperatura(stats.avgTemperature)
  );
  caixaMetricaPdf(
    doc, 40 + (boxW + gap) * 3, metricY, boxW, 58,
    'MAXIMA',
    formatarTemperatura(stats.maxTemperature)
  );

  doc.y = metricY + 70;
  linhaInfoPdf(
    doc,
    'Amostras validas',
    Number(stats.totalRecords) || 0
  );
  linhaInfoPdf(
    doc,
    'Limites configurados',
    `${formatarTemperatura(device.temp_min)} / ${formatarTemperatura(device.temp_max)}`
  );

  desenharGraficoTemperaturaPdf(doc, series, device);

  // Operacao
  tituloSecaoPdf(doc, 'Operacao - Compressor e Degelo');
  const opY = doc.y;
  const opGap = 10;
  const opW = (doc.page.width - 80 - opGap) / 2;

  function operacaoBox(x, y, w, titulo, data, ligado) {
    const h = 106;
    doc.roundedRect(x, y, w, h, 7).fillAndStroke('#F8FAFC', '#D7E0EC');
    doc.font('Helvetica-Bold').fontSize(12).fillColor('#17365D')
      .text(titulo, x + 12, y + 10, { width: w - 24 });
    doc.font('Helvetica-Bold').fontSize(9.5)
      .fillColor(ligado ? '#15803D' : '#64748B')
      .text(ligado ? 'ATIVO' : 'DESLIGADO', x + 12, y + 29, { width: w - 24 });
    doc.font('Helvetica').fontSize(8.5).fillColor('#111827')
      .text(`Ciclos concluidos: ${data.cycles}`, x + 12, y + 48)
      .text(`Tempo total: ${formatarDuracao(data.totalSec)}`, x + 12, y + 62)
      .text(`Tempo medio: ${formatarDuracao(data.avgSec)}`, x + 12, y + 76)
      .text(`Maior ciclo: ${formatarDuracao(data.maxSec)}`, x + 12, y + 90);
  }

  operacaoBox(
    40, opY, opW, 'COMPRESSOR',
    compressor, publicDevice.compressorOn
  );
  operacaoBox(
    40 + opW + opGap, opY, opW, 'DEGELO',
    defrost, publicDevice.defrostOn
  );

  doc.y = opY + 118;
  linhaInfoPdf(
    doc,
    'Alarme compressor',
    device.compressor_max_on_sec > 0
      ? `Apos ${formatarDuracao(device.compressor_max_on_sec)} ligado`
      : 'Desativado'
  );
  linhaInfoPdf(
    doc,
    'Alarme degelo',
    device.defrost_max_sec > 0
      ? `Apos ${formatarDuracao(device.defrost_max_sec)} ativo`
      : 'Desativado'
  );

  // Alarmes
  tituloSecaoPdf(doc, 'Alarmes do periodo');
  if (!alarmSummary.length) {
    doc.font('Helvetica').fontSize(9.5).fillColor('#334155')
      .text('Nenhum alarme registrado no periodo.');
  } else {
    alarmSummary.forEach(row => {
      const label = nomeAlarmeRelatorio(row.type);
      linhaInfoPdf(
        doc,
        label,
        `${Number(row.total) || 0} ocorrencia(s)` +
          (Number(row.active) ? ` - ${Number(row.active)} ativa(s)` : '')
      );
    });
  }

  if (recentAlarms.length) {
    doc.moveDown(0.2);
    doc.font('Helvetica-Bold').fontSize(9.5).fillColor('#334155')
      .text('Ultimas ocorrencias:');
    doc.moveDown(0.25);

    recentAlarms.forEach(a => {
      garantirEspacoPdf(doc, 38);
      const status = a.status === 'ACTIVE' ? 'ATIVO' : 'NORMALIZADO';
      const fim = a.ended_at ? ` | Fim ${formatarDataRelatorio(a.ended_at)}` : '';
      doc
        .font('Helvetica-Bold')
        .fontSize(8.5)
        .fillColor('#111827')
        .text(`${nomeAlarmeRelatorio(a.type)} - ${status}`);
      doc
        .font('Helvetica')
        .fontSize(8)
        .fillColor('#475569')
        .text(
          `Inicio ${formatarDataRelatorio(a.started_at)}${fim} | Valor ${valorAlarmeRelatorio(a)}`
        );
      doc.moveDown(0.35);
    });
  }

  // Ciclos recentes
  tituloSecaoPdf(doc, 'Ciclos recentes');
  if (!recentOperations.length) {
    doc.font('Helvetica').fontSize(9.5).fillColor('#334155')
      .text('Nenhum ciclo de compressor ou degelo registrado no periodo.');
  } else {
    recentOperations.forEach(c => {
      garantirEspacoPdf(doc, 36);
      const nome = c.type === 'COMPRESSOR' ? 'Compressor' : 'Degelo';
      const fim = c.ended_at ? formatarDataRelatorio(c.ended_at) : 'EM ANDAMENTO';
      const duracao = c.duration_sec == null
        ? (c.status === 'ACTIVE'
            ? formatarDuracao(
                Math.max(
                  0,
                  Math.round((Date.now() - new Date(c.started_at).getTime()) / 1000)
                )
              )
            : '-')
        : formatarDuracao(c.duration_sec);

      doc.font('Helvetica-Bold').fontSize(8.5).fillColor('#111827')
        .text(`${nome} - ${c.status === 'ACTIVE' ? 'ATIVO' : 'CONCLUIDO'}`);
      doc.font('Helvetica').fontSize(8).fillColor('#475569')
        .text(
          `Inicio ${formatarDataRelatorio(c.started_at)} | Fim ${fim} | Duracao ${duracao}`
        );
      doc.moveDown(0.35);
    });
  }

  // Leituras recentes
  tituloSecaoPdf(doc, 'Leituras recentes');
  if (!recentHistory.length) {
    doc.font('Helvetica').fontSize(9.5).fillColor('#334155')
      .text('Nenhuma leitura valida registrada no periodo.');
  } else {
    recentHistory.forEach(r => {
      garantirEspacoPdf(doc, 22);
      doc
        .font('Helvetica')
        .fontSize(8.3)
        .fillColor('#334155')
        .text(
          `${formatarDataRelatorio(r.created_at)}  |  ` +
          `${formatarTemperatura(r.temperature)}  |  ` +
          `Comp ${r.compressor_on ? 'ON' : 'OFF'}  |  ` +
          `Degelo ${r.defrost_on ? 'ON' : 'OFF'}`
        );
    });
  }

  addRodapePaginasPdf(doc, nowIso);
  doc.end();
}


app.get('/health', (_, res) => {
  res.json({
    ok: true,
    service: 'ELETRO MAIS',
    version: 'V11.1',
    database: 'sqlite',
    firebasePush: firebasePushReady,
    time: isoNow()
  });
});

app.get('/api/devices', (_, res) => {
  res.json(db.prepare('SELECT * FROM devices ORDER BY name').all().map(rowToPublicDevice));
});

app.post('/api/devices/register', (req, res) => {
  const { id, name, client = '', location = '' } = req.body || {};
  if (!id || !name) {
    return res.status(400).json({ error: 'id e name sao obrigatorios' });
  }

  if (getDevice(id)) {
    db.prepare(`
      UPDATE devices SET name = ?, client = ?, location = ? WHERE id = ?
    `).run(name, client, location, id);
  } else {
    db.prepare(`
      INSERT INTO devices (
        id, name, client, location,
        temperature, vibration, humidity, compressor_on, updated_at
      ) VALUES (?, ?, ?, ?, 0, 0, 0, 0, NULL)
    `).run(id, name, client, location);
  }

  res.json({ ok: true, device: rowToPublicDevice(getDevice(id)) });
});

app.patch('/api/devices/:id/settings', (req, res) => {
  const id = req.params.id;
  const d = getDevice(id);
  if (!d) return res.status(404).json({ error: 'Equipamento nao encontrado' });

  const body = req.body || {};

  const tempMin = body.tempMin === undefined
    ? d.temp_min
    : (body.tempMin === null || body.tempMin === '' ? null : Number(body.tempMin));

  const tempMax = body.tempMax === undefined
    ? d.temp_max
    : (body.tempMax === null || body.tempMax === '' ? null : Number(body.tempMax));

  const alarmDelaySec = body.alarmDelaySec === undefined
    ? d.alarm_delay_sec
    : Math.max(0, Math.floor(Number(body.alarmDelaySec) || 0));

  const offlineDelaySec = body.offlineDelaySec === undefined
    ? Math.max(0, Number(d.offline_delay_sec) || 0)
    : Math.max(0, Math.floor(Number(body.offlineDelaySec) || 0));

  const compressorMaxOnSec = body.compressorMaxOnSec === undefined
    ? Math.max(0, Number(d.compressor_max_on_sec) || 0)
    : Math.max(0, Math.floor(Number(body.compressorMaxOnSec) || 0));

  const defrostMaxSec = body.defrostMaxSec === undefined
    ? Math.max(0, Number(d.defrost_max_sec) || 0)
    : Math.max(0, Math.floor(Number(body.defrostMaxSec) || 0));

  if (tempMin !== null && !Number.isFinite(tempMin)) {
    return res.status(400).json({ error: 'tempMin invalida' });
  }
  if (tempMax !== null && !Number.isFinite(tempMax)) {
    return res.status(400).json({ error: 'tempMax invalida' });
  }
  if (tempMin !== null && tempMax !== null && tempMin >= tempMax) {
    return res.status(400).json({ error: 'tempMin deve ser menor que tempMax' });
  }

  db.prepare(`
    UPDATE devices
    SET temp_min = ?, temp_max = ?, alarm_delay_sec = ?,
        offline_delay_sec = ?,
        compressor_max_on_sec = ?,
        defrost_max_sec = ?,
        high_since = NULL, low_since = NULL
    WHERE id = ?
  `).run(
    tempMin,
    tempMax,
    alarmDelaySec,
    offlineDelaySec,
    compressorMaxOnSec,
    defrostMaxSec,
    id
  );

  res.json({ ok: true, device: rowToPublicDevice(getDevice(id)) });
});


// ============================================================
// PUSH / FCM
// ============================================================

app.post('/api/push/register', (req, res) => {
  const token = String(req.body?.token || '').trim();
  const platform = String(req.body?.platform || 'android').trim() || 'android';
  const rawDeviceIds = Array.isArray(req.body?.deviceIds)
    ? req.body.deviceIds
    : [];

  if (token.length < 20) {
    return res.status(400).json({ error: 'Token FCM invalido' });
  }

  const deviceIds = [...new Set(
    rawDeviceIds
      .map(v => String(v || '').trim())
      .filter(Boolean)
      .filter(id => !!getDevice(id))
  )];

  const now = isoNow();

  const tx = db.transaction(() => {
    db.prepare(`
      INSERT INTO push_tokens (token, platform, updated_at)
      VALUES (?, ?, ?)
      ON CONFLICT(token) DO UPDATE SET
        platform = excluded.platform,
        updated_at = excluded.updated_at
    `).run(token, platform, now);

    db.prepare('DELETE FROM push_subscriptions WHERE token = ?').run(token);

    const insertSubscription = db.prepare(`
      INSERT OR IGNORE INTO push_subscriptions (token, device_id)
      VALUES (?, ?)
    `);

    for (const deviceId of deviceIds) {
      insertSubscription.run(token, deviceId);
    }
  });

  tx();

  res.json({
    ok: true,
    registered: true,
    subscriptions: deviceIds
  });
});

app.get('/api/push/status', (_, res) => {
  const tokens = db.prepare('SELECT COUNT(*) AS n FROM push_tokens').get().n;
  const subscriptions =
    db.prepare('SELECT COUNT(*) AS n FROM push_subscriptions').get().n;

  res.json({
    ok: true,
    firebasePush: firebasePushReady,
    registeredPhones: Number(tokens) || 0,
    subscriptions: Number(subscriptions) || 0
  });
});

app.post('/api/push/test', async (req, res) => {
  const deviceId = String(req.body?.deviceId || '').trim();
  const device = getDevice(deviceId);

  if (!device) {
    return res.status(404).json({ error: 'Equipamento nao encontrado' });
  }

  const result = await enviarPushParaEquipamento(
    deviceId,
    '🔔 ELETRO MAIS',
    `Teste automatico de notificacao - ${device.name}`,
    {
      event: 'push_test',
      deviceId
    }
  );

  res.json({
    ok: true,
    firebasePush: firebasePushReady,
    ...result
  });
});

app.post('/api/telemetry', (req, res) => {
  const {
    id,
    temperature,
    vibration = 0,
    humidity = 0,
    compressorOn = false,
    defrostOn = false,
    sensorOk = true
  } = req.body || {};

  if (!id) {
    return res.status(400).json({ error: 'Envie id' });
  }

  const sensorHealthy = sensorOk !== false;

  if (
    sensorHealthy &&
    (typeof temperature !== 'number' || !Number.isFinite(temperature))
  ) {
    return res.status(400).json({
      error: 'Com sensorOk=true, envie temperature numerica'
    });
  }

  ensureDevice(String(id));
  const deviceBefore = getDevice(String(id));

  const now = isoNow();
  const vib = Number(vibration) || 0;
  const hum = Number(humidity) || 0;
  const comp = compressorOn ? 1 : 0;
  const defrost = defrostOn ? 1 : 0;

  const tx = db.transaction(() => {
    if (sensorHealthy) {
      db.prepare(`
        UPDATE devices
        SET temperature = ?,
            vibration = ?,
            humidity = ?,
            compressor_on = ?,
            defrost_on = ?,
            sensor_ok = 1,
            updated_at = ?
        WHERE id = ?
      `).run(temperature, vib, hum, comp, defrost, now, id);

      // So gravamos historico de temperatura quando existe uma leitura valida.
      db.prepare(`
        INSERT INTO telemetry_history (
          device_id, temperature, vibration, humidity,
          compressor_on, defrost_on, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
      `).run(id, temperature, vib, hum, comp, defrost, now);
    } else {
      // Mantem a ultima temperatura valida, mas atualiza a comunicacao.
      // Assim falha do SB70 nao vira alarme OFFLINE.
      db.prepare(`
        UPDATE devices
        SET vibration = ?,
            humidity = ?,
            compressor_on = ?,
            defrost_on = ?,
            sensor_ok = 0,
            updated_at = ?
        WHERE id = ?
      `).run(vib, hum, comp, defrost, now, id);
    }
  });

  tx();

  // Mantem ciclos de compressor e degelo sincronizados com o estado recebido.
  // Se o servidor reiniciar durante um ciclo ativo, ele reabre o ciclo
  // na primeira telemetria observada como ON.
  syncOperationCycle(String(id), 'COMPRESSOR', !!compressorOn, now);
  syncOperationCycle(String(id), 'DEFROST', !!defrostOn, now);

  // Notifica somente se o tempo de operacao ultrapassar o limite.
  // Liga/desliga normal continua SEM push.
  processOperationDurationAlarms(
    String(id),
    !!compressorOn,
    !!defrostOn,
    now
  );

  processCommunicationRecovery(deviceBefore, now);
  processTemperatureSensorAlarm(String(id), sensorHealthy, now);

  // Alarmes de temperatura so fazem sentido com sensor valido.
  if (sensorHealthy) {
    processTemperatureAlarms(String(id), temperature, now);
  } else {
    // Cancela temporizadores pendentes de alta/baixa enquanto o sensor falhou.
    db.prepare(`
      UPDATE devices
      SET high_since = NULL, low_since = NULL
      WHERE id = ?
    `).run(String(id));
  }

  const payload = {
    type: 'telemetry',
    data: rowToPublicDevice(getDevice(String(id)))
  };

  broadcast(payload);

  res.json({
    ok: true,
    sensorOk: sensorHealthy,
    received: payload.data
  });
});

app.get('/api/history/:id/summary', (req, res) => {
  const id = req.params.id;
  if (!getDevice(id)) {
    return res.status(404).json({ error: 'Equipamento nao encontrado' });
  }

  const hours = Math.min(24 * 365, Math.max(1, Number(req.query.hours) || 24));
  const bucketMinutes = Math.min(
    24 * 60,
    Math.max(1, Math.floor(Number(req.query.bucketMinutes) || 5))
  );
  const recentLimit = Math.min(
    500,
    Math.max(1, Math.floor(Number(req.query.recentLimit) || 100))
  );

  const since = new Date(Date.now() - hours * 3600 * 1000).toISOString();
  const bucketSeconds = bucketMinutes * 60;

  const stats = db.prepare(`
    SELECT
      COUNT(*) AS totalRecords,
      MIN(temperature) AS minTemperature,
      AVG(temperature) AS avgTemperature,
      MAX(temperature) AS maxTemperature,
      MIN(created_at) AS firstAt,
      MAX(created_at) AS lastAt
    FROM telemetry_history
    WHERE device_id = ? AND created_at >= ?
  `).get(id, since);

  const series = db.prepare(`
    SELECT
      CAST(CAST(strftime('%s', created_at) AS INTEGER) / ? AS INTEGER) AS bucket,
      MIN(created_at) AS createdAt,
      AVG(temperature) AS temperature,
      MIN(temperature) AS minTemperature,
      MAX(temperature) AS maxTemperature,
      COUNT(*) AS samples
    FROM telemetry_history
    WHERE device_id = ? AND created_at >= ?
    GROUP BY bucket
    ORDER BY bucket ASC
  `).all(bucketSeconds, id, since);

  const recent = db.prepare(`
    SELECT
      id,
      device_id AS deviceId,
      temperature,
      vibration,
      humidity,
      compressor_on AS compressorOn,
      defrost_on AS defrostOn,
      created_at AS createdAt
    FROM telemetry_history
    WHERE device_id = ? AND created_at >= ?
    ORDER BY created_at DESC
    LIMIT ?
  `).all(id, since, recentLimit);

  res.json({
    deviceId: id,
    hours,
    bucketMinutes,
    stats: {
      totalRecords: Number(stats.totalRecords) || 0,
      minTemperature: stats.minTemperature == null ? null : Number(stats.minTemperature),
      avgTemperature: stats.avgTemperature == null ? null : Number(stats.avgTemperature),
      maxTemperature: stats.maxTemperature == null ? null : Number(stats.maxTemperature),
      firstAt: stats.firstAt,
      lastAt: stats.lastAt
    },
    series: series.map(r => ({
      createdAt: r.createdAt,
      temperature: Number(r.temperature),
      minTemperature: Number(r.minTemperature),
      maxTemperature: Number(r.maxTemperature),
      samples: Number(r.samples)
    })),
    recent: recent.map(r => ({
      ...r,
      compressorOn: !!r.compressorOn,
      defrostOn: !!r.defrostOn
    }))
  });
});

app.get('/api/history/:id', (req, res) => {
  const id = req.params.id;
  if (!getDevice(id)) {
    return res.status(404).json({ error: 'Equipamento nao encontrado' });
  }

  const hours = Math.min(24 * 365, Math.max(1, Number(req.query.hours) || 24));
  const limit = Math.min(20000, Math.max(1, Number(req.query.limit) || 5000));
  const since = new Date(Date.now() - hours * 3600 * 1000).toISOString();

  const rows = db.prepare(`
    SELECT
      id,
      device_id AS deviceId,
      temperature,
      vibration,
      humidity,
      compressor_on AS compressorOn,
      defrost_on AS defrostOn,
      created_at AS createdAt
    FROM telemetry_history
    WHERE device_id = ? AND created_at >= ?
    ORDER BY created_at ASC
    LIMIT ?
  `).all(id, since, limit);

  res.json(rows.map(r => ({
    ...r,
    compressorOn: !!r.compressorOn,
    defrostOn: !!r.defrostOn
  })));
});

// ============================================================
// COMPRESSOR + DEGELO - CICLOS / HISTORICO OPERACIONAL
// ============================================================

app.get('/api/operations/:id', (req, res) => {
  const id = req.params.id;
  if (!getDevice(id)) {
    return res.status(404).json({ error: 'Equipamento nao encontrado' });
  }

  const hours = Math.min(24 * 365, Math.max(1, Number(req.query.hours) || 24));
  const limit = Math.min(5000, Math.max(1, Math.floor(Number(req.query.limit) || 500)));
  const type = String(req.query.type || 'ALL').toUpperCase();
  const since = new Date(Date.now() - hours * 3600 * 1000).toISOString();

  let rows;

  if (type === 'COMPRESSOR' || type === 'DEFROST') {
    rows = db.prepare(`
      SELECT * FROM operation_cycles
      WHERE device_id = ? AND type = ?
        AND (started_at >= ? OR status = 'ACTIVE')
      ORDER BY started_at DESC
      LIMIT ?
    `).all(id, type, since, limit);
  } else {
    rows = db.prepare(`
      SELECT * FROM operation_cycles
      WHERE device_id = ?
        AND (started_at >= ? OR status = 'ACTIVE')
      ORDER BY started_at DESC
      LIMIT ?
    `).all(id, since, limit);
  }

  res.json(rows.map(publicOperationCycle));
});

app.get('/api/operations/:id/summary', (req, res) => {
  const id = req.params.id;
  if (!getDevice(id)) {
    return res.status(404).json({ error: 'Equipamento nao encontrado' });
  }

  const hours = Math.min(24 * 365, Math.max(1, Number(req.query.hours) || 24));
  const since = new Date(Date.now() - hours * 3600 * 1000).toISOString();

  function summarize(type) {
    const completed = db.prepare(`
      SELECT
        COUNT(*) AS cycles,
        COALESCE(SUM(duration_sec), 0) AS totalSec,
        COALESCE(AVG(duration_sec), 0) AS avgSec,
        COALESCE(MAX(duration_sec), 0) AS maxSec
      FROM operation_cycles
      WHERE device_id = ?
        AND type = ?
        AND status = 'COMPLETED'
        AND started_at >= ?
    `).get(id, type, since);

    const active = getActiveOperationCycle(id, type);
    let activeSec = 0;

    if (active) {
      const startMs = new Date(active.started_at).getTime();
      if (Number.isFinite(startMs)) {
        activeSec = Math.max(0, Math.round((Date.now() - startMs) / 1000));
      }
    }

    return {
      cycles: Number(completed.cycles) || 0,
      totalSec: Number(completed.totalSec) || 0,
      avgSec: Math.round(Number(completed.avgSec) || 0),
      maxSec: Number(completed.maxSec) || 0,
      active: !!active,
      activeSince: active?.started_at || null,
      activeSec
    };
  }

  res.json({
    deviceId: id,
    hours,
    compressor: summarize('COMPRESSOR'),
    defrost: summarize('DEFROST')
  });
});

app.get('/api/alarms/:id', (req, res) => {
  const id = req.params.id;
  if (!getDevice(id)) {
    return res.status(404).json({ error: 'Equipamento nao encontrado' });
  }

  const status = String(req.query.status || 'all').toUpperCase();
  const limit = Math.min(1000, Math.max(1, Number(req.query.limit) || 200));

  let rows;

  if (status === 'ACTIVE' || status === 'RESOLVED') {
    rows = db.prepare(`
      SELECT * FROM alarms
      WHERE device_id = ? AND status = ?
      ORDER BY started_at DESC
      LIMIT ?
    `).all(id, status, limit);
  } else {
    rows = db.prepare(`
      SELECT * FROM alarms
      WHERE device_id = ?
      ORDER BY started_at DESC
      LIMIT ?
    `).all(id, limit);
  }

  res.json(rows.map(publicAlarm));
});


// ============================================================
// V11 - PDF DE HISTORICO / MONITORAMENTO
// ============================================================

app.get('/api/reports/:id/pdf', (req, res) => {
  const id = req.params.id;
  const device = getDevice(id);

  if (!device) {
    return res.status(404).json({ error: 'Equipamento nao encontrado' });
  }

  const hours = Math.min(
    24 * 365,
    Math.max(1, Math.floor(Number(req.query.hours) || 24))
  );

  try {
    gerarRelatorioPdf(req, res, device, hours);
  } catch (err) {
    console.error('Erro ao gerar relatorio PDF:', err);

    if (!res.headersSent) {
      return res.status(500).json({
        error: 'Nao foi possivel gerar o relatorio PDF'
      });
    }

    try {
      res.end();
    } catch (_) {}
  }
});


const server = http.createServer(app);
const wss = new WebSocket.Server({ noServer: true });

server.on('upgrade', (request, socket, head) => {
  if (request.url !== '/ws') {
    socket.destroy();
    return;
  }

  wss.handleUpgrade(request, socket, head, (ws) => {
    wss.emit('connection', ws, request);
  });
});

function broadcast(obj) {
  const data = JSON.stringify(obj);
  wss.clients.forEach((client) => {
    if (client.readyState === WebSocket.OPEN) client.send(data);
  });
}

wss.on('connection', (ws) => {
  ws.send(JSON.stringify({
    type: 'hello',
    data: { service: 'ELETRO MAIS', connected: true }
  }));
});

setInterval(processOfflineDevices, OFFLINE_CHECK_INTERVAL_MS);
setTimeout(processOfflineDevices, 3000);

server.listen(PORT, '0.0.0.0', () => {
  console.log('========================================');
  console.log('ELETRO MAIS V11.1 CLOUD - RELATORIO PDF CORRIGIDO + HISTORICO + ALARMES');
  console.log(`Servidor:  http://0.0.0.0:${PORT}`);
  console.log(`WebSocket: ws://0.0.0.0:${PORT}/ws`);
  console.log(`Banco:     ${DB_FILE}`);
  console.log(`Modo:      ${process.env.RAILWAY_ENVIRONMENT ? 'CLOUD / RAILWAY' : 'LOCAL'}`);
  console.log(`Firebase:  ${firebasePushReady ? 'PRONTO' : 'AGUARDANDO CREDENCIAL'}`);
  console.log('Offline:   monitoramento ativo (padrao 120 s)');
  console.log('Compressor: alarme por tempo excessivo (padrao 2 h)');
  console.log('Degelo:     alarme por duracao excessiva (padrao 45 min)');
  console.log('PDF:        relatorio de historico ativo em /api/reports/:id/pdf');
  console.log('========================================');
});
