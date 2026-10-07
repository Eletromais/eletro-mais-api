const express = require('express');
const cors = require('cors');
const fs = require('fs');
const path = require('path');
const http = require('http');
const WebSocket = require('ws');
const Database = require('better-sqlite3');
const admin = require('firebase-admin');

// A Railway injeta process.env.PORT. Fallback para 8080.
const PORT = process.env.PORT || 8080;

// 1. Definição do diretório seguro para produção e desenvolvimento
const baseDir = process.env.NODE_ENV === 'production' ? '/tmp' : __dirname;

if (!fs.existsSync(baseDir)) {
  fs.mkdirSync(baseDir, { recursive: true });
}

const dbPath = path.join(baseDir, 'database.db');
const DATA_FILE = path.join(baseDir, 'devices.json');
const PUSH_TOKENS_FILE = path.join(baseDir, 'push_tokens.json');

// 2. Inicialização do Firebase Admin SDK para envio de Push Notifications
try {
  if (process.env.FIREBASE_SERVICE_ACCOUNT) {
    const serviceAccount = JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT);
    admin.initializeApp({
      credential: admin.credential.cert(serviceAccount)
    });
    console.log('[FCM] Firebase Admin SDK inicializado via Variável de Ambiente.');
  } else if (fs.existsSync(path.join(__dirname, 'serviceAccountKey.json'))) {
    const serviceAccount = require('./serviceAccountKey.json');
    admin.initializeApp({
      credential: admin.credential.cert(serviceAccount)
    });
    console.log('[FCM] Firebase Admin SDK inicializado via serviceAccountKey.json.');
  } else {
    console.warn('[FCM] Nenhuma credencial do Firebase encontrada. As notificações Push ficarão desativadas até configurar o Firebase Admin.');
  }
} catch (error) {
  console.error('[FCM] Erro ao inicializar o Firebase Admin SDK:', error);
}

// 3. Inicialização do SQLite e tabelas
let db;
try {
  db = new Database(dbPath);
  console.log(`Banco de dados SQLite carregado em: ${dbPath}`);

  // Tabela para guardar registros de alarmes
  db.exec(`
    CREATE TABLE IF NOT EXISTS alarms (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      deviceId TEXT NOT NULL,
      type TEXT NOT NULL,
      status TEXT DEFAULT 'ACTIVE',
      startedAt TEXT NOT NULL,
      endedAt TEXT,
      startValue REAL DEFAULT 0,
      endValue REAL,
      lastValue REAL DEFAULT 0,
      thresholdValue REAL DEFAULT 0
    )
  `);

  // Tabela para guardar histórico de telemetria
  db.exec(`
    CREATE TABLE IF NOT EXISTS telemetry_history (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      deviceId TEXT NOT NULL,
      temperature REAL DEFAULT 0,
      vibration REAL DEFAULT 0,
      humidity INTEGER DEFAULT 0,
      compressorOn INTEGER DEFAULT 0,
      defrostOn INTEGER DEFAULT 0,
      createdAt TEXT NOT NULL
    )
  `);
} catch (error) {
  console.error('Erro ao inicializar o banco SQLite:', error);
}

// 4. Gestão e Persistência de Dados (Equipamentos e Push Tokens)
function loadJSON(filePath) {
  try {
    if (!fs.existsSync(filePath)) {
      fs.writeFileSync(filePath, JSON.stringify({}), 'utf8');
      return {};
    }
    const data = fs.readFileSync(filePath, 'utf8');
    return JSON.parse(data || '{}');
  } catch (err) {
    console.error(`Erro ao carregar ${filePath}:`, err);
    return {};
  }
}

function saveJSON(filePath, data) {
  try {
    fs.writeFileSync(filePath, JSON.stringify(data, null, 2), 'utf8');
  } catch (err) {
    console.error(`Erro ao salvar ${filePath}:`, err);
  }
}

let devices = loadJSON(DATA_FILE);
let pushTokens = loadJSON(PUSH_TOKENS_FILE); // { token: { platform: 'android', deviceIds: ['EM-CF-0001'] } }

// Envio de Push Notification via Firebase Messaging
async function sendPushNotification(title, body, targetDeviceId) {
  if (!admin.apps.length) return;

  const recipientTokens = [];

  for (const [token, data] of Object.entries(pushTokens)) {
    if (!data.deviceIds || data.deviceIds.length === 0 || data.deviceIds.includes(targetDeviceId)) {
      recipientTokens.push(token);
    }
  }

  if (recipientTokens.length === 0) {
    console.log(`[FCM] Nenhum token registrado para o dispositivo: ${targetDeviceId}`);
    return;
  }

  const message = {
    notification: {
      title,
      body
    },
    data: {
      deviceId: targetDeviceId || '',
      click_action: 'FLUTTER_NOTIFICATION_CLICK'
    },
    tokens: recipientTokens
  };

  try {
    const response = await admin.messaging().sendEachForMulticast(message);
    console.log(`[FCM] Notificação Push enviada com sucesso: ${response.successCount} entregues, ${response.failureCount} falhas.`);
  } catch (err) {
    console.error('[FCM] Erro ao disparar mensagem Multicast:', err);
  }
}

// Emissão e Notificação de Alarmes
function triggerAlarm(deviceId, type, startValue, thresholdValue, title, messageText) {
  const timestamp = new Date().toISOString();
  if (db) {
    try {
      const active = db.prepare(
        'SELECT * FROM alarms WHERE deviceId = ? AND type = ? AND status = "ACTIVE"'
      ).get(deviceId, type);

      if (!active) {
        const stmt = db.prepare(`
          INSERT INTO alarms (deviceId, type, status, startedAt, startValue, lastValue, thresholdValue)
          VALUES (?, ?, 'ACTIVE', ?, ?, ?, ?)
        `);
        stmt.run(deviceId, type, timestamp, startValue, startValue, thresholdValue);

        // Notifica via WebSocket (App Aberto)
        broadcast({
          type: 'alarm',
          data: { deviceId, type, status: 'ACTIVE', startedAt: timestamp }
        });

        // Notifica via Push Notification (Barra do Telemóvel / Background)
        sendPushNotification(title, messageText, deviceId);
      }
    } catch (err) {
      console.error('Erro ao processar alarme:', err);
    }
  }
}

// 5. Servidor Express e WebSockets
const app = express();
app.use(cors());
app.use(express.json());

const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

wss.on('connection', (ws) => {
  console.log('Novo cliente WebSocket conectado.');
  ws.on('message', (message) => {
    console.log(`Mensagem recebida via WS: ${message}`);
  });
  ws.on('close', () => {
    console.log('Cliente WebSocket desconectado.');
  });
});

function broadcast(data) {
  wss.clients.forEach((client) => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(JSON.stringify(data));
    }
  });
}

// 6. Rotas da API REST

// Dashboard / Resumo de status
app.get('/api/summary', (req, res) => {
  const deviceList = Object.values(devices);
  let activeAlarmsCount = 0;

  if (db) {
    try {
      const row = db.prepare('SELECT COUNT(*) as count FROM alarms WHERE status = "ACTIVE"').get();
      activeAlarmsCount = row ? row.count : 0;
    } catch (e) {
      console.error('Erro ao ler resumo de alarmes:', e);
    }
  }

  res.json({
    ok: true,
    clientsCount: 1,
    equipmentsCount: deviceList.length,
    onlineCount: deviceList.filter(d => d.online).length,
    alarmsCount: activeAlarmsCount
  });
});

// Retorna lista direta de equipamentos para o Flutter
app.get('/api/devices', (req, res) => {
  res.json(Object.values(devices));
});

// Configurações de limites de alarme
app.patch('/api/devices/:id/settings', (req, res) => {
  const { id } = req.params;
  const { tempMin, tempMax, alarmDelaySec, offlineDelaySec } = req.body;

  if (!devices[id]) {
    return res.status(404).json({ ok: false, message: 'Dispositivo não encontrado.' });
  }

  devices[id] = {
    ...devices[id],
    tempMin: tempMin !== undefined ? tempMin : devices[id].tempMin,
    tempMax: tempMax !== undefined ? tempMax : devices[id].tempMax,
    alarmDelaySec: alarmDelaySec !== undefined ? alarmDelaySec : devices[id].alarmDelaySec,
    offlineDelaySec: offlineDelaySec !== undefined ? offlineDelaySec : devices[id].offlineDelaySec,
  };

  saveJSON(DATA_FILE, devices);
  res.json({ ok: true, device: devices[id] });
});

// Cadastro de novo equipamento
app.post('/api/devices/register', (req, res) => {
  const { id, name, client, location } = req.body;

  if (!id || !name) {
    return res.status(400).json({ ok: false, message: 'ID e nome do equipamento são obrigatórios.' });
  }

  devices[id] = {
    id,
    name,
    client: client || '',
    location: location || '',
    temperature: 0,
    vibration: 0,
    humidity: 0,
    compressorOn: false,
    defrostOn: false,
    sensorOk: true,
    online: true,
    updatedAt: new Date().toISOString(),
    tempMin: null,
    tempMax: null,
    alarmDelaySec: 0,
    offlineDelaySec: 120
  };

  saveJSON(DATA_FILE, devices);
  res.json({ ok: true, device: devices[id] });
});

// Recepção de Telemetria (ESP32 etc.)
app.post('/api/telemetry', (req, res) => {
  const { id, temperature, humidity, vibration, compressorOn, defrostOn, sensorOk } = req.body;

  if (!id) {
    return res.status(400).json({ ok: false, message: 'ID do dispositivo é obrigatório.' });
  }

  const timestamp = new Date().toISOString();
  const existing = devices[id] || { id, name: id, client: '', location: '' };

  const updatedDevice = {
    ...existing,
    id,
    temperature: temperature ?? existing.temperature ?? 0,
    humidity: humidity ?? existing.humidity ?? 0,
    vibration: vibration ?? existing.vibration ?? 0,
    compressorOn: compressorOn ?? existing.compressorOn ?? false,
    defrostOn: defrostOn ?? existing.defrostOn ?? false,
    sensorOk: sensorOk ?? existing.sensorOk ?? true,
    online: true,
    updatedAt: timestamp
  };

  devices[id] = updatedDevice;
  saveJSON(DATA_FILE, devices);

  // Registro no Histórico do SQLite
  if (db) {
    try {
      db.prepare(`
        INSERT INTO telemetry_history (deviceId, temperature, vibration, humidity, compressorOn, defrostOn, createdAt)
        VALUES (?, ?, ?, ?, ?, ?, ?)
      `).run(
        id,
        updatedDevice.temperature,
        updatedDevice.vibration,
        updatedDevice.humidity,
        updatedDevice.compressorOn ? 1 : 0,
        updatedDevice.defrostOn
