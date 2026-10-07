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
const CLIENTS_FILE = path.join(baseDir, 'clients.json');
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
    console.warn('[FCM] Nenhuma credencial do Firebase encontrada. As notificações Push ficarão desativadas.');
  }
} catch (error) {
  console.error('[FCM] Erro ao inicializar o Firebase Admin SDK:', error);
}

// 3. Inicialização do SQLite e tabelas
let db;
try {
  db = new Database(dbPath);
  console.log(`Banco de dados SQLite carregado em: ${dbPath}`);

  // Tabela para guardar registros de clientes
  db.exec(`
    CREATE TABLE IF NOT EXISTS clients (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      name TEXT NOT NULL,
      email TEXT,
      phone TEXT,
      createdAt TEXT NOT NULL
    )
  `);

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

// 4. Gestão e Persistência de Dados
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
let clients = loadJSON(CLIENTS_FILE);
let pushTokens = loadJSON(PUSH_TOKENS_FILE);

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
    notification: { title, body },
    data: {
      deviceId: targetDeviceId || '',
      click_action: 'FLUTTER_NOTIFICATION_CLICK'
    },
    tokens: recipientTokens
  };

  try {
    const response = await admin.messaging().sendEachForMulticast(message);
    console.log(`[FCM] Notificação Push enviada com sucesso: ${response.successCount} entregues.`);
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

        broadcast({
          type: 'alarm',
          data: { deviceId, type, status: 'ACTIVE', startedAt: timestamp }
        });

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
  const clientList = Object.values(clients);
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
    clientsCount: clientList.length,
    equipmentsCount: deviceList.length,
    onlineCount: deviceList.filter(d => d.online).length,
    alarmsCount: activeAlarmsCount
  });
});

// --- ROTAS DE CLIENTES ---

// Listar todos os clientes
app.get('/api/clients', (req, res) => {
  res.json(Object.values(clients));
});

// Cadastrar novo cliente
app.post('/api/clients', (req, res) => {
  const { name, email, phone } = req.body;

  if (!name) {
    return res.status(400).json({ ok: false, message: 'Nome do cliente é obrigatório.' });
  }

  const clientId = Date.now().toString();
  const createdAt = new Date().toISOString();

  const newClient = {
    id: clientId,
    name,
    email: email || '',
    phone: phone || '',
    createdAt
  };

  clients[clientId] = newClient;
  saveJSON(CLIENTS_FILE, clients);

  if (db) {
    try {
      db.prepare(`
        INSERT INTO clients (name, email, phone, createdAt)
        VALUES (?, ?, ?, ?)
      `).run(name, email || '', phone || '', createdAt);
    } catch (err) {
      console.error('Erro ao gravar cliente no SQLite:', err);
    }
  }

  res.json({ ok: true, client: newClient });
});

// Excluir cliente
app.delete('/api/clients/:id', (req, res) => {
  const { id } = req.params;

  if (!clients[id]) {
    return res.status(404).json({ ok: false, message: 'Cliente não encontrado.' });
  }

  delete clients[id];
  saveJSON(CLIENTS_FILE, clients);

  res.json({ ok: true, message: 'Cliente excluído com sucesso.' });
});

// --- ROTAS DE EQUIPAMENTOS ---

// Listar todos os equipamentos
app.get('/api/devices', (req, res) => {
  res.json(Object.values(devices));
});

// Cadastrar novo equipamento
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

  broadcast({
    type: 'device_registered',
    data: devices[id]
  });

  res.json({ ok: true, device: devices[id] });
});

// EXCLUIR EQUIPAMENTO
app.delete('/api/devices/:id', (req, res) => {
  const { id } = req.params;

  if (!devices[id]) {
    return res.status(404).json({ ok: false, message: 'Equipamento não encontrado.' });
  }

  delete devices[id];
  saveJSON(DATA_FILE, devices);

  // Remove histórico e alarmes associados no SQLite
  if (db) {
    try {
      db.prepare('DELETE FROM alarms WHERE deviceId = ?').run(id);
      db.prepare('DELETE FROM telemetry_history WHERE deviceId = ?').run(id);
    } catch (err) {
      console.error('Erro ao excluir histórico do equipamento no SQLite:', err);
    }
  }

  // Notifica os apps em tempo real
  broadcast({
    type: 'device_deleted',
    data: { id }
  });

  res.json({ ok: true, message: `Equipamento ${id} excluído com sucesso.` });
});

// Atualizar limites e configurações do equipamento
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

// --- TELEMETRIA E NOTIFICAÇÕES ---

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
        updatedDevice.defrostOn ? 1 : 0,
        timestamp
      );
    } catch (e) {
      console.error('Erro ao gravar histórico no SQLite:', e);
    }
  }

  if (updatedDevice.tempMax !== null && updatedDevice.temperature > updatedDevice.tempMax) {
    triggerAlarm(
      id,
      'TEMP_HIGH',
      updatedDevice.temperature,
      updatedDevice.tempMax,
      'Alarme: Temperatura Alta!',
      `O equipamento ${existing.name || id} atingiu ${updatedDevice.temperature}°C (Limite: ${updatedDevice.tempMax}°C)`
    );
  }

  if (updatedDevice.tempMin !== null && updatedDevice.temperature < updatedDevice.tempMin) {
    triggerAlarm(
      id,
      'TEMP_LOW',
      updatedDevice.temperature,
      updatedDevice.tempMin,
      'Alarme: Temperatura Baixa!',
      `O equipamento ${existing.name || id} atingiu ${updatedDevice.temperature}°C (Limite: ${updatedDevice.tempMin}°C)`
    );
  }

  broadcast({
    type: 'telemetry',
    data: updatedDevice
  });

  res.json({ ok: true, device: updatedDevice });
});

// Registro de Tokens Push FCM
app.post('/api/push/register', (req, res) => {
  const { token, platform, deviceIds } = req.body;

  if (!token) {
    return res.status(400).json({ ok: false, message: 'Token de notificação não fornecido.' });
  }

  pushTokens[token] = {
    platform: platform || 'android',
    deviceIds: deviceIds || [],
    updatedAt: new Date().toISOString()
  };

  saveJSON(PUSH_TOKENS_FILE, pushTokens);
  res.json({ ok: true, message: 'Token registrado com sucesso.' });
});

// --- CONSULTAS E HISTÓRICOS ---

app.get('/api/alarms/:id', (req, res) => {
  const { id } = req.params;
  const { status } = req.query;

  if (!db) return res.json([]);

  try {
    let query = 'SELECT * FROM alarms WHERE deviceId = ?';
    const params = [id];

    if (status && status !== 'all') {
      query += ' AND status = ?';
      params.push(status.toUpperCase());
    }

    query += ' ORDER BY id DESC LIMIT 500';
    const alarms = db.prepare(query).all(...params);
    res.json(alarms);
  } catch (err) {
    console.error('Erro ao procurar alarmes:', err);
    res.status(500).json({ ok: false, message: 'Erro ao buscar alarmes.' });
  }
});

app.get('/api/history/:id/summary', (req, res) => {
  const { id } = req.params;
  const hours = parseInt(req.query.hours || '24', 10);

  if (!db) {
    return res.json({
      deviceId: id,
      hours,
      bucketMinutes: 5,
      stats: { totalRecords: 0, minTemperature: null, avgTemperature: null, maxTemperature: null, firstAt: null, lastAt: null },
      series: [],
      recent: []
    });
  }

  try {
    const recent = db.prepare('SELECT * FROM telemetry_history WHERE deviceId = ? ORDER BY id DESC LIMIT 100').all(id);
    const statsRow = db.prepare(`
      SELECT 
        COUNT(*) as totalRecords,
        MIN(temperature) as minTemperature,
        AVG(temperature) as avgTemperature,
        MAX(temperature) as maxTemperature,
        MIN(createdAt) as firstAt,
        MAX(createdAt) as lastAt
      FROM telemetry_history WHERE deviceId = ?
    `).get(id);

    res.json({
      deviceId: id,
      hours,
      bucketMinutes: 5,
      stats: statsRow || { totalRecords: 0, minTemperature: null, avgTemperature: null, maxTemperature: null, firstAt: null, lastAt: null },
      series: [],
      recent
    });
  } catch (e) {
    console.error('Erro ao compor histórico:', e);
    res.status(500).json({ ok: false, message: 'Erro ao gerar histórico.' });
  }
});

app.get('/api/operations/:id/summary', (req, res) => {
  const { id } = req.params;
  const hours = parseInt(req.query.hours || '24', 10);

  res.json({
    deviceId: id,
    hours,
    compressor: { cycles: 0, totalSec: 0, avgSec: 0, maxSec: 0, active: false, activeSince: null, activeSec: 0 },
    defrost: { cycles: 0, totalSec: 0, avgSec: 0, maxSec: 0, active: false, activeSince: null, activeSec: 0 }
  });
});

app.get('/api/operations/:id', (req, res) => {
  res.json([]);
});

app.get('/', (req, res) => {
  res.send('API Eletro Mais em execução com sucesso!');
});

// 7. Arranque do Servidor
server.listen(PORT, '0.0.0.0', () => {
  console.log(`Servidor rodando na porta ${PORT}`);
});
