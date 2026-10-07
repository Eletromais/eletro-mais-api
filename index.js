const express = require('express');
const cors = require('cors');
const fs = require('fs');
const path = require('path');
const http = require('http');
const WebSocket = require('ws');
const Database = require('better-sqlite3');

const PORT = process.env.PORT || 8080;

// 1. Definição do diretório seguro para produção e desenvolvimento
const baseDir = process.env.NODE_ENV === 'production' ? '/tmp' : __dirname;

if (!fs.existsSync(baseDir)) {
  fs.mkdirSync(baseDir, { recursive: true });
}

const dbPath = path.join(baseDir, 'database.db');
const DATA_FILE = path.join(baseDir, 'devices.json');

// 2. Inicialização do SQLite e tabelas
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

// 3. Funções auxiliares para persistência em JSON
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

// Helper para emitir alarmes
function triggerAlarm(deviceId, type, startValue, thresholdValue) {
  const timestamp = new Date().toISOString();
  if (db) {
    try {
      // Verifica se já existe um alarme ativo do mesmo tipo para não duplicar
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
      }
    } catch (err) {
      console.error('Erro ao gerar alarme:', err);
    }
  }
}

// 4. Servidor Express e WebSockets
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

// 5. Rotas da API REST

// Retorna lista direta de equipamentos (O Flutter aguarda um Array)
app.get('/api/devices', (req, res) => {
  res.json(Object.values(devices));
});

// Atualiza configurações de alarme do equipamento
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

  saveDevices();
  res.json({ ok: true, device: devices[id] });
});

// Registra novo equipamento
app.post('/api/devices/register', (req, res) => {
  const { id, name, client, location } = req.body;

  if (!id || !name) {
    return res.status(400).json({ ok: false, message: 'ID e nome são obrigatórios.' });
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

  saveDevices();
  res.json({ ok: true, device: devices[id] });
});

// Recebe telemetria do dispositivo
app.post('/api/telemetry', (req, res) => {
  const { id, temperature, humidity, vibration, compressorOn, defrostOn, sensorOk } = req.body;

  if (!id) {
    return res.status(400).json({ ok: false, message: 'ID é obrigatório.' });
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
  saveDevices();

  // Salva no histórico do SQLite
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

  // Regras de Alarmes
  if (updatedDevice.tempMax !== null && updatedDevice.temperature > updatedDevice.tempMax) {
    triggerAlarm(id, 'TEMP_HIGH', updatedDevice.temperature, updatedDevice.tempMax);
  }
  if (updatedDevice.tempMin !== null && updatedDevice.temperature < updatedDevice.tempMin) {
    triggerAlarm(id, 'TEMP_LOW', updatedDevice.temperature, updatedDevice.tempMin);
  }

  // Notifica clientes em tempo real via WS
  broadcast({
    type: 'telemetry',
    data: updatedDevice
  });

  res.json({ ok: true, device: updatedDevice });
});

// Registra token FCM para Notificações Push
app.post('/api/push/register', (req, res) => {
  const { token, platform, deviceIds } = req.body;
  console.log(`Token FCM registrado para platform [${platform}]: ${token}`);
  res.json({ ok: true, message: 'Token registrado com sucesso.' });
});

// Rota de alarmes para um dispositivo específico
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
    console.error('Erro ao buscar alarmes:', err);
    res.status(500).json({ ok: false, message: 'Erro ao buscar alarmes.' });
  }
});

// Resumo do Histórico para os Gráficos no Flutter
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

// Operações / Ciclos de Compressor e Degelo
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

// Rota raiz
app.get('/', (req, res) => {
  res.send('API Eletro Mais em execução com sucesso!');
});

// 6. Arranque do Servidor
server.listen(PORT, '0.0.0.0', () => {
  console.log(`Servidor rodando na porta ${PORT}`);
});
