const express = require('express');
const cors = require('cors');
const fs = require('fs');
const path = require('path');
const http = require('http');
const WebSocket = require('ws');
const Database = require('better-sqlite3');

// A Railway injeta process.env.PORT. Fallback para 8080.
const PORT = process.env.PORT || 8080;

// 1. Definição e criação do diretório seguro para o banco e arquivos em produção
const baseDir = process.env.NODE_ENV === 'production' ? '/tmp' : __dirname;

if (!fs.existsSync(baseDir)) {
  fs.mkdirSync(baseDir, { recursive: true });
}

// Caminhos dos arquivos de persistência
const dbPath = path.join(baseDir, 'database.db');
const DATA_FILE = path.join(baseDir, 'devices.json');

// 2. Inicialização do Banco de Dados SQLite e tabela de Alarmes
let db;

try {
  db = new Database(dbPath);
  console.log('Banco de dados SQLite carregado em: ${dbPath}');

  // Tabela para histórico persistente de alarmes/notificações
  db.exec(`
    CREATE TABLE IF NOT EXISTS alarms (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      deviceId TEXT NOT NULL,
      type TEXT NOT NULL,
      message TEXT NOT NULL,
      severity TEXT DEFAULT 'warning',
      resolved INTEGER DEFAULT 0,
      timestamp TEXT NOT NULL
    )
  `);
} catch (error) {
  console.error('Erro ao inicializar o banco SQLite:', error);
}

// 3. Funções auxiliares para gerenciar o arquivo JSON secundário
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
    fs.writeFileSync(
      DATA_FILE,
      JSON.stringify(devices, null, 2),
      'utf8'
    );
  } catch (err) {
    console.error('Erro ao salvar devices.json:', err);
  }
}

// Auxiliar para registar alarmes no banco e notificar via WebSocket
function createAlarm(deviceId, type, message, severity = 'warning') {
  const timestamp = new Date().toISOString();
  
  if (db) {
    try {
      const stmt = db.prepare(`
        INSERT INTO alarms (deviceId, type, message, severity, resolved, timestamp)
        VALUES (?, ?, ?, ?, 0, ?)
      `);
      stmt.run(deviceId, type, message, severity, timestamp);
    } catch (err) {
      console.error('Erro ao guardar alarme no SQLite:', err);
    }
  }

  const alarmPayload = {
    deviceId,
    type,
    message,
    severity,
    resolved: false,
    timestamp
  };

  // Dispara a notificação de alarme em tempo real
  broadcast({
    type: 'ALARM_NOTIFICATION',
    data: alarmPayload
  });

  return alarmPayload;
}

// 4. Configuração do Servidor Express e WebSockets
const app = express();

app.use(cors());
app.use(express.json());

const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

// Eventos de WebSocket
wss.on('connection', (ws) => {
  console.log('Novo cliente WebSocket conectado.');

  ws.on('message', (message) => {
    console.log('Mensagem recebida via WS: ${message}');
  });

  ws.on('close', () => {
    console.log('Cliente WebSocket desconectado.');
  });
});

// Broadcast para enviar atualizações em tempo real a todos os clientes
function broadcast(data) {
  wss.clients.forEach((client) => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(JSON.stringify(data));
    }
  });
}

// 5. Rotas da API REST

// Status do servidor e estatísticas do painel
app.get('/api/summary', (req, res) => {
  const deviceList = Object.values(devices);
  let activeAlarmsCount = 0;

  if (db) {
    try {
      const row = db.prepare('SELECT COUNT(*) as count FROM alarms WHERE resolved = 0').get();
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

// Lista de equipamentos — o app espera um ARRAY
app.get('/api/devices', (req, res) => {
  res.json(Object.values(devices));
});

// Cadastro de novo equipamento
app.post('/api/devices/register', (req, res) => {
  const { id, name, client, location } = req.body;

  if (!id || !name) {
    return res.status(400).json({
      ok: false,
      message: 'ID e nome do equipamento são obrigatórios.'
    });
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
    online: false,
    updatedAt: new Date().toISOString()
  };

  saveDevices();

  res.json({ ok: true, device: devices[id] });
});

// Telemetria enviada pelos dispositivos (ESP32 etc.)
app.post('/api/telemetry', (req, res) => {
  const { id, temperature, humidity, vibration, compressorOn } = req.body;

  if (!id) {
    return res.status(400).json({
      ok: false,
      message: 'ID do dispositivo é obrigatório.'
    });
  }

  const timestamp = new Date().toISOString();

  const existing = devices[id] || {
    id,
    name: id,
    client: '',
    location: ''
  };

  const updatedDevice = {
    ...existing,
    id,
    temperature: temperature ?? existing.temperature ?? 0,
    humidity: humidity ?? existing.humidity ?? 0,
    vibration: vibration ?? existing.vibration ?? 0,
    compressorOn: compressorOn ?? existing.compressorOn ?? false,
    online: true,
    updatedAt: timestamp
  };

  devices[id] = updatedDevice;
  saveDevices();

  // --- Regras de disparo de Alarmes/Notificações automáticas ---
  if (temperature !== undefined && temperature > 10) {
    createAlarm(
      id,
      'HIGH_TEMPERATURE',
      Temperatura elevada detetada no equipamento ${existing.name || id}: ${temperature}°C,
      'critical'
    );
  }

  if (vibration !== undefined && vibration > 5) {
    createAlarm(
      id,
      'HIGH_VIBRATION',
      Nível de vibração anormal no equipamento ${existing.name || id}: ${vibration},
      'warning'
    );
  }

  // Dispara evento de telemetria via WebSocket em tempo real
  broadcast({
    type: 'telemetry',
    data: updatedDevice
  });

  res.json({
    ok: true,
    device: updatedDevice
  });
});

// --- ROTAS DE ALARMES E NOTIFICAÇÕES ---

// Obter todos os alarmes
app.get('/api/alarms', (req, res) => {
  if (!db) return res.json([]);

  try {
    const alarms = db.prepare('SELECT * FROM alarms ORDER BY id DESC LIMIT 50').all();
    res.json(alarms);
  } catch (err) {
    console.error('Erro ao procurar alarmes:', err);
    res.status(500).json({ ok: false, message: 'Erro ao carregar alarmes.' });
  }
});

// Obter alarmes específicos de um equipamento
app.get('/api/alarms/:id', (req, res) => {
  const { id } = req.params;
  if (!db) return res.json({ ok: true, deviceId: id, alarms: [] });

  try {
    const alarms = db.prepare('SELECT * FROM alarms WHERE deviceId = ? ORDER BY id DESC').all(id);
    res.json({
      ok: true,
      deviceId: id,
      alarms
    });
  } catch (err) {
    console.error('Erro ao procurar alarmes do dispositivo:', err);
    res.status(500).json({ ok: false, message: 'Erro ao carregar alarmes do dispositivo.' });
  }
});

// Marcar alarme como resolvido
app.post('/api/alarms/:id/resolve', (req, res) => {
  const { id } = req.params;

  if (db) {
    try {
      db.prepare('UPDATE alarms SET resolved = 1 WHERE id = ?').run(id);
    } catch (err) {
      console.error('Erro ao resolver alarme:', err);
    }
  }

  res.json({ ok: true, message: 'Alarme resolvido com sucesso.' });
});

// Rota de histórico por dispositivo
app.get('/api/history/:id/summary', (req, res) => {
  const { id } = req.params;
  const { period } = req.query;

  res.json({
    ok: true,
    deviceId: id,
    period: period || '24h',
    history: []
  });
});

// Rota padrão para verificação de status
app.get('/', (req, res) => {
  res.send('API Eletro Mais em execução com sucesso!');
});

// 6. Inicialização do Servidor
server.listen(PORT, '0.0.0.0', () => {
  console.log('Servidor rodando na porta ${PORT}');
});
