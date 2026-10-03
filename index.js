const express = require('express');
const cors = require('cors');
const fs = require('fs');
const path = require('path');
const http = require('http');
const WebSocket = require('ws');
const Database = require('better-sqlite3');

const PORT = process.env.PORT || 3000;

// 1. Definição do diretório seguro
const baseDir = process.env.NODE_ENV === 'production' ? '/tmp' : __dirname;

if (!fs.existsSync(baseDir)) {
  fs.mkdirSync(baseDir, { recursive: true });
}

// Caminhos de persistência
const dbPath = path.join(baseDir, 'database.db');
const DATA_FILE = path.join(baseDir, 'devices.json');

// 2. Inicialização do SQLite
let db;
try {
  db = new Database(dbPath);
  console.log('Banco de dados SQLite carregado em: ${dbPath}');
} catch (error) {
  console.error('Erro ao inicializar o banco SQLite:', error);
}

// 3. Funções auxiliares JSON
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

// 4. Servidor Express e WebSockets
const app = express();
app.use(cors());
app.use(express.json());

const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

wss.on('connection', (ws) => {
  console.log('Novo cliente WebSocket conectado.');

  ws.on('message', (message) => {
    console.log('Mensagem recebida via WS: ${message}');
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

// Rota de Health Check / Resumo do Painel (Resolve "Servidor indisponível")
app.get('/api/status', (req, res) => {
  res.json({ ok: true, status: 'online' });
});

app.get('/api/summary', (req, res) => {
  const deviceList = Object.values(devices);
  res.json({
    ok: true,
    clientsCount: 0,
    equipmentsCount: deviceList.length,
    onlineCount: deviceList.length,
    alarmsCount: 0
  });
});

// Dispositivos / Equipamentos
app.get('/api/devices', (req, res) => {
  res.json({ ok: true, devices });
});

app.get('/api/equipments', (req, res) => {
  res.json({ ok: true, equipments: Object.values(devices) });
});

// Telemetria
app.post('/api/telemetry', (req, res) => {
  const { id, temperature, humidity } = req.body;

  if (!id) {
    return res.status(400).json({ ok: false, message: 'ID do dispositivo é obrigatório.' });
  }

  const timestamp = new Date().toISOString();

  devices[id] = {
    id,
    temperature,
    humidity,
    updatedAt: timestamp
  };
  saveDevices();

  broadcast({ type: 'TELEMETRY_UPDATE', device: devices[id] });

  res.json({ ok: true, device: devices[id] });
});

// Alarmes
app.get('/api/alarms/:id', (req, res) => {
  const { id } = req.params;
  res.json({
    ok: true,
    deviceId: id,
    alarms: []
  });
});

app.get('/api/alarms', (req, res) => {
  res.json({
    ok: true,
    alarms: []
  });
});

// Histórico
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

// Rota raiz
app.get('/', (req, res) => {
  res.send('API Eletro Mais em execução com sucesso!');
});

// 6. Arranque do Servidor
server.listen(PORT, '0.0.0.0', () => {
  console.log('Servidor rodando na porta ${PORT}');
});
