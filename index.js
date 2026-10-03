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

// 2. Inicialização do Banco de Dados SQLite
let db;

try {
  db = new Database(dbPath);
  console.log('Banco de dados SQLite carregado em: ${dbPath}');
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
    console.log(Mensagem recebida via WS: ${message});
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

// Lista de equipamentos — o app espera um ARRAY, não um objeto
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

  devices[id] = {
    ...existing,
    id,
    temperature: temperature ?? existing.temperature ?? 0,
    humidity: humidity ?? existing.humidity ?? 0,
    vibration: vibration ?? existing.vibration ?? 0,
    compressorOn: compressorOn ?? existing.compressorOn ?? false,
    online: true,
    updatedAt: timestamp
  };

  saveDevices();

  // Dispara evento via WebSocket em tempo real
  broadcast({
    type: 'telemetry',
    data: devices[id]
  });

  res.json({
    ok: true,
    device: devices[id]
  });
});

// Rota padrão para verificação de status
app.get('/', (req, res) => {
  res.send('API Eletro Mais em execução com sucesso!');
});

// 6. Inicialização do Servidor
// Escutando em 0.0.0.0 para aceitar conexões do proxy Railway
server.listen(PORT, '0.0.0.0', () => {
  console.log(Servidor rodando na porta ${PORT});
});
