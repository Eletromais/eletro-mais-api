const express = require('express');
const cors = require('cors');
const fs = require('fs');
const path = require('path');
const http = require('http');
const WebSocket = require('ws');
const Database = require('better-sqlite3');

const PORT = process.env.PORT || 8080;

// 1. Definição e criação do diretório seguro para o banco e arquivos em produção
const baseDir = process.env.NODE_ENV === 'production' ? '/tmp' : __dirname;

if (!fs.existsSync(baseDir)) {
  fs.mkdirSync(baseDir, { recursive: true });
}

// Caminhos dos arquivos de persistência
const dbPath = path.join(baseDir, 'database.db');
const DATA_FILE = path.join(baseDir, 'devices.json');

// 2. Inicialização do Banco de Dados SQLite (better-sqlite3)
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
    fs.writeFileSync(DATA_FILE, JSON.stringify(devices, null, 2), 'utf8');
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
    console.log(`Mensagem recebida via WS: ${message}`);
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
app.get('/api/devices', (req, res) => {
  res.json({ ok: true, devices });
});

app.post('/api/telemetry', (req, res) => {
  const { id, temperature, humidity } = req.body;

  if (!id) {
    return res.status(400).json({ ok: false, message: 'ID do dispositivo é obrigatório.' });
  }

  const timestamp = new Date().toISOString();

  // Atualiza em memória e salva no JSON
  devices[id] = {
    id,
    temperature,
    humidity,
    updatedAt: timestamp
  };
  saveDevices();

  // Dispara evento via WebSocket em tempo real
  broadcast({ type: 'TELEMETRY_UPDATE', device: devices[id] });

  res.json({ ok: true, device: devices[id] });
});

// Rota padrão para verificação de status
app.get('/', (req, res) => {
  res.send('API Eletro Mais em execução com sucesso!');
});

// 6. Inicialização do Servidor
server.listen(PORT, () => {
  console.log(`Servidor rodando na porta ${PORT}`);
});
