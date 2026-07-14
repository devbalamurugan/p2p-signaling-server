const WebSocket = require('ws');
const http = require('http');

const users = new Map();

// Back4App path structures routing checks configuration
const server = http.createServer((req, res) => {
  if (req.url === '/' || req.url === '/health') {
    res.writeHead(200, { 'Content-Type': 'text/plain', 'Access-Control-Allow-Origin': '*' });
    res.end('Back4App secure signaling container live sync!');
  } else {
    res.writeHead(404);
    res.end();
  }
});

// IMPORTANT: Do NOT use noServer true. Bind directly inside single unified port architecture
const wss = new WebSocket.Server({ server });

wss.on('connection', (ws) => {
  let registeredUserId = null;
  ws.isAlive = true;

  ws.on('pong', () => { ws.isAlive = true; });

  ws.on('message', (message) => {
    try {
      const data = JSON.parse(message);
      const { type, from, to, data: payload } = data;

      if (type === 'register') {
        const userId = from;
        if (!userId || typeof userId !== 'string' || userId.trim() === '') {
          ws.send(JSON.stringify({ type: 'error', message: 'Invalid registration name data properties' }));
          return;
        }
        if (users.has(userId)) {
          ws.send(JSON.stringify({ type: 'error', message: 'User ID already taken' }));
          return;
        }
        users.set(userId, ws);
        registeredUserId = userId;
        ws.send(JSON.stringify({ type: 'registered', userId }));
        console.log(`Cloud data routing identity trace: ${userId}`);
        return;
      }

      if (!registeredUserId || from !== registeredUserId) return;

      const recipientWs = users.get(to);
      if (recipientWs && recipientWs.readyState === WebSocket.OPEN) {
        recipientWs.send(JSON.stringify({ type, from: registeredUserId, to, data: payload }));
      }
    } catch (error) {
      console.log("JSON Schema validation crash pattern");
    }
  });

  ws.on('close', () => {
    if (registeredUserId) {
      users.delete(registeredUserId);
    }
  });
});

const interval = setInterval(() => {
  wss.clients.forEach((ws) => {
    if (ws.isAlive === false) return ws.terminate();
    ws.isAlive = false;
    ws.ping();
  });
}, 30000);

wss.on('close', () => clearInterval(interval));

// Back4App internal global deployment container port dynamic allocation injection matching
const PORT = process.env.PORT || 8080;
server.listen(PORT, '0.0.0.0', () => {
  console.log(`Container mapping server pipelines active visibility interface listening: ${PORT}`);
});
