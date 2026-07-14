const WebSocket = require('ws');
const http = require('http');

const users = new Map();

// Cloud production global environment routing path configuration
const server = http.createServer((req, res) => {
  if (req.url === '/' || req.url === '/health') {
    res.writeHead(200, { 'Content-Type': 'text/plain', 'Access-Control-Allow-Origin': '*' });
    res.end('Zeabur cloud signaling server is live and running!');
  } else {
    res.writeHead(404);
    res.end();
  }
});

const wss = new WebSocket.Server({ noServer: true });

// Cloud architecture reverse proxy headers upgrade pipeline
server.on('upgrade', (request, socket, head) => {
  wss.handleUpgrade(request, socket, head, (ws) => {
    wss.emit('connection', ws, request);
  });
});

wss.on('connection', (ws) => {
  let registeredUserId = null;
  ws.isAlive = true;

  ws.on('pong', () => { ws.isAlive = true; });

  ws.on('message', (message) => {
    try {
      const data = JSON.parse(message);
      const { type, from, to, data: payload } = data;

      // New user register identity checking routine
      if (type === 'register') {
        const userId = from;
        if (!userId || typeof userId !== 'string' || userId.trim() === '') {
          ws.send(JSON.stringify({ type: 'error', message: 'Invalid ID specification' }));
          return;
        }
        if (users.has(userId)) {
          ws.send(JSON.stringify({ type: 'error', message: 'User ID already taken' }));
          return;
        }
        users.set(userId, ws);
        registeredUserId = userId;
        ws.send(JSON.stringify({ type: 'registered', userId }));
        console.log(`User registered on cloud: ${userId}`);
        return;
      }

      if (!registeredUserId || from !== registeredUserId) {
        ws.send(JSON.stringify({ type: 'error', message: 'Unauthorized target session trace' }));
        return;
      }

      // Dynamic signal communication mapping logic pipeline
      const recipientWs = users.get(to);
      if (recipientWs && recipientWs.readyState === WebSocket.OPEN) {
        recipientWs.send(JSON.stringify({
          type,
          from: registeredUserId,
          to,
          data: payload
        }));
      } else {
        ws.send(JSON.stringify({ type: 'error', message: 'Target client peer currently offline' }));
      }

    } catch (error) {
      ws.send(JSON.stringify({ type: 'error', message: 'Malformed JSON dataset payload' }));
    }
  });

  ws.on('close', () => {
    if (registeredUserId) {
      users.delete(registeredUserId);
      console.log(`User cloud session disconnected: ${registeredUserId}`);
    }
  });
});

// Dynamic automated connection alive tracker checking script (30 Seconds interval)
const interval = setInterval(() => {
  wss.clients.forEach((ws) => {
    if (ws.isAlive === false) return ws.terminate();
    ws.isAlive = false;
    ws.ping();
  });
}, 30000);

wss.on('close', () => clearInterval(interval));

// Cloud automatic platform environment standard injection port selector runtime rules
const PORT = process.env.PORT || 8080;
server.listen(PORT, '0.0.0.0', () => {
  console.log(`Production container listening live on global interface port: ${PORT}`);
});
