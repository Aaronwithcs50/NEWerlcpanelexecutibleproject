const express = require('express');
const axios = require('axios');
const dotenv = require('dotenv');
const bodyParser = require('body-parser');
const cors = require('cors');
const fs = require('fs').promises;
const path = require('path');
const crypto = require('crypto');

dotenv.config();

const PORT = Number(process.env.PORT) || 3000;
const SERVER_KEY = process.env.SERVER_KEY || 'YOUR_SERVER_KEY_HERE';
const GLOBAL_KEY = process.env.GLOBAL_KEY || '';
const BASE_URL = 'https://api.policeroleplay.community/v1';
const LOGS_PATH = path.join(__dirname, 'logs.json');

const ERROR_MAP = {
  0: 'Unknown error',
  1001: 'Roblox/in-game comms error',
  1002: 'Internal system error',
  2000: 'Missing server-key header',
  2001: 'Malformed server-key',
  2002: 'Invalid or expired server-key',
  2003: 'Invalid global API key',
  2004: 'Server-key banned',
  3001: 'Invalid or missing command in body',
  3002: 'Server offline / no players',
  4001: 'Rate limited',
  4002: 'Command restricted',
  4003: 'Message prohibited',
  9998: 'Resource restricted',
  9999: 'In-game module out of date'
};

const COMMAND_BUCKET_MS = 5000;
const COMMAND_QUEUE = new Map();

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

class ErlcClient {
  constructor(serverKey, globalKey = '') {
    this.serverKey = serverKey;
    this.globalKey = globalKey;
    this.baseURL = BASE_URL;
    this.rateLimitRemaining = null;
    this.consecutive403 = 0;
    this.blocked = false;
  }

  setServerKey(serverKey) {
    this.serverKey = serverKey;
    this.consecutive403 = 0;
    this.blocked = false;
    this.rateLimitRemaining = null;
  }

  getHeaders() {
    const headers = {
      'server-key': this.serverKey
    };

    if (this.globalKey) {
      headers.Authorization = this.globalKey;
    }

    return headers;
  }

  async request(method, endpoint, data = undefined, attempt = 0) {
    if (this.blocked) {
      const err = new Error('Too many forbidden responses. Requests halted.');
      err.code = 403;
      throw err;
    }

    try {
      const response = await axios({
        method,
        url: `${this.baseURL}${endpoint}`,
        headers: this.getHeaders(),
        data,
        validateStatus: () => true
      });

      if (response.headers && response.headers['x-ratelimit-remaining'] !== undefined) {
        this.rateLimitRemaining = Number(response.headers['x-ratelimit-remaining']);
      }

      if (response.status === 403) {
        this.consecutive403 += 1;
        if (this.consecutive403 >= 3) {
          this.blocked = true;
        }
      } else {
        this.consecutive403 = 0;
      }

      if (response.status === 200) {
        return response.data;
      }

      if (response.status === 422 && endpoint === '/server/command') {
        const err = new Error('Server has no players.');
        err.code = 3002;
        throw err;
      }

      const body = response.data || {};
      const code = typeof body.Code === 'number' ? body.Code : (typeof body.code === 'number' ? body.code : null);

      if (response.status === 429 || code === 4001) {
        const retryAfter = Number(body.retry_after || body.retryAfter || 1);
        if (attempt < 1) {
          await sleep(retryAfter * 1000);
          return this.request(method, endpoint, data, attempt + 1);
        }
        const err = new Error(ERROR_MAP[4001]);
        err.code = 4001;
        err.retryAfter = retryAfter;
        throw err;
      }

      if (code === 0 && attempt < 1) {
        await sleep(2000);
        return this.request(method, endpoint, data, attempt + 1);
      }

      if (code !== null) {
        const err = new Error(ERROR_MAP[code] || 'API error');
        err.code = code;
        if (body.retry_after) {
          err.retryAfter = Number(body.retry_after);
        }
        throw err;
      }

      const genericError = new Error(body.message || `HTTP ${response.status}`);
      genericError.code = response.status;
      throw genericError;
    } catch (err) {
      if (err.response) {
        throw err;
      }
      if (err.code && ERROR_MAP[err.code]) {
        throw err;
      }

      const networkError = new Error(err.message || 'Network error');
      networkError.code = 0;
      throw networkError;
    }
  }

  enqueueCommand(task) {
    const bucket = `command-${this.serverKey}`;
    const state = COMMAND_QUEUE.get(bucket) || { chain: Promise.resolve(), lastRunAt: 0 };

    const run = async () => {
      const now = Date.now();
      const wait = Math.max(0, COMMAND_BUCKET_MS - (now - state.lastRunAt));
      if (wait > 0) {
        await sleep(wait);
      }
      const result = await task();
      state.lastRunAt = Date.now();
      return result;
    };

    state.chain = state.chain.then(run, run);
    COMMAND_QUEUE.set(bucket, state);
    return state.chain;
  }

  getServerStatus() {
    return this.request('GET', '/server');
  }

  getPlayers() {
    return this.request('GET', '/server/players');
  }

  getJoinLogs() {
    return this.request('GET', '/server/joinlogs');
  }

  getKillLogs() {
    return this.request('GET', '/server/killlogs');
  }

  getCommandLogs() {
    return this.request('GET', '/server/commandlogs');
  }

  getModCalls() {
    return this.request('GET', '/server/modcalls');
  }

  getBans() {
    return this.request('GET', '/server/bans');
  }

  getVehicles() {
    return this.request('GET', '/server/vehicles');
  }

  getQueue() {
    return this.request('GET', '/server/queue');
  }

  getStaff() {
    return this.request('GET', '/server/staff');
  }

  runCommand(commandStr) {
    return this.enqueueCommand(async () => this.request('POST', '/server/command', { command: commandStr }));
  }

  async findUserByUsername(username) {
    const players = await this.getPlayers();
    const target = String(username || '').trim().toLowerCase();

    const player = (players || []).find((entry) => {
      const [name] = String(entry.Player || '').split(':');
      return name.trim().toLowerCase() === target;
    });

    if (!player) {
      const err = new Error('User not found in live players list.');
      err.code = 404;
      throw err;
    }

    const [name, id] = String(player.Player || '').split(':');
    return {
      raw: player,
      username: name || '',
      playerId: id || '',
      permission: player.Permission || '',
      callsign: player.Callsign || '',
      team: player.Team || ''
    };
  }
}

let erlcClient = new ErlcClient(SERVER_KEY, GLOBAL_KEY);
let activeServerKey = SERVER_KEY;
let writeLock = Promise.resolve();

async function ensureLogsFile() {
  try {
    await fs.access(LOGS_PATH);
  } catch {
    await fs.writeFile(LOGS_PATH, '{}', 'utf8');
  }
}

async function readLogs() {
  await ensureLogsFile();
  const raw = await fs.readFile(LOGS_PATH, 'utf8');
  try {
    return JSON.parse(raw || '{}');
  } catch {
    return {};
  }
}

function hashKey(serverKey) {
  return crypto.createHash('sha256').update(serverKey).digest('hex');
}

function createEmptyLogSet() {
  return {
    joinLogs: [],
    killLogs: [],
    commandLogs: []
  };
}

function normalizeLogEntry(logType, entry) {
  if (logType === 'joinLogs') {
    return {
      Join: Boolean(entry.Join),
      Timestamp: Number(entry.Timestamp),
      Player: String(entry.Player || '')
    };
  }
  if (logType === 'killLogs') {
    return {
      Killed: String(entry.Killed || ''),
      Timestamp: Number(entry.Timestamp),
      Killer: String(entry.Killer || '')
    };
  }
  return {
    Player: String(entry.Player || ''),
    Timestamp: Number(entry.Timestamp),
    Command: String(entry.Command || '')
  };
}

function dedupeLogEntries(logType, existing, incoming) {
  const byKey = new Map();
  const all = [...(existing || []), ...(incoming || [])].map((item) => normalizeLogEntry(logType, item));

  all.forEach((entry) => {
    let key;
    if (logType === 'joinLogs') {
      key = `${entry.Timestamp}-${entry.Player}-${entry.Join}`;
    } else if (logType === 'killLogs') {
      key = `${entry.Timestamp}-${entry.Killed}-${entry.Killer}`;
    } else {
      key = `${entry.Timestamp}-${entry.Player}-${entry.Command}`;
    }
    byKey.set(key, entry);
  });

  return Array.from(byKey.values()).sort((a, b) => (Number(a.Timestamp) || 0) - (Number(b.Timestamp) || 0));
}

async function withWriteLock(operation) {
  const next = writeLock.then(operation, operation);
  writeLock = next.then(() => undefined, () => undefined);
  return next;
}

async function mergeAndStoreLogs(serverKey, logType, incoming) {
  return withWriteLock(async () => {
    const allLogs = await readLogs();
    const keyHash = hashKey(serverKey);
    if (!allLogs[keyHash]) {
      allLogs[keyHash] = createEmptyLogSet();
    }

    const existing = allLogs[keyHash][logType] || [];
    const merged = dedupeLogEntries(logType, existing, incoming);
    allLogs[keyHash][logType] = merged;

    await fs.writeFile(LOGS_PATH, JSON.stringify(allLogs, null, 2), 'utf8');
    return merged;
  });
}

function sendError(res, error) {
  const code = error.code || 500;
  const payload = {
    error: true,
    code,
    message: error.message || ERROR_MAP[code] || 'Unexpected error'
  };
  if (error.retryAfter !== undefined) {
    payload.retryAfter = error.retryAfter;
  }
  res.status(code >= 400 && code < 600 ? code : 500).json(payload);
}

const app = express();
app.use(cors());
app.use(bodyParser.json());
app.use(express.static(path.join(__dirname, 'public')));

app.get('/api/server', async (req, res) => {
  try {
    const data = await erlcClient.getServerStatus();
    res.json(data);
  } catch (error) {
    sendError(res, error);
  }
});

app.get('/api/players', async (req, res) => {
  try {
    const data = await erlcClient.getPlayers();
    res.json(data);
  } catch (error) {
    sendError(res, error);
  }
});

app.get('/api/logs/join', async (req, res) => {
  try {
    const data = await erlcClient.getJoinLogs();
    const merged = await mergeAndStoreLogs(activeServerKey, 'joinLogs', data || []);
    res.json(merged);
  } catch (error) {
    sendError(res, error);
  }
});

app.get('/api/logs/kill', async (req, res) => {
  try {
    const data = await erlcClient.getKillLogs();
    const merged = await mergeAndStoreLogs(activeServerKey, 'killLogs', data || []);
    res.json(merged);
  } catch (error) {
    sendError(res, error);
  }
});

app.get('/api/logs/command', async (req, res) => {
  try {
    const data = await erlcClient.getCommandLogs();
    const merged = await mergeAndStoreLogs(activeServerKey, 'commandLogs', data || []);
    res.json(merged);
  } catch (error) {
    sendError(res, error);
  }
});

app.get('/api/logs/modcalls', async (req, res) => {
  try {
    const data = await erlcClient.getModCalls();
    res.json(data);
  } catch (error) {
    sendError(res, error);
  }
});

app.get('/api/server/bans', async (req, res) => {
  try {
    const data = await erlcClient.getBans();
    res.json(data);
  } catch (error) {
    sendError(res, error);
  }
});

app.get('/api/server/vehicles', async (req, res) => {
  try {
    const data = await erlcClient.getVehicles();
    res.json(data);
  } catch (error) {
    sendError(res, error);
  }
});

app.get('/api/server/queue', async (req, res) => {
  try {
    const data = await erlcClient.getQueue();
    res.json(data);
  } catch (error) {
    sendError(res, error);
  }
});

app.get('/api/server/staff', async (req, res) => {
  try {
    const data = await erlcClient.getStaff();
    res.json(data);
  } catch (error) {
    sendError(res, error);
  }
});

app.post('/api/command', async (req, res) => {
  try {
    const command = String(req.body.command || '').trim();
    if (!command) {
      const err = new Error('Command is required.');
      err.code = 3001;
      throw err;
    }
    await erlcClient.runCommand(command);
    res.json({ success: true, command });
  } catch (error) {
    sendError(res, error);
  }
});

app.post('/api/findUser', async (req, res) => {
  try {
    const username = String(req.body.username || '').trim();
    if (!username) {
      const err = new Error('Username is required.');
      err.code = 400;
      throw err;
    }
    const data = await erlcClient.findUserByUsername(username);
    res.json(data);
  } catch (error) {
    sendError(res, error);
  }
});

app.post('/api/setServer', async (req, res) => {
  try {
    const serverKey = String(req.body.serverKey || '').trim();
    if (!serverKey) {
      const err = new Error('serverKey is required.');
      err.code = 400;
      throw err;
    }
    activeServerKey = serverKey;
    erlcClient = new ErlcClient(serverKey, GLOBAL_KEY);
    res.json({ success: true });
  } catch (error) {
    sendError(res, error);
  }
});

ensureLogsFile().then(() => {
  app.listen(PORT, () => {
    process.stdout.write(`ER:LC Operations Panel running on port ${PORT}\n`);
  });
});
