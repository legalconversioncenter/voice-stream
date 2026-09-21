/**
 * five9-elevenlabs-bridge / server.js
 *
 * Bridges Five9 VoiceStream (gRPC) → ElevenLabs real-time STT (WebSocket).
 *
 * Single-port architecture (required for Railway / Cloud Run):
 *   A TCP connection sniffer listens on PORT (default 3000).
 *   - HTTP/2 connections (gRPC from Five9) → proxied to internal gRPC server
 *   - HTTP/1.1 connections (webhooks)      → handled by Express
 *
 * Per-call flow:
 *   1. Five9 opens TWO separate gRPC sessions per call — agent leg + customer leg.
 *   2. Trust token is verified from gRPC metadata (x-five9-trust-token).
 *   3. For each gRPC session we open ONE ElevenLabs WebSocket.
 *   4. Each StreamMedia audio chunk (LINEAR16 @ 8 kHz) is forwarded as a
 *      base64-encoded `input_audio_chunk` to ElevenLabs.
 *   5. ElevenLabs returns partial/committed transcripts which are logged.
 *   6. StreamStop triggers cleanup.
 *
 * HTTP endpoints for Five9 callbacks:
 *   GET  /call-events                     — Subscription validation (SHA-256 challenge)
 *   POST /call-events                     — CTI Call events
 *   POST /subscriptions/:subscriptionId   — Stream Status events
 *   GET  /health                          — Health + active session count
 */

'use strict';

require('dotenv').config();

const net    = require('net');
const path   = require('path');
const fs     = require('fs');
const http   = require('http');
const crypto = require('crypto');
const grpc   = require('@grpc/grpc-js');
const loader = require('@grpc/proto-loader');
const WebSocket = require('ws');
const express   = require('express');

// ─── Config ──────────────────────────────────────────────────────────────────

const {
  ELEVENLABS_API_KEY,
  ELEVENLABS_MODEL_ID = 'scribe_v2_realtime',
  FIVE9_TRUST_TOKEN,
  FIVE9_API_KEY,
  PORT          = '3000',   // single public port (Railway injects $PORT automatically)
  GRPC_INT_PORT = '50052',  // internal gRPC port (loopback only, not exposed)
  GRPC_TLS      = 'false',
  TLS_CERT_PATH,
  TLS_KEY_PATH,
  PROTO_DISCOVERY = 'false',
} = process.env;

if (!FIVE9_TRUST_TOKEN) throw new Error('Missing FIVE9_TRUST_TOKEN in environment');
if (PROTO_DISCOVERY !== 'true' && !ELEVENLABS_API_KEY) {
  throw new Error('Missing ELEVENLABS_API_KEY in environment');
}

const DISCOVERY_MODE = PROTO_DISCOVERY === 'true';
if (DISCOVERY_MODE) {
  console.log('⚠️  PROTO DISCOVERY MODE — raw gRPC messages will be logged. Do NOT use in production.');
}

const TRUST_TOKEN_SHA256 = crypto.createHash('sha256').update(FIVE9_TRUST_TOKEN).digest('hex');

// ─── Proto loading ────────────────────────────────────────────────────────────

const PROTO_PATH = path.join(__dirname, 'five9_voice_stream.proto');

const packageDefinition = loader.loadSync(PROTO_PATH, {
  keepCase: true,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true,
});

const { five9: { voicestream: proto } } = grpc.loadPackageDefinition(packageDefinition);

// ─── Active sessions ─────────────────────────────────────────────────────────

const sessions = new Map();   // key: `${callId}:${leg}` → { ws, callId, leg }

// ─── ElevenLabs WebSocket factory ────────────────────────────────────────────

function openElevenLabsSocket({ callId, leg, onTranscript }) {
  const url = new URL('wss://api.elevenlabs.io/v1/speech-to-text/realtime');
  url.searchParams.set('model_id',     ELEVENLABS_MODEL_ID);
  url.searchParams.set('audio_format', 'pcm_8000');   // LINEAR16 @ 8 kHz

  const ws = new WebSocket(url.toString(), {
    headers: { 'xi-api-key': ELEVENLABS_API_KEY },
  });

  ws.on('open', () => console.log(`[EL][${callId}][${leg}] connected`));

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw.toString()); } catch { return; }

    switch (msg.type) {
      case 'session_started':
        console.log(`[EL][${callId}][${leg}] session_started`);
        break;
      case 'partial_transcript':
        if (msg.text) {
          console.log(`[EL][${callId}][${leg}] PARTIAL: ${msg.text}`);
          onTranscript({ callId, leg, type: 'partial', text: msg.text });
        }
        break;
      case 'committed_transcript':
        if (msg.text) {
          console.log(`[EL][${callId}][${leg}] FINAL:   ${msg.text}`);
          onTranscript({ callId, leg, type: 'final', text: msg.text });
        }
        break;
      default:
        if (msg.error || String(msg.type).includes('error')) {
          console.error(`[EL][${callId}][${leg}] error:`, msg);
        }
    }
  });

  ws.on('error', (err) => console.error(`[EL][${callId}][${leg}] WS error:`, err.message));
  ws.on('close', (code) => console.log(`[EL][${callId}][${leg}] closed (${code})`));

  return ws;
}

function sendAudioChunk(ws, audioBytes) {
  if (!ws || ws.readyState !== WebSocket.OPEN) return;
  ws.send(JSON.stringify({
    type:        'input_audio_chunk',
    audio_chunk: Buffer.from(audioBytes).toString('base64'),
  }));
}

// ─── Transcript handler — extend this ────────────────────────────────────────

function handleTranscript({ callId, leg, type, text }) {
  // TODO: write to DB, POST to CRM/webhook, push to agent UI via Socket.IO, etc.
}

// ─── Trust token verification ─────────────────────────────────────────────────

function verifyTrustToken(metadata) {
  const values = metadata.get('x-five9-trust-token');
  if (!values || values.length === 0) {
    console.warn('[GR] Missing x-five9-trust-token header');
    return false;
  }
  if (values[0] !== FIVE9_TRUST_TOKEN) {
    console.warn('[GR] Invalid trust token received');
    return false;
  }
  return true;
}

function extractCallIdFromMetadata(metadata) {
  const values = metadata.get('x-five9-call-id');
  return (values && values.length > 0) ? values[0] : null;
}

// ─── Proto discovery helper ───────────────────────────────────────────────────

function logDiscovery(label, obj) {
  const cleaned = JSON.parse(JSON.stringify(obj, (k, v) => {
    if (k === 'payload' && Buffer.isBuffer(v)) return `<Buffer ${v.length} bytes>`;
    if (typeof v === 'string' && v.length > 200) return `<string ${v.length} chars>`;
    return v;
  }));
  console.log(`\n📦 [DISCOVERY] ${label}:\n`, JSON.stringify(cleaned, null, 2), '\n');
}

// ─── gRPC service implementation ──────────────────────────────────────────────

function startStream(call) {
  let callId  = extractCallIdFromMetadata(call.metadata);
  let leg     = null;
  let ws      = null;
  let sessionKey = null;

  if (!verifyTrustToken(call.metadata)) {
    console.warn('[GR] Rejected — bad/missing trust token');
    call.destroy(new Error('Unauthorized'));
    return;
  }

  call.on('data', (request) => {

    if (DISCOVERY_MODE) {
      logDiscovery('incoming message', request);
      call.write({ call_id: callId || 'unknown', status: 'OK', message: 'discovery-ack' });
      return;
    }

    const eventType = request.event;

    if (eventType === 'start') {
      const s = request.start;
      callId = s.vcc_call_id || s.call_id || callId || 'unknown';
      leg    = (s.call_leg || s.callLeg || s.channel || 'unknown').toLowerCase();
      sessionKey = `${callId}:${leg}`;

      console.log(
        `[GR][${callId}][${leg}] StreamStart` +
        `  domain=${s.domain_id || s.domainId}` +
        `  agent=${s.agent_id || s.agentId}` +
        `  encoding=${s.encoding}  rate=${s.sample_per_second || s.sampleRate}`
      );

      ws = openElevenLabsSocket({ callId, leg, onTranscript: handleTranscript });
      sessions.set(sessionKey, { ws, callId, leg });
      call.write({ call_id: callId, status: 'OK', message: 'Stream started' });
    }

    else if (eventType === 'media') {
      if (!ws) return;
      sendAudioChunk(ws, request.media.payload);
    }

    else if (eventType === 'stop') {
      const id  = request.stop?.vcc_call_id || request.stop?.call_id || callId;
      const key = sessionKey || `${id}:${leg}`;
      console.log(`[GR][${id}][${leg}] StreamStop  reason=${request.stop?.reason}`);
      cleanupSession(key);
      call.end();
    }
  });

  call.on('end',   () => { if (sessionKey) cleanupSession(sessionKey); call.end(); });
  call.on('error', (err) => {
    console.error(`[GR][${callId}][${leg}] error:`, err.message);
    if (sessionKey) cleanupSession(sessionKey);
  });
}

function cleanupSession(key) {
  const s = sessions.get(key);
  if (!s) return;
  if (s.ws?.readyState === WebSocket.OPEN) s.ws.close();
  sessions.delete(key);
  console.log(`[GR][${s.callId}][${s.leg}] session cleaned up`);
}

// ─── gRPC server (binds on loopback only — mux forwards to it) ───────────────

function buildServerCredentials() {
  if (GRPC_TLS === 'true') {
    if (!TLS_CERT_PATH || !TLS_KEY_PATH) throw new Error('GRPC_TLS=true but TLS_CERT_PATH/TLS_KEY_PATH not set');
    return grpc.ServerCredentials.createSsl(null, [{
      cert_chain:  fs.readFileSync(TLS_CERT_PATH),
      private_key: fs.readFileSync(TLS_KEY_PATH),
    }], false);
  }
  return grpc.ServerCredentials.createInsecure();
}

const grpcServer = new grpc.Server();
grpcServer.addService(proto.VoiceStreamService.service, { StartStream: startStream });

grpcServer.bindAsync(`127.0.0.1:${GRPC_INT_PORT}`, buildServerCredentials(), (err, port) => {
  if (err) { console.error('gRPC bind error:', err); process.exit(1); }
  console.log(`gRPC  server bound on loopback :${port}`);
});

// ─── Express HTTP server ──────────────────────────────────────────────────────

const app = express();
app.use(express.json());

// Subscription validation — Five9 GETs this before saving the subscription
app.get('/call-events', (_req, res) => {
  res.json(TRUST_TOKEN_SHA256);
});

// CTI Call events
app.post('/call-events', (req, res) => {
  if (FIVE9_API_KEY) {
    const incoming = req.headers['x-f9-apikey'];
    if (incoming !== FIVE9_API_KEY) {
      console.warn('[HTTP] CTI event rejected — bad/missing X-F9-APIKEY');
      return res.status(401).json({ error: 'Unauthorized' });
    }
  }
  console.log('[HTTP] CTI CallEvent:', JSON.stringify(req.body, null, 2));
  res.json({ received: true });
});

// Stream Status events
app.post('/subscriptions/:subscriptionId', (req, res) => {
  const { subscriptionId } = req.params;
  const { callId, callLeg, status, failureType, failureDetail } = req.body;
  console.log(
    `[HTTP] StreamStatus [sub=${subscriptionId}] callId=${callId} leg=${callLeg} status=${status}` +
    (failureType ? ` failureType=${failureType} detail=${failureDetail}` : '')
  );
  res.json({ received: true });
});

app.get('/health', (_req, res) => {
  const uniqueCalls = new Set([...sessions.values()].map(s => s.callId)).size;
  res.json({ status: 'ok', activeSessions: sessions.size, activeCallCount: uniqueCalls });
});

const httpServer = http.createServer(app);

// ─── TCP mux — routes HTTP/2 (gRPC) and HTTP/1.1 on the same port ────────────
//
// HTTP/2 connections begin with the client preface: "PRI * HTTP/2.0\r\n..."
// whose first three bytes are 0x50 0x52 0x49 ("PRI").
// Everything else is HTTP/1.1 and goes to Express.

const muxServer = net.createServer((socket) => {
  socket.once('data', (head) => {
    const isHttp2 = head[0] === 0x50 && head[1] === 0x52 && head[2] === 0x49;

    if (isHttp2) {
      // gRPC (HTTP/2) — proxy to internal gRPC server
      const grpcSocket = net.connect(Number(GRPC_INT_PORT), '127.0.0.1', () => {
        grpcSocket.write(head);
        socket.pipe(grpcSocket);
        grpcSocket.pipe(socket);
      });
      grpcSocket.on('error', (err) => {
        console.error('[MUX] gRPC proxy error:', err.message);
        socket.destroy();
      });
    } else {
      // HTTP/1.1 — hand to Express
      httpServer.emit('connection', socket);
      socket.unshift(head);
    }

    socket.on('error', (err) => {
      if (err.code !== 'ECONNRESET') console.error('[MUX] socket error:', err.message);
    });
  });
});

muxServer.listen(Number(PORT), () => {
  console.log(`Mux   server listening on :${PORT} (HTTP/1.1 + gRPC/HTTP/2)`);
  console.log(`  Subscription validation → GET  http://HOST:${PORT}/call-events`);
  console.log(`  CTI Events             → POST http://HOST:${PORT}/call-events`);
  console.log(`  Stream Status          → POST http://HOST:${PORT}/subscriptions/{subscriptionId}`);
  console.log(`  Health                 → GET  http://HOST:${PORT}/health`);
});

process.on('SIGTERM', () => {
  for (const [key] of sessions) cleanupSession(key);
  grpcServer.forceShutdown();
  muxServer.close();
  process.exit(0);
});
