# Five9 VoiceStream → ElevenLabs Bridge

Bridges **Five9 VoiceStream** (gRPC) to **ElevenLabs real-time speech-to-text** (WebSocket), producing live transcripts of both caller and agent audio.

---

## Architecture

```
Five9 ──gRPC (agent leg)──▶ ──────────────────────────────▶ ElevenLabs STT
Five9 ──gRPC (customer leg)▶ Bridge Server ──WebSocket──────▶ ElevenLabs STT
                                   │
                           HTTP webhooks
                      ┌────────────┴────────────┐
               Stream Status            CTI Events
```

**Important:** Five9 does NOT multiplex channels on one gRPC connection.
It opens **two separate gRPC sessions per call** — one for the agent leg, one for the customer leg.
Both sessions share the same `vccCallId` so you can correlate them.
Your server opens **one ElevenLabs WebSocket per gRPC session**.

---

## Five9 Subscription Settings

Fill in the **Create Subscription** form as follows:

| Field | Value |
|---|---|
| Subscription Name | `Audio to ElevenLabs` |
| Streaming Type | **Voice streaming** |
| Streaming Destination | **Other (Self Service)** |
| Streaming Protocol | **gRPC** |
| Primary Streaming Destination | `your-server.com:443` |
| CTI Call Event Destination | `https://your-server.com/call-events` |
| MediaStream Event Destination | `https://your-server.com/subscriptions/{subscriptionId}` |
| Trust Token | *(your `FIVE9_TRUST_TOKEN` value)* |
| Enable third-party audio streaming | ✅ **Checked** |
| Enable audio streaming for agent-only conference calls | ✅ Checked |
| Streaming Filter | **All calls** |

> **Before saving the subscription:** Five9 sends an HTTP **GET** to your CTI Call Event
> Destination URL to validate it. Your server must already be running and publicly reachable.
> It must respond with the **SHA-256 hash of your trust token** as a JSON string.
> The server handles this automatically at `GET /call-events`.

> **After saving:** Five9 shows your **API Key** once. Copy it immediately and set
> `FIVE9_API_KEY` in your `.env`. It authenticates all CTI webhook POSTs via the
> `X-F9-APIKEY` header.

> **Production requirement:** Five9 requires TLS (port 443) for the gRPC Streaming Destination.
> Use nginx, Cloud Run, or another TLS-terminating reverse proxy in front of the gRPC port.

---

## Quick Start

### 1. Install dependencies
```bash
npm install
```

### 2. Configure environment
```bash
cp .env.example .env
# Edit .env with your ElevenLabs API key, trust token, etc.
```

### 3. Get the official Five9 proto file
Download from your Five9 developer portal and replace `five9_voice_stream.proto`:
```
https://webapps.five9.com/assets/files/for_customers/documentation/voicestream/grpc_voice_proto.zip
```
The included proto mirrors the documented contract. If field names differ, update `server.js` accordingly
or use PROTO_DISCOVERY mode (see below).

### 4. Run
```bash
npm start
```

---

## Production Deployment (Cloud Run example)

```dockerfile
FROM node:20-slim
WORKDIR /app
COPY package*.json ./
RUN npm ci --omit=dev
COPY . .
ENV GRPC_TLS=false   # Cloud Run terminates TLS; gRPC passes through plaintext
EXPOSE 50051 3000
CMD ["node", "server.js"]
```

Cloud Run routes port 443 → your container. Set all env vars as Cloud Run secrets.

---

## Debugging Proto Field Names (PROTO_DISCOVERY mode)

If Five9's actual proto field names differ from this repo's `five9_voice_stream.proto`,
enable discovery mode to log raw incoming messages:

```bash
PROTO_DISCOVERY=true npm start
```

Make a test call, then read the console output to see the exact field names Five9 sends.
Update `server.js` to match.

---

## gRPC Authentication

Five9 passes two values in gRPC metadata headers (not in the proto message body):

| Header | Description |
|---|---|
| `x-five9-trust-token` | Your subscription Trust Token — verified on every connection |
| `x-five9-call-id` | The Five9 call ID — available before the first data message |

---

## HTTP Endpoints

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/call-events` | Subscription validation — returns SHA-256 of trust token |
| `POST` | `/call-events` | CTI Call events — call start/end, transfers, agent info |
| `POST` | `/subscriptions/:id` | Stream Status events — STREAMING, ENDED_NORMALLY, ERROR |
| `GET` | `/health` | Health check + active session count |

### Stream Status Event payload
```json
{
  "callId": "600000000054602",
  "domainId": "domain-id",
  "callLeg": "Agent",
  "status": "STREAMING",
  "failureType": "",
  "failureDetail": "",
  "destinationUrl": "https://your-server.com/subscriptions/...",
  "occurredOn": "2025-12-12T01:15:37.917Z"
}
```
Status values: `STREAMING`, `ENDED_NORMALLY`, `ERROR`

---

## Extending Transcripts

In `server.js`, find `handleTranscript()` and add your logic:

```js
function handleTranscript({ callId, leg, type, text }) {
  // leg is "agent" or "customer"
  // type is "partial" or "final"
  // Examples:
  // await db.transcripts.insert({ callId, leg, type, text, ts: Date.now() });
  // await fetch('https://your-crm.com/transcripts', { method: 'POST', body: JSON.stringify({...}) });
  // io.to(callId).emit('transcript', { leg, type, text });  // Socket.IO to agent UI
}
```

---

## Audio Format

Five9 gRPC streams send **LINEAR16** (signed 16-bit PCM, little-endian) at **8 kHz**.
The bridge forwards this to ElevenLabs using the `pcm_8000` audio format.

---

## Environment Variables

| Variable | Required | Description |
|---|---|---|
| `ELEVENLABS_API_KEY` | ✅ | ElevenLabs API key |
| `ELEVENLABS_MODEL_ID` | | Default: `scribe_v2_realtime` |
| `FIVE9_TRUST_TOKEN` | ✅ | Matches what you enter in Five9 subscription |
| `FIVE9_API_KEY` | Recommended | Generated by Five9 after subscription creation; validates CTI webhook POSTs |
| `GRPC_PORT` | | gRPC listen port (default `50051`) |
| `HTTP_PORT` | | HTTP webhook port (default `3000`) |
| `GRPC_TLS` | | `true` to handle TLS directly; `false` for reverse-proxy TLS |
| `PROTO_DISCOVERY` | | `true` to log raw gRPC messages for field name discovery |

---

## IP Ranges to Allow

Five9 gRPC audio traffic originates from these IP ranges:

| Region | Source IP Range |
|---|---|
| US | 147.124.160.0/19 |
| EU / UK | 147.189.224.0/20, 147.124.160.0/19 |
| JP / AU / IN | 202.92.198.0/23 |
| BR | 34.39.142.160/31 |

CTI Events and Stream Status webhooks can originate from anywhere (0.0.0.0/0). Use the `X-F9-APIKEY` header to authenticate them instead of IP allowlisting.
