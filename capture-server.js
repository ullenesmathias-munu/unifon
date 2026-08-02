// capture-server.js
// Local Unifon queue-presence capture.
// Polls /queue/summary every N seconds, detects when agents go off/on a queue,
// and serves the results at http://localhost:8787/events.json for the dashboard to read.
//
// Requires Node.js 18+ (uses the built-in fetch — no npm install needed).

const fs = require('fs');
const path = require('path');
const http = require('http');

// Any truly unexpected error exits cleanly with a readable message instead of
// a raw stack trace, so start-windows.bat's restart loop can pick it back up
// rather than the capture silently stopping until someone notices the window.
process.on('uncaughtException', (err) => {
  console.error('Unexpected error, restarting:', err);
  process.exit(1);
});
process.on('unhandledRejection', (err) => {
  console.error('Unexpected rejection, restarting:', err);
  process.exit(1);
});

const CONFIG_PATH = path.join(__dirname, 'config.json');
if (!fs.existsSync(CONFIG_PATH)) {
  console.error('Missing config.json — copy config.example.json to config.json and fill in your Unifon credentials.');
  process.exit(1);
}
const config = JSON.parse(fs.readFileSync(CONFIG_PATH, 'utf8'));
const {
  client_id,
  client_secret,
  port         = 8787,
  poll_seconds = 20,
  wrapup_seconds = 90,  // max off-time (seconds) to classify as post-call wrapup
} = config;

if (!client_id || !client_secret) {
  console.error('config.json is missing client_id or client_secret.');
  process.exit(1);
}

const EVENTS_PATH = path.join(__dirname, 'events.json');
let events = [];
try {
  if (fs.existsSync(EVENTS_PATH)) events = JSON.parse(fs.readFileSync(EVENTS_PATH, 'utf8'));
} catch (e) {
  console.warn('Could not read existing events.json, starting fresh.');
}

const TOKEN_PATH = path.join(__dirname, 'token.json');
let token = null;
let tokenExpiresAt = 0;

// Load a previously saved token if it's still valid
try {
  const saved = JSON.parse(fs.readFileSync(TOKEN_PATH, 'utf8'));
  if (saved.access_token && saved.expires_at && saved.expires_at > Date.now() + 60000) {
    token = saved.access_token;
    tokenExpiresAt = saved.expires_at;
    const hoursLeft = Math.round((tokenExpiresAt - Date.now()) / 3600000);
    console.log(`Reusing saved token, ~${hoursLeft}h remaining`);
  }
} catch (e) { /* no saved token — will fetch on first poll */ }

async function getToken() {
  if (token && Date.now() < tokenExpiresAt - 60000) return token;
  const res = await fetch('https://bnapi.unifon.no/bnapi/v1/session/token', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ client_id, client_secret, grant_type: 'client_credentials' })
  });
  if (!res.ok) {
    const body = await res.text().catch(() => '');
    throw new Error(`Token request failed: ${res.status} ${body}`);
  }
  const data = await res.json();
  token = data.access_token;
  tokenExpiresAt = Date.now() + (data.expires_in || 86400) * 1000;
  fs.writeFileSync(TOKEN_PATH, JSON.stringify({ access_token: token, expires_at: tokenExpiresAt }));
  console.log('Got a new access token, valid ~' + Math.round((data.expires_in || 86400) / 3600) + 'h');
  return token;
}

// Tracks current ready state per agent+queue.
// key = catalog_id + '|' + queue_id
// value = { ready: bool, since: timestamp (ms) using device.changed for precision }
const state = new Map();

// Buffers "out" events until we know whether the off-period is wrapup or a real absence.
// key = catalog_id + '|' + queue_id
// value = { emitted: bool }
// The event itself is reconstructed from state.since when needed.
const pendingOut = new Map();

// Persist state/pendingOut across restarts so a login that happened while this
// process wasn't running (computer off, terminal closed, etc.) still gets logged
// with its real timestamp on the next poll, instead of being silently adopted as
// a fresh baseline. Relies on the Unifon API's own device.changed timestamp being
// accurate regardless of how long we were away — the same field already used for
// precise durations between two ordinary polls.
const STATE_PATH = path.join(__dirname, 'state.json');
try {
  if (fs.existsSync(STATE_PATH)) {
    const saved = JSON.parse(fs.readFileSync(STATE_PATH, 'utf8'));
    for (const [k, v] of Object.entries(saved.state || {})) state.set(k, v);
    for (const [k, v] of Object.entries(saved.pendingOut || {})) pendingOut.set(k, v);
    console.log(`Restored presence state for ${state.size} agent/queue pair(s) from last run.`);
  }
} catch (e) {
  console.warn('Could not read existing state.json, starting fresh.');
}

// Last raw API response — served at /debug.json so you can inspect available fields
let lastRawResponse = null;

// Current queue snapshot — served at /status.json for the live wallboard
let currentStatus = null;

function saveEvents() {
  const cutoff = Date.now() - 30 * 24 * 3600 * 1000; // keep 30 days
  events = events.filter(e => e.ts >= cutoff);
  fs.writeFileSync(EVENTS_PATH, JSON.stringify(events));
}

function saveState() {
  fs.writeFileSync(STATE_PATH, JSON.stringify({
    state: Object.fromEntries(state),
    pendingOut: Object.fromEntries(pendingOut),
  }));
}

async function poll() {
  try {
    const tok = await getToken();
    const res = await fetch('https://bnapi.unifon.no/bnapi/v1/queue/summary', {
      headers: { Authorization: 'Bearer ' + tok }
    });
    if (!res.ok) {
      console.error('queue/summary failed:', res.status, await res.text().catch(() => ''));
      return;
    }
    const data = await res.json();
    lastRawResponse = data;
    const now = Date.now();

    // Build the status snapshot for the live wallboard
    currentStatus = {
      ts: now,
      queues: (data.entries || []).map(q => {
        const members = (q.members || []).filter(m => m.mobile || m.fixed);
        const getName = m => [m.firstname, m.lastname].filter(Boolean).join(' ') || m.catalog_id;
        const readyMembers   = members.filter(m => !!((m.mobile && m.mobile.ready) || (m.fixed && m.fixed.ready)));
        const offlineMembers = members.filter(m => !((m.mobile && m.mobile.ready) || (m.fixed && m.fixed.ready)));
        return {
          queue_id:          q.queue_id,
          description:       q.description,
          ready:             readyMembers.length,
          total:             members.length,
          answered_waittime: (q.keyfigures || {}).answered_waittime || 0,
          ready_agents:      readyMembers.map(getName),
          offline_agents:    offlineMembers.map(getName),
        };
      }),
    };

    for (const q of (data.entries || [])) {
      for (const m of (q.members || [])) {
        const name = [m.firstname, m.lastname].filter(Boolean).join(' ') || m.catalog_id;
        const device = m.mobile || m.fixed;
        if (!device) continue;

        const ready = !!device.ready;

        // Use device.changed for precise event timestamps instead of poll time.
        const changedTs = device.changed ? new Date(device.changed).getTime() : now;

        const key = m.catalog_id + '|' + q.queue_id;
        const prev = state.get(key);

        if (!prev) {
          state.set(key, { ready, since: changedTs });
          continue;
        }

        if (prev.ready !== ready) {
          if (ready) {
            // Agent came back ready — decide: wrapup or genuine absence?
            const offSeconds = (changedTs - prev.since) / 1000;
            const pending = pendingOut.get(key);

            if (offSeconds <= wrapup_seconds) {
              // Short off-period: this is post-call wrapup.
              // Suppress the buffered "out" event entirely — not a real absence.
              // Flag if it ran meaningfully longer than the configured 1-minute auto-wrapup.
              const flagged = offSeconds > 75;
              events.push({
                ts: changedTs,
                name,
                queue: q.description,
                direction: 'wrapup_end',
                wrapupMinutes: offSeconds / 60,
                flagged,
              });
              console.log(`Wrapup: ${name} — ${offSeconds.toFixed(0)}s${flagged ? ' (extended)' : ''}`);
            } else {
              // Genuine absence: emit the buffered out event (if not already emitted) then in.
              if (pending && !pending.emitted) {
                events.push({ ts: prev.since, name, queue: q.description, direction: 'out' });
              }
              const offMinutes = Math.round(offSeconds / 60);
              events.push({ ts: changedTs, name, queue: q.description, direction: 'in', offMinutes });
              console.log(`Back: ${name} after ${offMinutes}m absence`);
            }

            pendingOut.delete(key);
            state.set(key, { ready, since: changedTs });

          } else {
            // Agent went not-ready: buffer the out event.
            // We don't emit it yet — it might be wrapup.
            pendingOut.set(key, { emitted: false });
            state.set(key, { ready, since: changedTs });
          }

        } else if (!ready && pendingOut.has(key)) {
          // Agent is still not-ready from a previous poll.
          // If they've been off longer than the wrapup threshold, emit the out event now.
          const pending = pendingOut.get(key);
          if (!pending.emitted && (now - prev.since) > wrapup_seconds * 1000) {
            events.push({ ts: prev.since, name, queue: q.description, direction: 'out' });
            pending.emitted = true;
            pendingOut.set(key, pending);
            console.log(`Off queue: ${name}`);
          }
        }
      }
    }
    saveEvents();
    saveState();
  } catch (err) {
    console.error('poll error:', err.message);
  }
}

setInterval(poll, poll_seconds * 1000);
poll();

const server = http.createServer((req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.url.startsWith('/events.json')) {
    res.setHeader('Content-Type', 'application/json');
    res.end(JSON.stringify(events));
  } else if (req.url.startsWith('/status.json')) {
    res.setHeader('Content-Type', 'application/json');
    res.end(JSON.stringify(currentStatus || {}));
  } else if (req.url.startsWith('/debug.json')) {
    res.setHeader('Content-Type', 'application/json');
    res.end(JSON.stringify(lastRawResponse, null, 2));
  } else {
    res.writeHead(404);
    res.end('Not found');
  }
});

server.on('error', (err) => {
  if (err.code === 'EADDRINUSE') {
    console.error(`\nPort ${port} is already in use — is capture-server.js already running in another window?`);
    console.error('Close the other instance first, or change "port" in config.json.\n');
  } else {
    console.error('Server error:', err.message);
  }
  process.exit(1);
});

server.listen(port, () => {
  console.log(`Capture running. Serving events at http://localhost:${port}/events.json`);
  console.log(`Wrapup threshold: ${wrapup_seconds}s — off-periods shorter than this are classified as post-call wrapup`);
  console.log('Leave this window open while you want data captured. Close it to stop.');
});
