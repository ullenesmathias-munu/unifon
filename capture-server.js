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
  wrapup_seconds = 90,   // max off-time (seconds) to classify as post-call wrapup
  calls_poll_seconds = 4, // live calls change fast, so this polls independently of poll_seconds
} = config;

if (!client_id || !client_secret) {
  console.error('config.json is missing client_id or client_secret.');
  process.exit(1);
}

// Queues that exist in Unifon but aren't relevant to scheduling/reporting here.
const EXCLUDED_QUEUES = ['overflow'];
function isExcludedQueue(description) {
  return EXCLUDED_QUEUES.includes(String(description || '').trim().toLowerCase());
}

const SCHEDULE_PATH = path.join(__dirname, 'schedule.json');
let schedule = {};
try {
  if (fs.existsSync(SCHEDULE_PATH)) schedule = JSON.parse(fs.readFileSync(SCHEDULE_PATH, 'utf8'));
} catch (e) {
  console.warn('Could not read existing schedule.json, starting fresh.');
}

// Names hidden from the dashboard's schedule + login history (e.g. short-term
// helpers) — kept separate from events.json so hiding someone never touches
// their captured data, and unhiding brings their history straight back.
const HIDDEN_AGENTS_PATH = path.join(__dirname, 'hidden-agents.json');
let hiddenAgents = [];
try {
  if (fs.existsSync(HIDDEN_AGENTS_PATH)) hiddenAgents = JSON.parse(fs.readFileSync(HIDDEN_AGENTS_PATH, 'utf8'));
} catch (e) {
  console.warn('Could not read existing hidden-agents.json, starting fresh.');
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

// Live call/agent state — served at /calls.json. Comes from Unifon's own
// wallboard GraphQL API (graphql.unifon.no), not the REST queue/summary
// endpoint above, since that one has no call- or busy-state data at all. The
// same client_credentials token already works against it. Deliberately never
// requests callerid or the agent's own phone number ("user") — this is a
// presence indicator, not a call log.
let currentCalls = null;

// Per-agent answered-call totals for "this week so far" — served at
// /agent-stats.json. Comes straight from Unifon's own AgentReport query (the
// same report the agents-report .xlsx export is generated from), so it's
// Unifon's authoritative count, not something inferred from state polling.
let currentAgentStats = null;

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
    const entries = (data.entries || []).filter(q => !isExcludedQueue(q.description));

    // Build the status snapshot for the live wallboard
    currentStatus = {
      ts: now,
      queues: entries.map(q => {
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

    for (const q of entries) {
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

const LIVE_CALLS_QUERY = `{
  LiveAgentOverview(queue: []) {
    user_name
    queue_id
    queue_name
    state
  }
  LiveQueueCall(queue: []) {
    id
    date_start
    date_answer
    queue_id
    queue_name
    state
  }
}`;

async function pollCalls() {
  try {
    const tok = await getToken();
    const res = await fetch('https://graphql.unifon.no/graphql', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Authorization: 'Bearer ' + tok },
      body: JSON.stringify({ query: LIVE_CALLS_QUERY })
    });
    if (!res.ok) {
      console.error('live-calls poll failed:', res.status, await res.text().catch(() => ''));
      return;
    }
    const { data } = await res.json();
    const now = Date.now();

    const agents = (data.LiveAgentOverview || [])
      .filter(a => !isExcludedQueue(a.queue_name))
      .map(a => ({ name: a.user_name, queue: a.queue_name, onCall: a.state !== 'idle' }));

    // date_answer is the zero value ("0001-01-01T00:00:00Z") until the call is
    // picked up — that's how a still-waiting call is told apart from an active one.
    const calls = (data.LiveQueueCall || [])
      .filter(c => !isExcludedQueue(c.queue_name))
      .map(c => {
        const answered = !!c.date_answer && !c.date_answer.startsWith('0001-01-01');
        const sinceTs = new Date(answered ? c.date_answer : c.date_start).getTime();
        return {
          id: c.id,
          queue: c.queue_name,
          answered,
          seconds: Math.max(0, Math.round((now - sinceTs) / 1000)),
        };
      });

    currentCalls = { ts: now, agents, calls };
  } catch (err) {
    console.error('live-calls poll error:', err.message);
  }
}

setInterval(pollCalls, calls_poll_seconds * 1000);
pollCalls();

// Monday 00:00 local time of the current week.
function mondayOf(ts) {
  const d = new Date(ts);
  d.setHours(0, 0, 0, 0);
  const day = d.getDay(); // 0=Sun..6=Sat
  d.setDate(d.getDate() + (day === 0 ? -6 : 1 - day));
  return d;
}

async function pollAgentStats() {
  try {
    const tok = await getToken();
    const from = mondayOf(Date.now()).toISOString();
    const to = new Date().toISOString();
    const query = `{
      AgentReport(date_from: "${from}", date_to: "${to}") {
        user_name
        queue_name
        queue_calls_answer
      }
    }`;
    const res = await fetch('https://graphql.unifon.no/graphql', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Authorization: 'Bearer ' + tok },
      body: JSON.stringify({ query })
    });
    if (!res.ok) {
      console.error('agent-report poll failed:', res.status, await res.text().catch(() => ''));
      return;
    }
    const { data } = await res.json();
    const totals = new Map(); // name -> answered count, summed across queues
    for (const r of (data.AgentReport || [])) {
      if (isExcludedQueue(r.queue_name)) continue;
      totals.set(r.user_name, (totals.get(r.user_name) || 0) + (r.queue_calls_answer || 0));
    }
    currentAgentStats = {
      ts: Date.now(),
      weekStart: from,
      agents: Array.from(totals, ([name, count]) => ({ name, count })),
    };
  } catch (err) {
    console.error('agent-report poll error:', err.message);
  }
}

// This is a running weekly total, not something that needs 4-second freshness
// like the live call state above, so it polls on the same cadence as the main
// queue/summary poll.
setInterval(pollAgentStats, poll_seconds * 1000);

const server = http.createServer((req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

  if (req.method === 'OPTIONS') {
    res.writeHead(204);
    res.end();
    return;
  }

  if (req.url.startsWith('/events.json')) {
    res.setHeader('Content-Type', 'application/json');
    res.end(JSON.stringify(events));
  } else if (req.url.startsWith('/status.json')) {
    res.setHeader('Content-Type', 'application/json');
    res.end(JSON.stringify(currentStatus || {}));
  } else if (req.url.startsWith('/calls.json')) {
    res.setHeader('Content-Type', 'application/json');
    res.end(JSON.stringify(currentCalls || { ts: Date.now(), agents: [], calls: [] }));
  } else if (req.url.startsWith('/agent-stats.json')) {
    res.setHeader('Content-Type', 'application/json');
    res.end(JSON.stringify(currentAgentStats || { ts: Date.now(), agents: [] }));
  } else if (req.url.startsWith('/debug.json')) {
    res.setHeader('Content-Type', 'application/json');
    res.end(JSON.stringify(lastRawResponse, null, 2));
  } else if (req.url.startsWith('/schedule.json') && req.method === 'GET') {
    res.setHeader('Content-Type', 'application/json');
    res.end(JSON.stringify(schedule));
  } else if (req.url.startsWith('/schedule.json') && req.method === 'POST') {
    let body = '';
    req.on('data', chunk => {
      body += chunk;
      if (body.length > 1e6) req.destroy(); // guard against runaway/malicious bodies
    });
    req.on('end', () => {
      try {
        schedule = JSON.parse(body);
        fs.writeFileSync(SCHEDULE_PATH, JSON.stringify(schedule));
        res.setHeader('Content-Type', 'application/json');
        res.end(JSON.stringify({ ok: true }));
      } catch (e) {
        res.writeHead(400);
        res.end('Invalid JSON');
      }
    });
  } else if (req.url.startsWith('/hidden-agents.json') && req.method === 'GET') {
    res.setHeader('Content-Type', 'application/json');
    res.end(JSON.stringify(hiddenAgents));
  } else if (req.url.startsWith('/hidden-agents.json') && req.method === 'POST') {
    let body = '';
    req.on('data', chunk => {
      body += chunk;
      if (body.length > 1e6) req.destroy(); // guard against runaway/malicious bodies
    });
    req.on('end', () => {
      try {
        const parsed = JSON.parse(body);
        if (!Array.isArray(parsed)) throw new Error('expected an array of names');
        hiddenAgents = parsed;
        fs.writeFileSync(HIDDEN_AGENTS_PATH, JSON.stringify(hiddenAgents));
        res.setHeader('Content-Type', 'application/json');
        res.end(JSON.stringify({ ok: true }));
      } catch (e) {
        res.writeHead(400);
        res.end('Invalid JSON');
      }
    });
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
