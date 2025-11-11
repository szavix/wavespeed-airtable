// server.mjs
import express from "express";
import crypto from "crypto";
import { setTimeout as delay } from "timers/promises";

// Node >=18 has global fetch
const {
  PORT = 3000,
  PUBLIC_BASE_URL,                 // e.g. https://your-app.onrender.com
  WAVESPEED_API_KEY,
  AIRTABLE_TOKEN,
  AIRTABLE_BASE_ID,
  AIRTABLE_TABLE = "Generations",
} = process.env;

if (!PUBLIC_BASE_URL || !WAVESPEED_API_KEY || !AIRTABLE_TOKEN || !AIRTABLE_BASE_ID) {
  console.error("Missing required env: PUBLIC_BASE_URL, WAVESPEED_API_KEY, AIRTABLE_TOKEN, AIRTABLE_BASE_ID");
  process.exit(1);
}

const app = express();
app.use(express.json({ limit: "10mb" }));
app.use(express.urlencoded({ extended: true }));

// ------------------------------
// Helpers
// ------------------------------
const WS_BASE = "https://api.wavespeed.ai/api/v3";
const WS_EDIT_ENDPOINT = `${WS_BASE}/bytedance/seedream-v4/edit`; // image-to-image "Edit" accepts images[] :contentReference[oaicite:1]{index=1}
const WS_RESULT_ENDPOINT = (id) => `${WS_BASE}/predictions/${encodeURIComponent(id)}/result`; // polling endpoint :contentReference[oaicite:2]{index=2}

const AT_BASE = `https://api.airtable.com/v0/${AIRTABLE_BASE_ID}`;

const headersWave = {
  "Content-Type": "application/json",
  "Authorization": `Bearer ${WAVESPEED_API_KEY}`,
};
const headersAT = {
  "Content-Type": "application/json",
  "Authorization": `Bearer ${AIRTABLE_TOKEN}`,
};

const nowISO = () => new Date().toISOString();

function uniqueCsvJoin(arr) {
  return Array.from(new Set((arr || []).filter(Boolean))).join(",");
}
function csvToSet(csv) {
  return new Set((csv || "").split(",").map(s => s.trim()).filter(Boolean));
}

// Fetch a URL and return a data: URI (base64), preserving content-type
async function urlToDataUrl(url) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`Fetch failed ${res.status}: ${url}`);
  const ct = res.headers.get("content-type") || "application/octet-stream";
  const buf = Buffer.from(await res.arrayBuffer());
  const b64 = buf.toString("base64");
  return `data:${ct};base64,${b64}`;
}

// Airtable primitives
async function airtableCreateBatchRow({
  prompt, subjectUrl, referenceUrls, sizeText, model, runId,
}) {
  const body = {
    fields: {
      "Prompt": prompt || "",
      "Subject": subjectUrl ? [{ url: subjectUrl }] : [],
      "References": (referenceUrls || []).map(u => ({ url: u })),
      "Output": [],
      "Output URL": "",
      "Model": model || "bytedance/seedream-v4/edit",
      "Size": sizeText || "",
      "Request IDs": "",
      "Seen IDs": "",
      "Failed IDs": "",
      "Status": "processing",
      "Run ID": runId,
      "Created At": nowISO(),
      "Last Update": nowISO(),
    },
  };
  const resp = await fetch(`${AT_BASE}/${encodeURIComponent(AIRTABLE_TABLE)}`, {
    method: "POST",
    headers: headersAT,
    body: JSON.stringify(body),
  });
  if (!resp.ok) throw new Error(`Airtable create failed: ${resp.status} ${await resp.text()}`);
  const json = await resp.json();
  return json.id;
}

async function airtableGetRow(id) {
  const resp = await fetch(`${AT_BASE}/${encodeURIComponent(AIRTABLE_TABLE)}/${id}`, {
    headers: headersAT,
  });
  if (!resp.ok) throw new Error(`Airtable get failed: ${resp.status}`);
  return await resp.json();
}

async function airtableUpdateRow(id, fieldsPatch) {
  const body = { fields: { ...fieldsPatch, "Last Update": nowISO() } };
  const resp = await fetch(`${AT_BASE}/${encodeURIComponent(AIRTABLE_TABLE)}/${id}`, {
    method: "PATCH",
    headers: headersAT,
    body: JSON.stringify(body),
  });
  if (!resp.ok) throw new Error(`Airtable update failed: ${resp.status} ${await resp.text()}`);
  return await resp.json();
}

async function airtableAppendRequestId(parentId, requestId) {
  const row = await airtableGetRow(parentId);
  const before = row.fields["Request IDs"] || "";
  const csv = uniqueCsvJoin([ ...csvToSet(before), requestId ]);
  await airtableUpdateRow(parentId, { "Request IDs": csv });
}

async function airtableMarkSeen(parentId, requestId, { failed = false } = {}) {
  const row = await airtableGetRow(parentId);
  const seen = uniqueCsvJoin([ ...csvToSet(row.fields["Seen IDs"]), requestId ]);
  const failedCsv = failed
    ? uniqueCsvJoin([ ...csvToSet(row.fields["Failed IDs"]), requestId ])
    : row.fields["Failed IDs"] || "";

  let status = row.fields["Status"] || "processing";
  // Flip to completed if Seen == Request setwise (even if some failed)
  const reqSet = csvToSet(row.fields["Request IDs"]);
  const seenSet = csvToSet(seen);
  const allSeen = reqSet.size > 0 && reqSet.size === seenSet.size && [...reqSet].every(x => seenSet.has(x));
  let patch = { "Seen IDs": seen, "Failed IDs": failedCsv };

  if (allSeen && status !== "completed") {
    patch["Status"] = "completed";
    patch["Completed At"] = nowISO();
  }
  await airtableUpdateRow(parentId, patch);
}

async function airtableAppendOutputs(parentId, urls) {
  if (!urls?.length) return;
  const row = await airtableGetRow(parentId);
  const outputs = (row.fields["Output"] || []).map(a => ({ url: a.url }));
  const newAtts = urls.map(url => ({ url }));
  const firstUrl = (row.fields["Output URL"] && String(row.fields["Output URL"]).trim()) ? row.fields["Output URL"] : urls[0];
  await airtableUpdateRow(parentId, {
    "Output": [...outputs, ...newAtts],
    "Output URL": firstUrl,
  });
}

// WaveSpeed submit with retries & spacing handled externally
async function submitWavespeedJob({ prompt, images, width, height, webhookUrl }) {
  const size = `${Number(width)}*${Number(height)}`; // docs specify "2048*2048" style size :contentReference[oaicite:3]{index=3}
  const url = new URL(WS_EDIT_ENDPOINT);
  if (webhookUrl) url.searchParams.set("webhook", webhookUrl);

  const payload = {
    prompt,
    images,                        // subject first, refs after (caller ensures order)  :contentReference[oaicite:4]{index=4}
    size,
    enable_sync_mode: false,
    enable_base64_output: false,
  };

  let lastErr;
  for (let attempt = 1; attempt <= 3; attempt++) {
    try {
      const resp = await fetch(url, {
        method: "POST",
        headers: headersWave,
        body: JSON.stringify(payload),
      });
      if (!resp.ok) {
        const txt = await resp.text();
        throw new Error(`WS submit ${resp.status}: ${txt}`);
      }
      const json = await resp.json();
      // Expected response includes data.id and data.model; status created/processing :contentReference[oaicite:5]{index=5}
      const requestId = json?.data?.id;
      const model = json?.data?.model || "bytedance/seedream-v4/edit";
      if (!requestId) throw new Error("Missing requestId in response");
      return { requestId, model };
    } catch (err) {
      lastErr = err;
      const backoff = 500 * Math.pow(2, attempt - 1);
      await delay(backoff);
    }
  }
  throw lastErr;
}

// Polling with retries/backoff until terminal or timeout ~20m
async function pollUntilDone(requestId, parentId) {
  const started = Date.now();
  const timeoutMs = 20 * 60 * 1000;
  let intervalMs = 7000;

  const getOnce = async () => {
    let lastErr;
    for (let attempt = 1; attempt <= 3; attempt++) {
      try {
        const resp = await fetch(WS_RESULT_ENDPOINT(requestId), { headers: headersWave });
        if (!resp.ok) {
          const txt = await resp.text();
          throw new Error(`WS poll ${resp.status}: ${txt}`);
        }
        return await resp.json();
      } catch (e) {
        lastErr = e;
        await delay(600 * Math.pow(2, attempt - 1));
      }
    }
    throw lastErr;
  };

  while (Date.now() - started < timeoutMs) {
    try {
      const json = await getOnce();
      const status = json?.data?.status;
      if (status === "completed") {
        const outputs = json?.data?.outputs || [];
        if (outputs.length) await airtableAppendOutputs(parentId, outputs);
        await airtableMarkSeen(parentId, requestId, { failed: false });
        return;
      }
      if (status === "failed") {
        await airtableMarkSeen(parentId, requestId, { failed: true });
        return;
      }
      // else created/processing: keep polling
    } catch (err) {
      // swallow and continue polling with backoff bump (but cap)
      intervalMs = Math.min(15000, Math.floor(intervalMs * 1.2));
    }
    await delay(intervalMs);
  }
  // Timed out -> mark failed but still "seen"
  await airtableMarkSeen(parentId, requestId, { failed: true });
}

// ------------------------------
// Tiny UI at /app
// ------------------------------
app.get("/app", (_req, res) => {
  res.type("html").send(`<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <title>WaveSpeed Seedream v4 – Batch</title>
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <style>
    body { font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI; margin: 2rem; max-width: 800px; }
    label { display:block; margin-top: 0.75rem; font-weight: 600; }
    input, textarea { width: 100%; padding: .6rem; font: inherit; }
    button { margin-top: 1rem; padding: .7rem 1rem; font: inherit; cursor: pointer; }
    small { color:#555; }
  </style>
</head>
<body>
  <h1>Seedream v4 – Batch Runner</h1>
  <form method="POST" action="/start-batch">
    <label>Prompt
      <textarea name="prompt" rows="3" required placeholder="Describe the edit..."></textarea>
    </label>
    <label>Subject image URL
      <input type="url" name="subject" required placeholder="https://..." />
    </label>
    <label>Reference image URLs (comma-separated)
      <input type="text" name="refs" placeholder="https://a.jpg, https://b.png" />
    </label>
    <label>Width
      <input type="number" name="width" min="256" max="4096" value="2048" required />
    </label>
    <label>Height
      <input type="number" name="height" min="256" max="4096" value="2048" required />
    </label>
    <label>Batch count
      <input type="number" name="count" min="1" max="24" value="4" required />
    </label>
    <button type="submit">Start Batch</button>
    <p><small>Results will land in Airtable → ${AIRTABLE_TABLE}</small></p>
  </form>
</body>
</html>`);
});

// ------------------------------
// Batch start
// ------------------------------
app.post("/start-batch", async (req, res) => {
  try {
    const { prompt, subject, refs = "", width, height, count } = req.body;
    if (!prompt || !subject || !width || !height || !count) {
      return res.status(400).json({ error: "Missing prompt, subject, width, height, count" });
    }

    const w = Number(width), h = Number(height), n = Math.max(1, Math.min(100, Number(count)));
    const referenceUrls = refs
      .split(",")
      .map(s => s.trim())
      .filter(Boolean);

    const runId = `run_${crypto.randomUUID()}`;
    const sizeText = `${w}x${h}`;
    const model = "bytedance/seedream-v4/edit";

    // Create Airtable parent row (attachments use original URLs for Subject/References)
    const parentId = await airtableCreateBatchRow({
      prompt, subjectUrl: subject, referenceUrls, sizeText, model, runId,
    });

    // Pre-convert URLs to base64 data URLs (subject first, refs after)
    const subjectDataUrl = await urlToDataUrl(subject);
    const refsDataUrls = [];
    for (const r of referenceUrls) {
      try { refsDataUrls.push(await urlToDataUrl(r)); }
      catch (e) { console.warn("Ref fetch failed, skipping:", r, e?.message); }
    }
    const imagesOrdered = [subjectDataUrl, ...refsDataUrls];

    // Launch n jobs; space by ~1.2s; each submit has internal retries.
    const webhookUrl = `${PUBLIC_BASE_URL}/webhooks/wavespeed?parentId=${encodeURIComponent(parentId)}`;
    const launched = [];
    for (let i = 0; i < n; i++) {
      // eslint-disable-next-line no-await-in-loop
      await delay(1200);
      // eslint-disable-next-line no-await-in-loop
      const { requestId } = await submitWavespeedJob({
        prompt, images: imagesOrdered, width: w, height: h, webhookUrl,
      });
      launched.push(requestId);
      // record request id immediately
      // eslint-disable-next-line no-await-in-loop
      await airtableAppendRequestId(parentId, requestId);
      // fire-and-forget polling (no await here)
      void pollUntilDone(requestId, parentId).catch(err => {
        console.error("poll error", err);
      });
    }

    res.json({ ok: true, parentId, runId, requestIds: launched });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: String(err?.message || err) });
  }
});

// ------------------------------
// WaveSpeed webhook receiver
// ------------------------------
app.post("/webhooks/wavespeed", async (req, res) => {
  try {
    // You can verify signatures if WaveSpeed signs them; this example trusts source.
    const parentId = req.query.parentId;
    const payload = req.body || {};
    const data = payload.data || payload; // be tolerant
    const requestId = data?.id || payload?.id || req.query?.id;
    const status = data?.status || payload?.status;
    const outputs = data?.outputs || payload?.outputs || [];

    if (!parentId || !requestId) {
      return res.status(400).json({ error: "Missing parentId or requestId" });
    }

    if (status === "completed" && outputs.length) {
      await airtableAppendOutputs(parentId, outputs);
      await airtableMarkSeen(parentId, requestId, { failed: false });
    } else if (status === "failed") {
      await airtableMarkSeen(parentId, requestId, { failed: true });
    }
    // Always 200 quickly so provider doesn't retry excessively
    res.json({ ok: true });
  } catch (err) {
    console.error("Webhook error:", err);
    // Still return 200 to avoid retry storms; internal polling will resolve remaining jobs
    res.json({ ok: true });
  }
});

// Optional: quick health/status
app.get("/", (_req, res) => res.send("WaveSpeed Seedream v4 batch server is up."));
app.get("/healthz", (_req, res) => res.json({ ok: true, time: nowISO() }));

// Start
app.listen(PORT, () => {
  console.log(`Server on :${PORT}`);
});

