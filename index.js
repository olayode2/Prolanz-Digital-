const {
  default: makeWASocket,
  DisconnectReason,
  fetchLatestBaileysVersion,
  proto,
  getContentType,
  downloadMediaMessage,
  useMultiFileAuthState,
} = require("@whiskeysockets/baileys");

const express = require("express");
const axios = require("axios");
const pino = require("pino");
const { Boom } = require("@hapi/boom");
const qrcode = require("qrcode-terminal");
const QRCode = require("qrcode");
const { Pool } = require("pg");

// ─── CONFIG ──────────────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
const N8N_WEBHOOK_URL = process.env.N8N_WEBHOOK_URL;
const API_SECRET = process.env.API_SECRET || "123456789";
// ─────────────────────────────────────────────────────────────────────────────

// Postgres pool — for processed_messages_3 dedup only
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false },
});

const app = express();
app.use(express.json());

let sock = null;
let isConnected = false;
let currentQR = null;
let reconnectAttempts = 0;
let reconnectedAt = null;
const MAX_RECONNECT_ATTEMPTS = 10;

// Wipe stored auth (used when WhatsApp logs us out or session is bad)
async function clearAuth() {
  try {
    const fs = require('fs');
    const path = '/var/data/auth';
    if (fs.existsSync(path)) {
      fs.rmSync(path, { recursive: true, force: true });
    }
    console.log("🗑️  Cleared file auth state");
  } catch (err) {
    console.error("❌ Failed to clear auth:", err.message);
  }
}

// Ensure the processed_messages_3 table exists (for dedup)
async function ensureProcessedTable() {
  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS processed_messages_3 (
        message_id TEXT PRIMARY KEY,
        processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `);
    await pool.query(`
      DELETE FROM processed_messages_3
      WHERE processed_at < NOW() - INTERVAL '7 days'
    `);
    console.log("✅ processed_messages_3 table ready");
  } catch (err) {
    console.error("❌ Failed to set up processed_messages_3 table:", err.message);
  }
}

// Check if a message ID has already been processed
async function isMessageProcessed(messageId) {
  try {
    const result = await pool.query(
      "SELECT 1 FROM processed_messages_3 WHERE message_id = $1",
      [messageId]
    );
    return result.rows.length > 0;
  } catch (err) {
    console.error("Dedup check failed:", err.message);
    return false;
  }
}

// Mark a message ID as processed
async function markMessageProcessed(messageId) {
  try {
    await pool.query(
      "INSERT INTO processed_messages_3 (message_id) VALUES ($1) ON CONFLICT DO NOTHING",
      [messageId]
    );
  } catch (err) {
    console.error("Failed to mark message processed:", err.message);
  }
}

// ─── PER-LEAD PROCESSING LOCK + QUEUE ──────────────────────────────────────────
const processingLocks = new Set();
const messageQueues = new Map();
const LOCK_TIMEOUT_MS = 120000;

async function sendToN8n(payload) {
  console.log("📤 Payload to n8n:", JSON.stringify({ message: payload.message, type: payload.messageType }));
  if (!N8N_WEBHOOK_URL) return;
  try {
    await axios.post(N8N_WEBHOOK_URL, payload);
    console.log(`✅ Forwarded to n8n [${payload.messageType}]: ${payload.message}`);
  } catch (err) {
    console.error("❌ Failed to forward to n8n:", err.message);
  }
}

function lockAndSend(payload) {
  const jid = payload.jid;
  processingLocks.add(jid);
  console.log(`🔒 Locked ${jid}`);
  setTimeout(() => {
    if (processingLocks.has(jid)) {
      console.log(`⏱️ Lock timeout for ${jid} — force-releasing`);
      releaseLock(jid);
    }
  }, LOCK_TIMEOUT_MS);
  sendToN8n(payload);
}

function queueForLead(payload) {
  const jid = payload.jid;
  const existing = messageQueues.get(jid);
  if (existing) {
    existing.message = existing.message + "\n" + payload.message;
    existing.timestamp = payload.timestamp;
    existing.messageId = payload.messageId;
    if (payload.messageType && payload.messageType !== "text") {
      existing.messageType = payload.messageType;
      if (payload.audio) existing.audio = payload.audio;
      if (payload.image) existing.image = payload.image;
    }
    console.log(`📥 Combined into existing queue for ${jid}`);
  } else {
    messageQueues.set(jid, payload);
    console.log(`📥 Queued for locked lead ${jid}`);
  }
}

function releaseLock(jid) {
  processingLocks.delete(jid);
  const queued = messageQueues.get(jid);
  if (queued) {
    messageQueues.delete(jid);
    console.log(`🔓 Released ${jid} — forwarding queued message(s)`);
    lockAndSend(queued);
  } else {
    console.log(`🔓 Released ${jid}`);
  }
}

function releaseLeadLock(to) {
  const direct = to.includes("@") ? to : `${to}@s.whatsapp.net`;
  if (processingLocks.has(direct) || messageQueues.has(direct)) {
    return releaseLock(direct);
  }
  const num = to.replace(/@.*$/, "");
  for (const jid of processingLocks) {
    if (jid.replace(/@.*$/, "") === num) return releaseLock(jid);
  }
  for (const jid of messageQueues.keys()) {
    if (jid.replace(/@.*$/, "") === num) return releaseLock(jid);
  }
}
// ────────────────────────────────────────────────────────────────────────────

async function connectToWhatsApp() {
  const { state, saveCreds } = await useMultiFileAuthState('/var/data/auth_3')
  const { version } = await fetchLatestBaileysVersion();

  sock = makeWASocket({
    version,
    logger: pino({ level: "silent" }),
    auth: state,
    browser: ["LeadQualBot", "Chrome", "1.0.0"],
    syncFullHistory: false,
    connectTimeoutMs: 60000,
    keepAliveIntervalMs: 25000,
    retryRequestDelayMs: 2000,
  });

  sock.ev.on("creds.update", saveCreds);

  sock.ev.on("connection.update", async (update) => {
    const { connection, lastDisconnect, qr } = update;

    if (qr) {
      currentQR = qr;
      console.log("⚡ Scan QR code at /qr endpoint or in terminal:");
      qrcode.generate(qr, { small: true });
    }

    if (connection === "close") {
      isConnected = false;
      const reason = new Boom(lastDisconnect?.error)?.output?.statusCode;
      const errorMessage = lastDisconnect?.error?.message || '';
      console.log("❌ Connection closed. Reason:", reason, errorMessage);

      const isBadMac =
        reason === DisconnectReason.badSession ||
        (errorMessage.includes('Bad MAC') ||
        errorMessage.includes('bad-mac')) &&
        reason !== 500;

      const isLoggedOut = reason === DisconnectReason.loggedOut;
      const isTimeout = reason === 408 || reason === 503 || reason === 428;
      const isStreamError = reason === 500;

      if (isLoggedOut) {
        console.log("Logged out by user — clearing auth, will need fresh QR scan...");
        await clearAuth();
        reconnectAttempts = 0;
        setTimeout(() => connectToWhatsApp(), 3000);
        return;
      }

      if (isStreamError) {
        console.log('⚡ Stream error — reconnecting without wiping auth...');
        reconnectAttempts = 0;
        setTimeout(() => connectToWhatsApp(), 5000);
        return;
      }

      if (isBadMac) {
        console.log('🔑 Bad MAC / Bad Session — clearing auth and reconnecting fresh');
        await clearAuth();
        reconnectAttempts = 0;
        setTimeout(() => connectToWhatsApp(), 5000);
        return;
      }

      if (isTimeout) {
        console.log(`⏱️ Timeout disconnect (${reason}) — reconnecting without wiping auth...`);
        reconnectAttempts = 0;
        setTimeout(() => connectToWhatsApp(), 5000);
        return;
      }

      reconnectAttempts++;
      const delay = Math.min(3000 * reconnectAttempts, 60000);
      if (reconnectAttempts > MAX_RECONNECT_ATTEMPTS) {
        console.log(`❌ Failed after ${MAX_RECONNECT_ATTEMPTS} attempts. Wiping auth as last resort.`);
        await clearAuth();
        reconnectAttempts = 0;
        setTimeout(() => connectToWhatsApp(), 5000);
      } else {
        console.log(`Reconnecting in ${delay / 1000}s (attempt ${reconnectAttempts}/${MAX_RECONNECT_ATTEMPTS})...`);
        setTimeout(() => connectToWhatsApp(), delay);
      }

    } else if (connection === "open") {
      reconnectedAt = Date.now();
      isConnected = true;
      currentQR = null;
      reconnectAttempts = 0;
      console.log("✅ WhatsApp connected successfully!");
      try {
        const groups = await sock.groupFetchAllParticipating();
        console.log("\n📋 Groups the bot is in:");
        for (const groupId in groups) {
          const g = groups[groupId];
          console.log(`   ${g.subject}  →  ${g.id}`);
        }
        console.log("");
      } catch (err) {
        console.error("❌ Failed to fetch groups:", err.message);
      }
    }
  });

  const pendingMessages = new Map();
  const DEBOUNCE_MS = 6000;

  async function forwardToN8n(jid, senderNumber, text, originalMsg, extraPayload = {}) {
    console.log('📤 Payload being sent:', JSON.stringify({ text, ...extraPayload }));
    if (!N8N_WEBHOOK_URL) return;
    try {
      const payload = {
        from: senderNumber,
        jid: jid,
        message: text,
        timestamp: originalMsg.messageTimestamp,
        messageId: originalMsg.key.id,
        messageType: "text",
        ...extraPayload,
      };
      await axios.post(N8N_WEBHOOK_URL, payload);
      console.log(`✅ Forwarded to n8n [${payload.messageType}]: ${text}`);
    } catch (err) {
      console.error("❌ Failed to forward to n8n:", err.message);
    }
  }

  sock.ev.on("messages.upsert", async ({ messages, type }) => {
    if (type !== "notify" && type !== "append") return;

    for (const msg of messages) {
      if (msg.key.remoteJid === "status@broadcast") continue;
      if (msg.key.fromMe) continue;

      const messageId = msg.key.id;
      if (messageId && (await isMessageProcessed(messageId))) {
        const msgTimestamp = msg.messageTimestamp * 1000;
        const now = Date.now();
        const ageMinutes = (now - msgTimestamp) / 1000 / 60;

        const justReconnected = reconnectedAt && (now - reconnectedAt) < 120000;

        if (!justReconnected || ageMinutes > 30) {
          console.log(`⏭️  Skipping already-processed message ${messageId}`);
          continue;
        }
        console.log(`🔄 Reprocessing recent message ${messageId} (${Math.round(ageMinutes)}min old)`);
      }

      if (messageId) {
        await markMessageProcessed(messageId);
      }

      const rawFrom = msg.key.remoteJid;
// When remoteJid is a LID, the real number is in these fallback fields
const realJid =
  (rawFrom.includes("@lid")
    ? (msg.key.senderPn || msg.key.participantPn || msg.key.participant)
    : rawFrom) || rawFrom;
const from = realJid;
const senderNumber = from
  .replace("@s.whatsapp.net", "")
  .replace("@g.us", "")
  .replace("@lid", "");
const isGroup = rawFrom.endsWith("@g.us");

      if (isGroup) continue;

      const messageType = getContentType(msg.message);

      if (
        messageType === "protocolMessage" &&
        msg.message.protocolMessage?.type === 14
      ) {
        const editedContent = msg.message.protocolMessage.editedMessage;
        let editedText = "";
        if (editedContent?.conversation) {
          editedText = editedContent.conversation;
        } else if (editedContent?.extendedTextMessage?.text) {
          editedText = editedContent.extendedTextMessage.text;
        }

        if (editedText) {
          console.log(`✏️ Edit from ${senderNumber}: ${editedText}`);

          const pending = pendingMessages.get(from);
          if (pending) {
            console.log(`🔄 Replacing pending message with edit`);
            clearTimeout(pending.timer);
            pending.text = editedText;
            pending.timer = setTimeout(async () => {
              await forwardToN8n(from, senderNumber, pending.text, msg, pending.extraPayload || {});
              pendingMessages.delete(from);
            }, DEBOUNCE_MS);
            pendingMessages.set(from, pending);
          } else {
            console.log(`⚠️ Late edit, forwarding as correction note`);
            try {
              await sock.sendPresenceUpdate("composing", from);
            } catch (err) {}
            await forwardToN8n(
              from,
              senderNumber,
              `[CORRECTION FROM LEAD: My previous message should actually be "${editedText}". Please use this corrected version.]`,
              msg
            );
          }
        }
        continue;
      }

      let text = "";
      let extraPayload = {};

      if (messageType === "conversation") {
        text = msg.message.conversation;

      } else if (messageType === "extendedTextMessage") {
        text = msg.message.extendedTextMessage.text;

      } else if (messageType === "audioMessage") {
        try {
          console.log(`🎙️ Voice note from ${senderNumber} — downloading...`);
          const buffer = await downloadMediaMessage(
            msg,
            "buffer",
            {},
            {
              logger: pino({ level: "silent" }),
              reuploadRequest: sock.updateMediaMessage,
            }
          );
          const base64Audio = buffer.toString("base64");
          const mimetype =
            msg.message.audioMessage.mimetype || "audio/ogg; codecs=opus";
          const duration = msg.message.audioMessage.seconds || 0;

          text = "[Voice Note]";
          extraPayload = {
            messageType: "audio",
            audio: {
              base64: base64Audio,
              mimetype: mimetype,
              durationSeconds: duration,
            },
          };
          console.log(`🎙️ Voice note downloaded (${duration}s) from ${senderNumber}`);
        } catch (err) {
          console.error("❌ Failed to download voice note:", err.message);
          continue;
        }

      } else if (messageType === "imageMessage") {
        try {
          console.log(`🖼️ Image from ${senderNumber} — downloading...`);
          const buffer = await downloadMediaMessage(
            msg,
            "buffer",
            {},
            {
              logger: pino({ level: "silent" }),
              reuploadRequest: sock.updateMediaMessage,
            }
          );
          const base64Image = buffer.toString("base64");
          const mimetype =
            msg.message.imageMessage.mimetype || "image/jpeg";
          const caption = msg.message.imageMessage.caption || "";

          text = caption ? `[Image] ${caption}` : "[Image]";
          extraPayload = {
            messageType: "image",
            image: {
              base64: base64Image,
              mimetype: mimetype,
              caption: caption,
            },
          };
          console.log(`🖼️ Image downloaded from ${senderNumber}${caption ? ` — caption: "${caption}"` : ""}`);
        } catch (err) {
          console.error("❌ Failed to download image:", err.message);
          continue;
        }

      } else {
        console.log(
          `📦 Ignoring unsupported message type (${messageType}) from ${senderNumber}`
        );
        continue;
      }

      console.log(`📩 Message from ${senderNumber} [${extraPayload.messageType || "text"}]: ${text}`);

      try {
        await sock.readMessages([msg.key]);
        await sock.sendPresenceUpdate("composing", from);
      } catch (err) {
        console.error("Presence/read failed:", err.message);
      }

      const existingPending = pendingMessages.get(from);
      if (existingPending) {
        clearTimeout(existingPending.timer);
        if (!extraPayload.messageType) {
          text = existingPending.text + "\n" + text;
          extraPayload = existingPending.extraPayload || {};
        } else {
          await forwardToN8n(
            from,
            senderNumber,
            existingPending.text,
            msg,
            existingPending.extraPayload || {}
          );
          pendingMessages.delete(from);
        }
      }

      const timer = setTimeout(async () => {
        const payload = {
          from: senderNumber,
          jid: from,
          message: text,
          timestamp: msg.messageTimestamp,
          messageId: msg.key.id,
          messageType: "text",
          ...extraPayload,
        };
        if (processingLocks.has(from)) {
          queueForLead(payload);
        } else {
          lockAndSend(payload);
        }
        pendingMessages.delete(from);
      }, DEBOUNCE_MS);

      pendingMessages.set(from, {
        text,
        timer,
        msgKey: msg.key,
        extraPayload,
      });
    }
  });
}

// ─── REST ENDPOINTS ──────────────────────────────────────────────────────────

app.get("/", (req, res) => {
  res.json({
    status: isConnected ? "connected" : "disconnected",
    message: isConnected
      ? "WhatsApp bot is running ✅"
      : "Bot not connected yet",
  });
});

app.get("/qr", async (req, res) => {
  if (isConnected) {
    return res.send(
      "<h2>✅ WhatsApp is already connected! No QR needed.</h2>"
    );
  }
  if (!currentQR) {
    return res.send(
      "<h2>⏳ QR code not ready yet. Wait 10 seconds and refresh.</h2>"
    );
  }
  try {
    const qrImage = await QRCode.toDataURL(currentQR);
    res.send(`
      <html>
        <body style="display:flex;flex-direction:column;align-items:center;justify-content:center;height:100vh;font-family:sans-serif;background:#111;color:#fff;">
          <h2>📱 Scan with WhatsApp</h2>
          <p>Open WhatsApp → Linked Devices → Link a Device</p>
          <img src="${qrImage}" style="width:300px;height:300px;" />
          <p style="margin-top:20px;color:#aaa;">Page auto-refreshes every 30 seconds</p>
          <script>setTimeout(() => location.reload(), 30000)</script>
        </body>
      </html>
    `);
  } catch (err) {
    res.status(500).send("Error generating QR code");
  }
});

app.post("/send", async (req, res) => {
  const secret = req.headers["x-api-secret"];
  if (secret !== API_SECRET) {
    return res.status(401).json({ error: "Unauthorized" });
  }

  if (!isConnected) {
    return res.status(503).json({ error: "WhatsApp not connected yet" });
  }

  const { to, message, mentions, imageUrl } = req.body;

  if (!to || !message) {
    return res
      .status(400)
      .json({ error: "Missing 'to' or 'message' in body" });
  }

  try {
    const jid = to.includes("@") ? to : `${to}@s.whatsapp.net`;

    await sock.sendPresenceUpdate("paused", jid);

    let messageOptions;
    if (imageUrl) {
      messageOptions = {
        image: { url: imageUrl },
        caption: message,
      };
    } else {
      messageOptions = { text: message };
    }

    if (mentions && Array.isArray(mentions) && mentions.length > 0) {
      messageOptions.mentions = mentions;
    }

    await sock.sendMessage(jid, messageOptions);
    console.log(`📤 Sent to ${to}: ${message}`);
    releaseLeadLock(to);
    res.json({ success: true });
  } catch (err) {
    console.error("❌ Failed to send message:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// ─── START ────────────────────────────────────────────────────────────────────
app.listen(PORT, async () => {
  console.log(`🚀 Server running on port ${PORT}`);
  await ensureProcessedTable();
  connectToWhatsApp();
});
