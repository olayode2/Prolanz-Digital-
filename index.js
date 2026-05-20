const {
  default: makeWASocket,
  DisconnectReason,
  fetchLatestBaileysVersion,
  proto,
  getContentType,
  downloadMediaMessage, // ✅ Added for voice note + image support
} = require("@whiskeysockets/baileys");

const express = require("express");
const axios = require("axios");
const pino = require("pino");
const { Boom } = require("@hapi/boom");
const qrcode = require("qrcode-terminal");
const QRCode = require("qrcode");
const { Pool } = require("pg");
const { usePostgresAuthState } = require("./auth-state-pg");

// ─── CONFIG ──────────────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
const N8N_WEBHOOK_URL = process.env.N8N_WEBHOOK_URL;
const API_SECRET = process.env.API_SECRET || "changeme";
// ─────────────────────────────────────────────────────────────────────────────

// Postgres pool — created once, persists across reconnects
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
const MAX_RECONNECT_ATTEMPTS = 5;

// Wipe stored auth (used when WhatsApp logs us out or session is bad)
async function clearAuth() {
  try {
    await pool.query("DELETE FROM baileys_auth");
    console.log("🗑️  Cleared Postgres auth state");
  } catch (err) {
    console.error("❌ Failed to clear auth:", err.message);
  }
}

// Ensure the processed_messages table exists (for dedup)
async function ensureProcessedTable() {
  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS processed_messages (
        message_id TEXT PRIMARY KEY,
        processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `);
    // Cleanup old IDs so the table doesn't grow forever
    await pool.query(`
      DELETE FROM processed_messages
      WHERE processed_at < NOW() - INTERVAL '7 days'
    `);
    console.log("✅ processed_messages table ready");
  } catch (err) {
    console.error("❌ Failed to set up processed_messages table:", err.message);
  }
}

// Check if a message ID has already been processed
async function isMessageProcessed(messageId) {
  try {
    const result = await pool.query(
      "SELECT 1 FROM processed_messages WHERE message_id = $1",
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
      "INSERT INTO processed_messages (message_id) VALUES ($1) ON CONFLICT DO NOTHING",
      [messageId]
    );
  } catch (err) {
    console.error("Failed to mark message processed:", err.message);
  }
}

async function connectToWhatsApp() {
  const { state, saveCreds } = await usePostgresAuthState(pool);
  const { version } = await fetchLatestBaileysVersion();

  sock = makeWASocket({
    version,
    logger: pino({ level: "silent" }),
    auth: state,
    browser: ["LeadQualBot", "Chrome", "1.0.0"],
    syncFullHistory: true,
  });

  // ── Save credentials whenever they update ──
  sock.ev.on("creds.update", saveCreds);

  // ── Connection state handler ──
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
      console.log("❌ Connection closed. Reason:", reason);

      if (reason === DisconnectReason.loggedOut) {
        console.log("Logged out by user — clearing auth, will need fresh QR scan...");
        await clearAuth();
        reconnectAttempts = 0;
        connectToWhatsApp();
      } else {
        reconnectAttempts++;
        const delay = Math.min(2000 * reconnectAttempts, 30000);

        if (reconnectAttempts > MAX_RECONNECT_ATTEMPTS) {
          console.log(`❌ Failed to reconnect after ${MAX_RECONNECT_ATTEMPTS} attempts. Wiping auth as last resort.`);
          await clearAuth();
          reconnectAttempts = 0;
          setTimeout(() => connectToWhatsApp(), 2000);
        } else {
          console.log(`Reconnecting in ${delay / 1000}s (attempt ${reconnectAttempts}/${MAX_RECONNECT_ATTEMPTS}) without wiping auth...`);
          setTimeout(() => connectToWhatsApp(), delay);
        }
      }
    } else if (connection === "open") {
      isConnected = true;
      currentQR = null;
      reconnectAttempts = 0;
      console.log("✅ WhatsApp connected successfully!");

      // Print groups on connect so we can grab JIDs from logs
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

  // ── Holds messages briefly before forwarding, so edits can replace them ──
  const pendingMessages = new Map();
  const DEBOUNCE_MS = 7000;

  // ── Forward payload to n8n ──
  // Now accepts an extraPayload object to merge in media fields
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
        messageType: "text", // default; overridden by extraPayload if media
        ...extraPayload,
      };
      await axios.post(N8N_WEBHOOK_URL, payload);
      console.log(`✅ Forwarded to n8n [${payload.messageType}]: ${text}`);
    } catch (err) {
      console.error("❌ Failed to forward to n8n:", err.message);
    }
  }

  // ── Incoming message handler ──
  sock.ev.on("messages.upsert", async ({ messages, type }) => {
    // Process both real-time (notify) and historical/offline (append) messages
    if (type !== "notify" && type !== "append") return;

    for (const msg of messages) {
      if (msg.key.remoteJid === "status@broadcast") continue;
      if (msg.key.fromMe) continue;

      // Dedup: skip if we've already processed this message ID
      const messageId = msg.key.id;
      if (messageId && (await isMessageProcessed(messageId))) {
        console.log(`⏭️  Skipping already-processed message ${messageId}`);
        continue;
      }
      if (messageId) {
        await markMessageProcessed(messageId);
      }

      const from = msg.key.remoteJid;
      const senderNumber = from
        .replace("@s.whatsapp.net", "")
        .replace("@lid", "");
      const isGroup = from.endsWith("@g.us");

      if (isGroup) continue;

      const messageType = getContentType(msg.message);

      // ── Handle message edits (WhatsApp lets users edit within 15 min) ──
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

      // ── Parse message content by type ──
      let text = "";
      let extraPayload = {};

      if (messageType === "conversation") {
        // ── Plain text ──
        text = msg.message.conversation;

      } else if (messageType === "extendedTextMessage") {
        // ── Text with link preview or formatting ──
        text = msg.message.extendedTextMessage.text;

      } else if (messageType === "audioMessage") {
        // ── Voice note ──
        // Download as buffer and encode to base64 for n8n
        // n8n side: decode base64 → send to OpenAI Whisper for transcription
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
        // ── Image (including competitor pricing screenshots) ──
        // Download as buffer and encode to base64 for n8n
        // n8n side: pass base64 to Claude Vision or GPT-4o Vision
        // Prompt suggestion: "If this is a competitor pricing screenshot, extract
        // prices and compare with ours. Otherwise describe what the lead is showing."
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
        // ── Unsupported type — skip ──
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

      // ── Debounce: concatenate rapid messages (handles multi-part questions) ──
      // Messages sent within DEBOUNCE_MS of each other are merged into one payload
      // The AI system prompt should handle numbered answers for multi-part questions
      const existingPending = pendingMessages.get(from);
      if (existingPending) {
        clearTimeout(existingPending.timer);
        // Only concatenate text; if new message is media, it goes separately
        if (!extraPayload.messageType) {
          text = existingPending.text + "\n" + text;
          extraPayload = existingPending.extraPayload || {};
        } else {
          // Media arrived after a pending text — flush the text first, then handle media
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
        await forwardToN8n(from, senderNumber, text, msg, extraPayload);
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
