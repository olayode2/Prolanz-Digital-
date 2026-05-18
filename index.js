const {
  default: makeWASocket,
  DisconnectReason,
  fetchLatestBaileysVersion,
  proto,
  getContentType,
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

// Wipe stored auth (used when WhatsApp logs us out or session is bad)
async function clearAuth() {
  try {
    await pool.query("DELETE FROM baileys_auth");
    console.log("🗑️  Cleared Postgres auth state");
  } catch (err) {
    console.error("❌ Failed to clear auth:", err.message);
  }
}

async function connectToWhatsApp() {
  const { state, saveCreds } = await usePostgresAuthState(pool);
  const { version } = await fetchLatestBaileysVersion();

  sock = makeWASocket({
    version,
    logger: pino({ level: "silent" }),
    printQRInTerminal: true,
    auth: state,
    browser: ["LeadQualBot", "Chrome", "1.0.0"],
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

      if (reason === DisconnectReason.badSession) {
        console.log("Bad session — clearing auth and restarting...");
        await clearAuth();
        connectToWhatsApp();
      } else if (reason === DisconnectReason.connectionClosed) {
        console.log("Connection closed — reconnecting...");
        connectToWhatsApp();
      } else if (reason === DisconnectReason.connectionLost) {
        console.log("Connection lost — reconnecting...");
        connectToWhatsApp();
      } else if (reason === DisconnectReason.connectionReplaced) {
        console.log("Connection replaced — another session opened.");
      } else if (reason === DisconnectReason.loggedOut) {
        console.log("Logged out — clearing auth and restarting...");
        await clearAuth();
        connectToWhatsApp();
      } else if (reason === DisconnectReason.restartRequired) {
        console.log("Restart required — reconnecting...");
        connectToWhatsApp();
      } else if (reason === DisconnectReason.timedOut) {
        console.log("Timed out — reconnecting...");
        connectToWhatsApp();
      } else {
        console.log("Unknown disconnect reason — reconnecting...");
        connectToWhatsApp();
      }
 } else if (connection === "open") {
  isConnected = true;
  currentQR = null;
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

  // ── Incoming message handler ──
  // Holds messages briefly before forwarding, so edits can replace them
const pendingMessages = new Map();
const DEBOUNCE_MS = 3000;

async function forwardToN8n(jid, senderNumber, text, originalMsg) {
  if (!N8N_WEBHOOK_URL) return;
  try {
    await axios.post(N8N_WEBHOOK_URL, {
      from: senderNumber,
      jid: jid,
      message: text,
      timestamp: originalMsg.messageTimestamp,
      messageId: originalMsg.key.id,
    });
    console.log(`✅ Forwarded to n8n: ${text}`);
  } catch (err) {
    console.error("❌ Failed to forward to n8n:", err.message);
  }
}

sock.ev.on("messages.upsert", async ({ messages, type }) => {
  if (type !== "notify") return;

  for (const msg of messages) {
    if (msg.key.remoteJid === "status@broadcast") continue;
    if (msg.key.fromMe) continue;

    const from = msg.key.remoteJid;
    const senderNumber = from.replace("@s.whatsapp.net", "").replace("@lid", "");
    const isGroup = from.endsWith("@g.us");

    if (isGroup) continue;

    const messageType = getContentType(msg.message);

    // Handle message edits (WhatsApp lets users edit within 15 min)
    if (messageType === "protocolMessage" && msg.message.protocolMessage?.type === 14) {
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
            await forwardToN8n(from, senderNumber, pending.text, msg);
            pendingMessages.delete(from);
          }, DEBOUNCE_MS);
          pendingMessages.set(from, pending);
        } else {
          console.log(`⚠️ Late edit, forwarding as correction note`);
          try {
            await sock.sendPresenceUpdate('composing', from);
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
    if (messageType === "conversation") {
      text = msg.message.conversation;
    } else if (messageType === "extendedTextMessage") {
      text = msg.message.extendedTextMessage.text;
    } else {
      console.log(`📦 Ignoring non-text message (${messageType}) from ${senderNumber}`);
      continue;
    }

    console.log(`📩 Message from ${senderNumber}: ${text}`);

    try {
      await sock.readMessages([msg.key]);
      await sock.sendPresenceUpdate('composing', from);
    } catch (err) {
      console.error("Presence/read failed:", err.message);
    }

    const existingPending = pendingMessages.get(from);
    if (existingPending) {
      clearTimeout(existingPending.timer);
      text = existingPending.text + "\n" + text;
    }

    const timer = setTimeout(async () => {
      await forwardToN8n(from, senderNumber, text, msg);
      pendingMessages.delete(from);
    },const timer = setTimeout(async () => {
      await forwardToN8n(from, senderNumber, text, msg);
      pendingMessages.delete(from);
    }, DEBOUNCE_MS);

    pendingMessages.set(from, {
      text,
      timer,
      msgKey: msg.key
    });
  }
});
}
// ─── REST ENDPOINTS ──────────────────────────────────────────────────────────

app.get("/", (req, res) => {
  res.json({
    status: isConnected ? "connected" : "disconnected",
    message: isConnected ? "WhatsApp bot is running ✅" : "Bot not connected yet",
  });
});

app.get("/qr", async (req, res) => {
  if (isConnected) {
    return res.send("<h2>✅ WhatsApp is already connected! No QR needed.</h2>");
  }
  if (!currentQR) {
    return res.send("<h2>⏳ QR code not ready yet. Wait 10 seconds and refresh.</h2>");
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

  const { to, message, mentions } = req.body;

  if (!to || !message) {
    return res.status(400).json({ error: "Missing 'to' or 'message' in body" });
  }

  try {
   const jid = to.includes("@") ? to : `${to}@s.whatsapp.net`;
    // Clear typing indicator before sending
await sock.sendPresenceUpdate('paused', jid);

const messageOptions = { text: message };
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
app.listen(PORT, () => {
  console.log(`🚀 Server running on port ${PORT}`);
  connectToWhatsApp();
});
