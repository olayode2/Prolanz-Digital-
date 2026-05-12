const {
  default: makeWASocket,
  useMultiFileAuthState,
  DisconnectReason,
  fetchLatestBaileysVersion,
  makeInMemoryStore,
  jidDecode,
  proto,
  getContentType,
} = require("@whiskeysockets/baileys");

const express = require("express");
const axios = require("axios");
const pino = require("pino");
const { Boom } = require("@hapi/boom");
const fs = require("fs");
const path = require("path");
const qrcode = require("qrcode-terminal");

// ─── CONFIG ──────────────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
const N8N_WEBHOOK_URL = process.env.N8N_WEBHOOK_URL; // your n8n webhook URL
const API_SECRET = process.env.API_SECRET || "changeme"; // secret to protect your /send endpoint
const AUTH_FOLDER = "./auth_info"; // session files stored here
// ─────────────────────────────────────────────────────────────────────────────

const app = express();
app.use(express.json());

let sock = null;
let isConnected = false;

// In-memory store for message history (optional but useful)
const store = makeInMemoryStore({
  logger: pino({ level: "silent" }),
});

async function connectToWhatsApp() {
  const { state, saveCreds } = await useMultiFileAuthState(AUTH_FOLDER);
  const { version } = await fetchLatestBaileysVersion();

  sock = makeWASocket({
    version,
    logger: pino({ level: "silent" }), // keep logs clean
    printQRInTerminal: true,           // QR code shows in Railway logs
    auth: state,
    browser: ["LeadQualBot", "Chrome", "1.0.0"],
    getMessage: async (key) => {
      if (store) {
        const msg = await store.loadMessage(key.remoteJid, key.id);
        return msg?.message || undefined;
      }
      return proto.Message.fromObject({});
    },
  });

  store?.bind(sock.ev);

  // ── Save credentials whenever they update ──
  sock.ev.on("creds.update", saveCreds);

  // ── Connection state handler ──
  sock.ev.on("connection.update", async (update) => {
    const { connection, lastDisconnect, qr } = update;

    if (qr) {
      console.log("⚡ Scan this QR code in WhatsApp:");
      qrcode.generate(qr, { small: true });
    }

    if (connection === "close") {
      isConnected = false;
      const reason = new Boom(lastDisconnect?.error)?.output?.statusCode;
      console.log("❌ Connection closed. Reason:", reason);

      // Auto-reconnect logic
      if (reason === DisconnectReason.badSession) {
        console.log("Bad session — deleting auth and restarting...");
        fs.rmSync(AUTH_FOLDER, { recursive: true, force: true });
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
        console.log("Logged out — deleting session and restarting...");
        fs.rmSync(AUTH_FOLDER, { recursive: true, force: true });
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
      console.log("✅ WhatsApp connected successfully!");
    }
  });

  // ── Incoming message handler ──
  sock.ev.on("messages.upsert", async ({ messages, type }) => {
    if (type !== "notify") return;

    for (const msg of messages) {
      // Ignore status broadcasts and your own messages
      if (msg.key.remoteJid === "status@broadcast") continue;
      if (msg.key.fromMe) continue;

      const from = msg.key.remoteJid;
      const senderNumber = from.replace("@s.whatsapp.net", "");
      const isGroup = from.endsWith("@g.us");

      // Ignore group messages (only handle 1-on-1 for lead qualification)
      if (isGroup) continue;

      // Extract message text
      const messageType = getContentType(msg.message);
      let text = "";

      if (messageType === "conversation") {
        text = msg.message.conversation;
      } else if (messageType === "extendedTextMessage") {
        text = msg.message.extendedTextMessage.text;
      } else {
        // Non-text message received — optionally notify n8n
        text = `[${messageType}]`;
      }

      console.log(`📩 Message from ${senderNumber}: ${text}`);

      // Forward to n8n
      if (N8N_WEBHOOK_URL) {
        try {
          await axios.post(N8N_WEBHOOK_URL, {
            from: senderNumber,
            jid: from,
            message: text,
            timestamp: msg.messageTimestamp,
            messageId: msg.key.id,
          });
          console.log(`✅ Forwarded to n8n`);
        } catch (err) {
          console.error("❌ Failed to forward to n8n:", err.message);
        }
      }
    }
  });
}

// ─── REST ENDPOINTS ──────────────────────────────────────────────────────────

// Health check
app.get("/", (req, res) => {
  res.json({
    status: isConnected ? "connected" : "disconnected",
    message: isConnected ? "WhatsApp bot is running ✅" : "Bot not connected yet",
  });
});

// Send a message — called by n8n HTTP Request node
app.post("/send", async (req, res) => {
  // Simple secret check
  const secret = req.headers["x-api-secret"];
  if (secret !== API_SECRET) {
    return res.status(401).json({ error: "Unauthorized" });
  }

  if (!isConnected) {
    return res.status(503).json({ error: "WhatsApp not connected yet" });
  }

  const { to, message } = req.body;

  if (!to || !message) {
    return res.status(400).json({ error: "Missing 'to' or 'message' in body" });
  }

  try {
    // Format number to WhatsApp JID
    const jid = to.includes("@s.whatsapp.net") ? to : `${to}@s.whatsapp.net`;
    await sock.sendMessage(jid, { text: message });
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
