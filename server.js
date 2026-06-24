const crypto = require("crypto");
const express = require("express");
const http = require("http");
const WebSocket = require("ws");
const mysql = require("mysql2/promise");

// ===================== DB =====================
const db = mysql.createPool({
    host: process.env.DB_HOST,
    port: Number(process.env.DB_PORT || 4000),
    user: process.env.DB_USER,
    password: process.env.DB_PASS,
    database: process.env.DB_NAME,

    waitForConnections: true,
    connectionLimit: 10,
    queueLimit: 0,

    ssl: process.env.DB_SSL === "true"
        ? {
            minVersion: "TLSv1.2",
            rejectUnauthorized: false
        }
        : null
});


db.getConnection()
  .then(conn => {
    console.log("✅ TIDB CONNECTED OK");
    conn.release();
  })
  .catch(err => {
    console.error("❌ DB ERROR:", err.code || err.message);
  });



async function awaitFakeQuery(sql, params) {
    try {
        if (!process.env.DB_HOST) {
            throw new Error("DB not configured");
        }

        const [rows] = await db.execute(sql, params);
        return rows;
    } catch (err) {
        console.error("DB ERROR:", err.code || err.message);

        // evita crash de WS
        return [];
    }
}

if (!process.env.DB_HOST || !process.env.DB_USER || !process.env.DB_NAME) {
    console.error("❌ Missing DB env vars");
}

// ===================== APP =====================
const app = express();
app.use(express.json());

const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

// ===================== STATE =====================
const clients = new Map();      // ws -> chatId
const chatRooms = new Map();    // chatId -> Set(ws)
const userSockets = new Map(); // userId -> Set(ws)

// ===================== WS HEARTBEAT =====================
function heartbeat() {
    this.isAlive = true;
}

// ===================== WEBSOCKET =====================
wss.on("connection", (ws) => {
    ws.isAlive = true;
    ws.authed = false;
    ws.chatId = null;
    ws.on("pong", heartbeat);

    ws.on("message", async (msg) => {
        try {
            const data = JSON.parse(msg);

            // ping/pong app level
            if (data.type === "ping") {
                ws.send(JSON.stringify({ type: "pong" }));
                return;
            }

            // ================= AUTH =================
            if (data.type === "auth") {
                const chatId = Number(data.chat_id);
if (!chatId) {
    ws.close(1008, "NO_CHAT_ID");
    return;
}

              
                const expected = crypto
                    .createHmac("sha256", process.env.WS_SECRET)
                    .update(String(chatId))
                    .digest("hex");

             if (expected !== data.token) {
    console.log("AUTH FAIL", data.chat_id);

    ws.close(1008, "AUTH_FAILED");
    return;
}


// SOLO AQUÍ
ws.userId = Number(data.user_id);


  if (!userSockets.has(ws.userId)) {
    userSockets.set(ws.userId, new Set());
}
userSockets.get(ws.userId).add(ws);


ws.authed = true;
ws.chatId = chatId;


                // cerrar otros sockets del mismo chat
              for (const [client, room] of clients) {

    if (room === chatId && client !== ws) {

        client.close(1000, "REPLACED");

    }
}

                clients.set(ws, chatId);

                ws.authed = true;
                ws.chatId = chatId;

                if (!chatRooms.has(chatId)) {
                    chatRooms.set(chatId, new Set());
                }

                chatRooms.get(chatId).add(ws);

                return;
            }

            // ================= SYNC =================
            if (data.type === "sync") {

              if (!ws.authed || !ws.chatId) return;

const chatId = ws.chatId;
                const lastId = Number(data.last_message_id || 0);

                const rows = await awaitFakeQuery(`
                    SELECT id, chat_id, mensaje, enviado_por, fecha
                    FROM mensajes
                    WHERE chat_id = ?
                    AND id > ?
                    ORDER BY id ASC
                    LIMIT 100
                `, [chatId, lastId]);

                ws.send(JSON.stringify({
                    type: "sync_messages",
                    messages: rows
                }));

                return;
            }

        } catch (e) {
            console.error("WS MESSAGE ERROR:", e);
        }
    });

    ws.on("close", () => {


if (ws.userId && userSockets.has(ws.userId)) {
    userSockets.get(ws.userId).delete(ws);

    if (userSockets.get(ws.userId).size === 0) {
        userSockets.delete(ws.userId);
    }
}


        const chatId = clients.get(ws);

        clients.delete(ws);

        if (chatId && chatRooms.has(chatId)) {
            chatRooms.get(chatId).delete(ws);

            if (chatRooms.get(chatId).size === 0) {
                chatRooms.delete(chatId);
            }
        }
    });
});

// ===================== ROUTES =====================

// health check Render
app.get("/", (req, res) => {
    res.send("OK");
});

app.get("/health", (req, res) => {
    res.status(200).json({
        ok: true,
        timestamp: Date.now()
    });
});

// ===================== PUSH =====================
app.post("/push", (req, res) => {
    if (req.body.secret !== process.env.WS_SECRET) {
        return res.status(403).json({ error: "forbidden" });
    }


const { chat_id, id, message, sender, type, user_id, hearts_added, hearts_total } = req.body;

const chatId = parseInt(chat_id, 10);

if (!Number.isFinite(chatId)) {
    return res.status(400).json({ error: "invalid chat" });
}



    // payment update broadcast global
    if (type === "payment_update") {
        for (const [ws] of clients) {
            if (ws.readyState !== WebSocket.OPEN) continue;

            ws.send(JSON.stringify({
                type: "payment_update",
                user_id,
                hearts_added,
                hearts_total
            }));
        }

        return res.json({ ok: true });
    }

    if (
        typeof chat_id === "undefined" ||
        typeof id === "undefined" ||
        typeof message === "undefined"
    ) {
        return res.status(400).json({ error: "invalid" });
    }

    const sockets = chatRooms.get(chatId);
    if (!sockets) return res.json({ ok: true });

    if (!sockets || sockets.size === 0) {
        return res.json({ ok: true });
    }

    let delivered = 0;

    for (const ws of sockets) {

        if (ws.chatId !== chatId) continue;
        if (!ws || ws.readyState !== WebSocket.OPEN) continue;

        try {


if (ws.bufferedAmount > 1e6) {
    console.warn("WS slow client skipped");
    continue;
}


            ws.send(JSON.stringify({
                type: "new_message",
                chat_id: chatId,
                id: Number(id),
                message: String(message || ""),
                sender: sender === "ia" ? "ia" : "usuario"
            }));

            delivered++;
        } catch (e) {
            console.error("WS send error:", e);
        }
    }

    console.log("WS delivered:", delivered);

    return res.json({ ok: true });
});

// ===================== ERROR HANDLER =====================
app.use((err, req, res, next) => {
    console.error("EXPRESS ERROR:", err.stack);
    res.status(500).json({ error: "server_error" });
});

// ===================== HEARTBEAT (GLOBAL) =====================
setInterval(() => {
    wss.clients.forEach((ws) => {
        if (ws.readyState !== WebSocket.OPEN) return;

        if (ws.isAlive === false) {
            console.log("WS killed (no heartbeat)");
            return ws.terminate();
        }

        ws.isAlive = false;
        ws.ping();
    });
}, 25000);

// ===================== START =====================
const PORT = process.env.PORT || 10000;

server.listen(PORT, () => {
    console.log("Realtime server running on", PORT);
});