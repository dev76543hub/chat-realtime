const crypto = require("crypto");
const express = require("express");
const http = require("http");
const WebSocket = require("ws");
const mysql = require("mysql2/promise");

// ===================== DB =====================
const db = mysql.createPool({
    host: process.env.DB_HOST,
    user: process.env.DB_USER,
    password: process.env.DB_PASS,
    database: process.env.DB_NAME,
    waitForConnections: true,
    connectionLimit: 10
});

async function awaitFakeQuery(sql, params) {
    const [rows] = await db.execute(sql, params);
    return rows;
}

// ===================== APP =====================
const app = express();
app.use(express.json());

const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

// ===================== STATE =====================
const clients = new Map();      // ws -> chatId
const chatRooms = new Map();    // chatId -> Set(ws)

// ===================== WS HEARTBEAT =====================
function heartbeat() {
    this.isAlive = true;
}

// ===================== WEBSOCKET =====================
wss.on("connection", (ws) => {
    ws.isAlive = true;
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

                const expected = crypto
                    .createHmac("sha256", process.env.WS_SECRET)
                    .update(String(chatId))
                    .digest("hex");

                if (expected !== data.token) {
                    console.log("AUTH FAIL", {
                        chat_id: data.chat_id,
                        received: data.token,
                        expected
                    });
                    ws.close();
                    return;
                }

                // cerrar otros sockets del mismo chat
                for (const [client, room] of clients) {
                    if (room === chatId && client !== ws) {
                        client.close();
                        clients.delete(client);
                    }
                }

                clients.set(ws, chatId);

                if (!chatRooms.has(chatId)) {
                    chatRooms.set(chatId, new Set());
                }

                chatRooms.get(chatId).add(ws);

                return;
            }

            // ================= SYNC =================
            if (data.type === "sync") {
                const chatId = Number(data.chat_id);
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

    const {
        chat_id,
        id,
        message,
        sender,
        type,
        user_id,
        hearts_added,
        hearts_total
    } = req.body;

    const chatId = parseInt(chat_id, 10);

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

    if (!sockets || sockets.size === 0) {
        return res.json({ ok: true });
    }

    let delivered = 0;

    for (const ws of sockets) {
        if (!ws || ws.readyState !== WebSocket.OPEN) continue;

        try {
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
        if (ws.isAlive === false) {
            ws.terminate();
            return;
        }

        ws.isAlive = false;
        ws.ping();
    });
}, 30000);

// ===================== START =====================
const PORT = process.env.PORT || 10000;

server.listen(PORT, () => {
    console.log("Realtime server running on", PORT);
});