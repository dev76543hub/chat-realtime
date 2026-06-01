const express = require("express");
const http = require("http");
const WebSocket = require("ws");

const app = express();
app.use(express.json());

const server = http.createServer(app);

const wss = new WebSocket.Server({ server });

const clients = new Map();

// WebSocket connect
wss.on("connection", (ws) => {
    ws.on("message", (msg) => {
        try {
            const data = JSON.parse(msg);

            if (data.chat_id) {
                clients.set(ws, data.chat_id);
            }
        } catch (e) {}
    });

    ws.on("close", () => {
        clients.delete(ws);
    });
});

// health check (Render lo usa)
app.get("/", (req, res) => {
    res.send("OK");
});

// endpoint push desde PHP
app.post("/push", (req, res) => {
    const { chat_id, id, message, sender } = req.body;

    for (const [ws, room] of clients) {
        if (room == chat_id && ws.readyState === WebSocket.OPEN) {
            ws.send(JSON.stringify({ id, message, sender }));
        }
    }

    res.json({ ok: true });
});

const PORT = process.env.PORT || 10000;

server.listen(PORT, () => {
    console.log("Realtime server running on", PORT);
});