const crypto = require("crypto");
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



if (data.type === "ping") {

    ws.send(
        JSON.stringify({
            type: "pong"
        })
    );

    return;
}

           if (data.type === "auth") {

    const expected = crypto
        .createHmac(
            "sha256",
            process.env.WS_SECRET
        )
        .update(
            String(data.chat_id)
        )
        .digest("hex");

    if (
        expected !== data.token
    ) {

        ws.close();

        return;
    }

    clients.set(
        ws,
        data.chat_id
    );

    return;
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

if (
    req.body.secret !== process.env.WS_SECRET
) {
    return res.status(403).json({
        error: "forbidden"
    });
}

    const { chat_id, id, message, sender } = req.body;

if (
    !chat_id ||
    !id ||
    !message
) {
    return res.status(400).json({
        error: "invalid"
    });
}

    for (const [ws, room] of clients) {
        if (room == chat_id && ws.readyState === WebSocket.OPEN) {
            ws.send(JSON.stringify({
    type: "new_message",
    id,
    message,
    sender
}));
        }
    }

    res.json({ ok: true });
});

const PORT = process.env.PORT || 10000;

server.listen(PORT, () => {
    console.log("Realtime server running on", PORT);
});