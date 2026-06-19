const crypto = require("crypto");
const express = require("express");
const http = require("http");
const WebSocket = require("ws");

const app = express();
app.use(express.json());

const server = http.createServer(app);

const wss = new WebSocket.Server({ server });

const clients = new Map();

function heartbeat() {
    this.isAlive = true;
}

// WebSocket connect
wss.on("connection", (ws) => {

    ws.isAlive = true;

    ws.on("pong", heartbeat);

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

   if (expected !== data.token) {

    console.log("AUTH FAIL", {
        chat_id: data.chat_id,
        received: data.token,
        expected: expected
    });

    ws.close();
    return;
}

   for (const [client, room] of clients) {

    if (
        room == data.chat_id &&
        client !== ws
    ) {

        client.close();

        clients.delete(client);
    }
}

clients.set(
    ws,
    data.chat_id
);

    return;
}
        } catch (e) {

    console.error(
        "WS MESSAGE ERROR:",
        e
    );

}
    });

    ws.on("close", () => {
        clients.delete(ws);
    });
});

// health check (Render lo usa)
app.get("/", (req, res) => {
    res.send("OK");
});


app.get("/health", (req, res) => {
    res.status(200).json({
        ok: true,
        timestamp: Date.now()
    });
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

if (type === "payment_update") {

   for (const [ws] of clients) {

    if (ws.readyState !== WebSocket.OPEN) {
        continue;
    }

    ws.send(JSON.stringify({
        type: "payment_update",
        user_id,
        hearts_added,
        hearts_total
    }));
}

    return res.json({
        ok: true
    });
}


console.log("push:", { chat_id, id, sender });


if (
    typeof chat_id === "undefined" ||
    typeof id === "undefined" ||
    typeof message === "undefined"
) {
    return res.status(400).json({
        error: "invalid"
    });
}

    for (const [ws, room] of clients) {
        if (room == chat_id && ws.readyState === WebSocket.OPEN) {
            ws.send(JSON.stringify({
    type: "new_message",
    chat_id,
    id,
    message,
    sender
}));
        }
    }

    res.json({ ok: true });
});



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



app.use((err, req, res, next) => {

    console.error(
        "EXPRESS ERROR:",
        err.stack
    );

    res.status(500).json({
        error: "server_error"
    });

});






const PORT = process.env.PORT || 10000;

server.listen(PORT, () => {
    console.log("Realtime server running on", PORT);
});