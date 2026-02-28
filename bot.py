"""
bot.py
Simple REST API bot for Israel Public Transit Monitoring Platform.
Exposes real-time transit data over HTTP on port 5000.

Endpoints:
  GET /            - health check
  GET /status      - system status
  GET /buses       - latest bus positions from bus_positions.json
  GET /stops       - bus stops with nearest stop info
"""

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os

BOT_PORT = int(os.getenv("BOT_PORT", 5000))


def load_json(filename):
    path = os.path.join(os.path.dirname(__file__), filename)
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return []


class BotHandler(BaseHTTPRequestHandler):

    def log_message(self, format, *args):
        print(f"[BOT] {self.address_string()} - {format % args}")

    def send_json(self, data, status=200):
        body = json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", len(body))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    def send_html(self, filepath):
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                body = f.read().encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", len(body))
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(body)
        except FileNotFoundError:
            self.send_json({"error": "HTML file not found"}, status=404)

    def do_GET(self):
        if self.path == "/health":
            self.send_json({"status": "ok", "service": "Israel Transit Bot", "port": BOT_PORT})

        elif self.path == "/" or self.path == "/agent":
            # Serve the transit agent dashboard
            html_path = os.path.join(os.path.dirname(__file__), "agent_transit.html")
            self.send_html(html_path)

        elif self.path == "/status":
            self.send_json({
                "status": "running",
                "service": "Israel Public Transit Monitoring Platform",
                "endpoints": {
                    "Agent Dashboard": f"http://localhost:{BOT_PORT}/agent",
                    "Airflow UI":      "http://localhost:8081",
                    "Kafka UI":        "http://localhost:8080",
                    "MinIO Console":   "http://localhost:9001",
                    "Kibana":          "http://localhost:5601",
                    "Bot API":         f"http://localhost:{BOT_PORT}",
                }
            })

        elif self.path == "/buses":
            # Return array directly (expected by agent_transit.html)
            data = load_json("buses_with_nearest_stops.json") or load_json("bus_positions.json")
            self.send_json(data[:100])

        elif self.path == "/stops":
            data = load_json("buses_with_nearest_stops.json")
            self.send_json(data[:100])

        else:
            self.send_json({"error": "Not found", "path": self.path}, status=404)

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.end_headers()


def main():
    server = ThreadingHTTPServer(("0.0.0.0", BOT_PORT), BotHandler)
    print(f"🤖 Transit Bot API running on http://0.0.0.0:{BOT_PORT}")
    print(f"   GET /agent     → Agent Transit Dashboard (HTML)")
    print(f"   GET /health    → health check (JSON)")
    print(f"   GET /status    → system status & service URLs")
    print(f"   GET /buses     → latest bus positions (array)")
    print(f"   GET /stops     → buses with nearest stops (array)")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n🛑 Bot stopped.")
        server.server_close()


if __name__ == "__main__":
    main()
