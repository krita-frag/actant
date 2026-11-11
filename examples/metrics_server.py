"""第 3 层 metrics 托管样例：用标准库 ``http.server`` 暴露 Prometheus 文本。

核心 ``actant.metrics_text()`` 提供 Prometheus exposition format 文本，
不托管 HTTP server（外置默认守则）；HTTP 托管行为属第 3 层，不做自动化测试。

运行::

    uv run python examples/metrics_server.py
    curl -s http://127.0.0.1:9100/metrics
"""

import http.server
import threading

import actant


class MetricsHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        if self.path != "/metrics":
            self.send_response(404)
            self.end_headers()
            return
        body = actant.metrics_text().encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


with actant.Runtime(name="metrics-example") as rt:
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 9100), MetricsHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    print("metrics on http://127.0.0.1:9100/metrics, Ctrl+C to stop")
    try:
        threading.Event().wait()
    finally:
        server.shutdown()
        server.server_close()
