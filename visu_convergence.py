"""
Visualisation web — Phase 2 · convergence gossip

Serveur local + simulation in-process (même logique que 2_gossip_n.py :
pair aléatoire, push puis pull, LWW). Les événements partent en SSE vers
la page HTML (Tailwind CDN).

Usage :
  python visu_convergence.py
  # puis ouvrir http://127.0.0.1:8765
"""

from __future__ import annotations

import argparse
import json
import math
import queue
import random
import threading
import time
import webbrowser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse


HOST = "127.0.0.1"
DEFAULT_PORT = 8765
HTML_PATH = Path(__file__).resolve().parent / "visu_convergence.html"
CLE = "seed"


class ClusterSim:
    """Cluster gossip in-process, events poussés vers les abonnés SSE."""

    def __init__(self) -> None:
        self.lock = threading.RLock()
        self.running = False
        self.n = 0
        self.interval = 0.5
        self.stores: list[dict[str, tuple[object, float]]] = []
        self.tours: list[int] = []
        self.subscribers: list[queue.Queue] = []
        self._workers: list[threading.Thread] = []
        self._stop = threading.Event()
        self.converged_at: float | None = None
        self.put_at: float | None = None
        self.source: int | None = None

    def subscribe(self) -> queue.Queue:
        q: queue.Queue = queue.Queue(maxsize=500)
        snap = self._snapshot_event()
        with self.lock:
            self.subscribers.append(q)
        q.put(snap)
        return q

    def unsubscribe(self, q: queue.Queue) -> None:
        with self.lock:
            if q in self.subscribers:
                self.subscribers.remove(q)

    def _broadcast(self, event: dict) -> None:
        dead: list[queue.Queue] = []
        with self.lock:
            subs = list(self.subscribers)
        for q in subs:
            try:
                q.put_nowait(event)
            except queue.Full:
                dead.append(q)
        for q in dead:
            self.unsubscribe(q)

    def _snapshot_event(self) -> dict:
        with self.lock:
            infected = [
                i for i, st in enumerate(self.stores) if CLE in st
            ] if self.stores else []
            return {
                "type": "state",
                "n": self.n,
                "interval": self.interval,
                "running": self.running,
                "infected": infected,
                "tours": list(self.tours),
                "source": self.source,
                "put_at": self.put_at,
                "converged_at": self.converged_at,
                "elapsed": (
                    (self.converged_at or time.time()) - self.put_at
                    if self.put_at
                    else None
                ),
                "theorique": math.ceil(math.log2(self.n)) if self.n > 1 else 0,
            }

    def stop(self) -> None:
        self._stop.set()
        for t in self._workers:
            t.join(timeout=1.0)
        self._workers = []
        with self.lock:
            self.running = False
            self.stores = []
            self.tours = []
            self.converged_at = None
            self.put_at = None
            self.source = None
        self._broadcast({"type": "stopped"})
        self._broadcast(self._snapshot_event())

    def start(self, n: int, interval: float, seed: int | None = None) -> None:
        self.stop()
        self._stop = threading.Event()
        if seed is not None:
            random.seed(seed)
        with self.lock:
            self.n = n
            self.interval = interval
            self.stores = [{} for _ in range(n)]
            self.tours = [0] * n
            self.running = True
            self.converged_at = None
            self.put_at = None
            self.source = None
        self._broadcast({"type": "started", "n": n, "interval": interval})
        self._broadcast(self._snapshot_event())
        for i in range(n):
            t = threading.Thread(
                target=self._boucle_gossip,
                args=(i,),
                daemon=True,
                name=f"gossip-{i}",
            )
            self._workers.append(t)
            t.start()

    def put(self, source: int = 0, valeur: str = "hello") -> None:
        with self.lock:
            if not self.running or not (0 <= source < self.n):
                raise ValueError("cluster non pret ou source invalide")
            ts = time.time()
            self.stores[source][CLE] = (valeur, ts)
            self.put_at = ts
            self.source = source
            self.converged_at = None
        self._broadcast(
            {
                "type": "put",
                "source": source,
                "key": CLE,
                "value": valeur,
                "ts": ts,
            }
        )
        self._broadcast(self._snapshot_event())
        self._check_convergence()

    def _merge(self, dest: int, etat: list[tuple]) -> int:
        maj = 0
        with self.lock:
            store = self.stores[dest]
            for cle, valeur, ts in etat:
                local = store.get(cle)
                if local is None or ts > local[1]:
                    store[cle] = (valeur, ts)
                    maj += 1
        return maj

    def _digest(self, idx: int) -> list[tuple]:
        with self.lock:
            return [
                (c, v, ts) for c, (v, ts) in self.stores[idx].items()
            ]

    def _boucle_gossip(self, idx: int) -> None:
        time.sleep(0.3 + random.random() * 0.2)
        while not self._stop.is_set():
            with self.lock:
                n = self.n
                interval = self.interval
            if n < 2:
                if self._stop.wait(interval):
                    break
                continue
            peers = [j for j in range(n) if j != idx]
            peer = random.choice(peers)

            # push
            etat_local = self._digest(idx)
            maj_peer = self._merge(peer, etat_local)
            # pull
            etat_peer = self._digest(peer)
            maj_local = self._merge(idx, etat_peer)

            with self.lock:
                self.tours[idx] += 1
                tour = self.tours[idx]
                infected = [i for i, st in enumerate(self.stores) if CLE in st]

            self._broadcast(
                {
                    "type": "gossip",
                    "from": idx,
                    "to": peer,
                    "tour": tour,
                    "maj_peer": maj_peer,
                    "maj_local": maj_local,
                    "infected": infected,
                }
            )
            self._broadcast(self._snapshot_event())
            self._check_convergence()

            if self._stop.wait(interval):
                break

    def _check_convergence(self) -> None:
        with self.lock:
            if not self.running or self.put_at is None or self.converged_at is not None:
                return
            if self.n == 0:
                return
            if all(CLE in st for st in self.stores):
                self.converged_at = time.time()
                elapsed = self.converged_at - self.put_at
                max_delta = max(self.tours) if self.tours else 0
                event = {
                    "type": "converged",
                    "elapsed": elapsed,
                    "tours_estimes": elapsed / self.interval if self.interval else 0,
                    "max_tours": max_delta,
                    "theorique": math.ceil(math.log2(self.n)) if self.n > 1 else 0,
                    "n": self.n,
                    "interval": self.interval,
                }
            else:
                return
        self._broadcast(event)
        self._broadcast(self._snapshot_event())


SIM = ClusterSim()


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt: str, *args) -> None:
        print(f"[visu] {self.address_string()} {fmt % args}", flush=True)

    def _cors(self) -> None:
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")

    def _json(self, code: int, payload: dict) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self._cors()
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_OPTIONS(self) -> None:
        self.send_response(204)
        self._cors()
        self.end_headers()

    def do_GET(self) -> None:
        path = urlparse(self.path).path
        if path in ("/", "/index.html"):
            if not HTML_PATH.is_file():
                self.send_error(404, "visu_convergence.html introuvable")
                return
            data = HTML_PATH.read_bytes()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
            return
        if path == "/api/state":
            self._json(200, SIM._snapshot_event())
            return
        if path == "/events":
            self._sse()
            return
        self.send_error(404)

    def do_POST(self) -> None:
        path = urlparse(self.path).path
        length = int(self.headers.get("Content-Length", 0))
        raw = self.rfile.read(length) if length else b"{}"
        try:
            data = json.loads(raw.decode("utf-8") or "{}")
        except json.JSONDecodeError:
            self._json(400, {"error": "json invalide"})
            return

        try:
            if path == "/api/start":
                n = int(data.get("n", 8))
                interval = float(data.get("interval", 0.5))
                seed = data.get("seed")
                if n < 2 or n > 100:
                    raise ValueError("n entre 2 et 100")
                if interval < 0.05 or interval > 5:
                    raise ValueError("interval entre 0.05 et 5 secondes")
                SIM.start(n, interval, int(seed) if seed is not None else None)
                self._json(200, {"ok": True, "state": SIM._snapshot_event()})
                return
            if path == "/api/put":
                source = int(data.get("source", 0))
                valeur = str(data.get("value", "hello"))
                SIM.put(source=source, valeur=valeur)
                self._json(200, {"ok": True, "state": SIM._snapshot_event()})
                return
            if path == "/api/stop":
                SIM.stop()
                self._json(200, {"ok": True, "state": SIM._snapshot_event()})
                return
        except (ValueError, TypeError) as exc:
            self._json(400, {"error": str(exc)})
            return
        self.send_error(404)

    def _sse(self) -> None:
        q = SIM.subscribe()
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream; charset=utf-8")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "keep-alive")
        self._cors()
        self.end_headers()
        try:
            while True:
                try:
                    event = q.get(timeout=15.0)
                except queue.Empty:
                    self.wfile.write(b": keepalive\n\n")
                    self.wfile.flush()
                    continue
                payload = json.dumps(event, ensure_ascii=False)
                self.wfile.write(f"data: {payload}\n\n".encode("utf-8"))
                self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
            pass
        finally:
            SIM.unsubscribe(q)


def main() -> None:
    parser = argparse.ArgumentParser(description="Visualisation convergence gossip")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    parser.add_argument("--no-browser", action="store_true")
    args = parser.parse_args()

    if not HTML_PATH.is_file():
        raise SystemExit(f"Fichier manquant : {HTML_PATH}")

    server = ThreadingHTTPServer((HOST, args.port), Handler)
    url = f"http://{HOST}:{args.port}/"
    print(f"[visu] ecoute sur {url}", flush=True)
    if not args.no_browser:
        threading.Timer(0.6, lambda: webbrowser.open(url)).start()
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n[visu] arret", flush=True)
        SIM.stop()
        server.shutdown()


if __name__ == "__main__":
    main()
