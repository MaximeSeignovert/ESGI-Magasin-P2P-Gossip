"""
Visualisation web — Phase 3 · horloges vectorielles

Aligné sur 3_lww.py : chaque valeur porte une horloge {noeud: compteur}.
Deux écritures concurrentes (rouge / bleu) produisent des horloges
incomparables → VRAI conflit détecté. Résolution : le vecteur le plus
grand gagne et absorbe l'horloge du perdant ; le gossip propage la
version gagnante partout.

Usage :
  python visu_lww.py
  # puis ouvrir http://127.0.0.1:8767
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
DEFAULT_PORT = 8767
HTML_PATH = Path(__file__).resolve().parent / "visu_lww.html"
CLE = "couleur"
VAL_A = "rouge"
VAL_B = "bleu"


def node_label(i: int) -> str:
    return f"N{i}"


def vc_compare(a: dict, b: dict) -> str:
    a_gagne = any(a.get(k, 0) > b.get(k, 0) for k in set(a) | set(b))
    b_gagne = any(a.get(k, 0) < b.get(k, 0) for k in set(a) | set(b))
    if a_gagne and b_gagne:
        return "concurrent"
    if a_gagne:
        return "after"
    if b_gagne:
        return "before"
    return "equal"


def vc_merge(a: dict, b: dict) -> dict:
    return {k: max(a.get(k, 0), b.get(k, 0)) for k in set(a) | set(b)}


def vc_format(vc: dict) -> str:
    return "{" + ",".join(f"{k}:{v}" for k, v in sorted(vc.items())) + "}"


def vc_en_tuple(vc: dict) -> tuple:
    return tuple(sorted(vc.items()))


def poids(valeur, vc: dict) -> tuple:
    return (sum(vc.values()), vc_en_tuple(vc), str(valeur))


class ClusterVC:
    """Cluster gossip in-process — même sémantique que 3_lww.py."""

    def __init__(self) -> None:
        self.lock = threading.RLock()
        self.running = False
        self.n = 0
        self.interval = 0.5
        self.ids: list[str] = []
        # store[i] : clé -> (valeur, vc)
        self.stores: list[dict[str, tuple[object, dict]]] = []
        self.tours: list[int] = []
        self.subscribers: list[queue.Queue] = []
        self._workers: list[threading.Thread] = []
        self._stop = threading.Event()
        self.converged_at: float | None = None
        self.conflict_at: float | None = None
        self.puts: dict[str, dict] = {}
        self.attendu: str | None = None
        self.relation: str | None = None
        self.conflits_detectes = 0

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

    def _valeurs(self) -> list:
        return [None if CLE not in st else st[CLE][0] for st in self.stores]

    def _vcs(self) -> list:
        out = []
        for st in self.stores:
            if CLE not in st:
                out.append(None)
            else:
                out.append(dict(st[CLE][1]))
        return out

    def _counts(self, valeurs: list) -> dict:
        c = {VAL_A: 0, VAL_B: 0, "none": 0, "other": 0}
        for v in valeurs:
            if v is None:
                c["none"] += 1
            elif v == VAL_A:
                c[VAL_A] += 1
            elif v == VAL_B:
                c[VAL_B] += 1
            else:
                c["other"] += 1
        return c

    def _snapshot_event(self) -> dict:
        with self.lock:
            valeurs = self._valeurs()
            vcs = self._vcs()
            return {
                "type": "state",
                "n": self.n,
                "interval": self.interval,
                "running": self.running,
                "ids": list(self.ids),
                "valeurs": valeurs,
                "vcs": [vc_format(v) if v else None for v in vcs],
                "counts": self._counts(valeurs),
                "tours": list(self.tours),
                "conflict_at": self.conflict_at,
                "converged_at": self.converged_at,
                "puts": {
                    k: {
                        "source": v["source"],
                        "source_id": v["source_id"],
                        "vc": vc_format(v["vc"]),
                        "poids": list(poids(k, v["vc"])),
                    }
                    for k, v in self.puts.items()
                },
                "attendu": self.attendu,
                "relation": self.relation,
                "conflits_detectes": self.conflits_detectes,
                "elapsed": (
                    (self.converged_at or time.time()) - self.conflict_at
                    if self.conflict_at
                    else None
                ),
                "theorique": math.ceil(math.log2(self.n)) if self.n > 1 else 0,
                "val_a": VAL_A,
                "val_b": VAL_B,
                "key": CLE,
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
            self.ids = []
            self.converged_at = None
            self.conflict_at = None
            self.puts = {}
            self.attendu = None
            self.relation = None
            self.conflits_detectes = 0
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
            self.ids = [node_label(i) for i in range(n)]
            self.stores = [{} for _ in range(n)]
            self.tours = [0] * n
            self.running = True
            self.converged_at = None
            self.conflict_at = None
            self.puts = {}
            self.attendu = None
            self.relation = None
            self.conflits_detectes = 0
        self._broadcast({"type": "started", "n": n, "interval": interval, "ids": list(self.ids)})
        self._broadcast(self._snapshot_event())
        for i in range(n):
            t = threading.Thread(
                target=self._boucle_gossip,
                args=(i,),
                daemon=True,
                name=f"gossip-vc-{i}",
            )
            self._workers.append(t)
            t.start()

    def _put_local(self, idx: int, valeur: str) -> dict:
        """Put local = VC connue + 1 sur mon compteur (comme exposed_put)."""
        with self.lock:
            nid = self.ids[idx]
            ancienne = self.stores[idx].get(CLE)
            vc = dict(ancienne[1]) if ancienne else {}
            vc[nid] = vc.get(nid, 0) + 1
            self.stores[idx][CLE] = (valeur, vc)
            return dict(vc)

    def conflict(
        self,
        source_a: int = 0,
        source_b: int = 1,
        value_a: str = VAL_A,
        value_b: str = VAL_B,
    ) -> None:
        """Deux puts concurrentes → horloges incomparables (vrai conflit)."""
        with self.lock:
            if not self.running or self.n < 2:
                raise ValueError("cluster non pret (N >= 2 requis)")
            if not (0 <= source_a < self.n and 0 <= source_b < self.n):
                raise ValueError("sources invalides")
            if source_a == source_b:
                raise ValueError("les deux sources doivent etre distinctes")
            if value_a == value_b:
                raise ValueError("les deux valeurs doivent etre distinctes")

        barrier = threading.Barrier(2)
        results: dict[str, dict] = {}
        errors: list[BaseException] = []

        def ecrire(idx: int, valeur: str) -> None:
            try:
                barrier.wait(timeout=5)
                vc = self._put_local(idx, valeur)
                results[valeur] = {
                    "source": idx,
                    "source_id": self.ids[idx],
                    "vc": vc,
                }
            except BaseException as exc:
                errors.append(exc)

        t1 = threading.Thread(target=ecrire, args=(source_a, value_a))
        t2 = threading.Thread(target=ecrire, args=(source_b, value_b))
        t1.start()
        t2.start()
        t1.join()
        t2.join()
        if errors:
            raise RuntimeError(f"ecriture concurrente echouee : {errors[0]}")
        if value_a not in results or value_b not in results:
            raise RuntimeError("les deux puts n'ont pas abouti")

        vc_a = results[value_a]["vc"]
        vc_b = results[value_b]["vc"]
        relation = vc_compare(vc_a, vc_b)
        if poids(value_a, vc_a) > poids(value_b, vc_b):
            attendu = value_a
        else:
            attendu = value_b

        with self.lock:
            self.conflict_at = time.time()
            self.converged_at = None
            self.puts = results
            self.attendu = attendu
            self.relation = relation

        self._broadcast(
            {
                "type": "conflict",
                "key": CLE,
                "puts": {
                    k: {
                        "source": v["source"],
                        "source_id": v["source_id"],
                        "vc": vc_format(v["vc"]),
                        "poids": list(poids(k, v["vc"])),
                    }
                    for k, v in results.items()
                },
                "relation": relation,
                "attendu": attendu,
                "poids_a": list(poids(value_a, vc_a)),
                "poids_b": list(poids(value_b, vc_b)),
            }
        )
        self._broadcast(self._snapshot_event())
        self._check_convergence()

    def _integrer(self, dest: int, cle: str, valeur, vc: dict) -> tuple[bool, dict | None]:
        """Comme integrer_version de 3_lww.py. Retourne (changé, meta événement)."""
        with self.lock:
            store = self.stores[dest]
            entree = store.get(cle)
            if entree is None:
                store[cle] = (valeur, dict(vc))
                return True, {"kind": "new", "to": valeur, "vc": vc_format(vc)}

            valeur_loc, vc_loc = entree
            rel = vc_compare(vc, vc_loc)

            if rel in ("equal", "before"):
                return False, None

            if rel == "after":
                store[cle] = (valeur, dict(vc))
                return True, {
                    "kind": "dominates",
                    "from": valeur_loc,
                    "to": valeur,
                    "vc": vc_format(vc),
                }

            # concurrent
            self.conflits_detectes += 1
            if poids(valeur, vc) > poids(valeur_loc, vc_loc):
                gagnant, perdant = valeur, valeur_loc
            else:
                gagnant, perdant = valeur_loc, valeur
            vc_absorbe = vc_merge(vc, vc_loc)
            store[cle] = (gagnant, vc_absorbe)
            return True, {
                "kind": "conflict",
                "from": perdant,
                "to": gagnant,
                "vc": vc_format(vc_absorbe),
                "vc_recu": vc_format(vc),
                "vc_local": vc_format(vc_loc),
            }

    def _digest(self, idx: int) -> list[tuple]:
        with self.lock:
            return [
                (c, v, vc_en_tuple(vc))
                for c, (v, vc) in self.stores[idx].items()
            ]

    def _merge_etat(self, dest: int, etat: list[tuple]) -> tuple[int, list]:
        maj = 0
        events: list[dict] = []
        for cle, valeur, vc_tuple in etat:
            changed, meta = self._integrer(dest, str(cle), valeur, dict(vc_tuple))
            if changed and meta:
                maj += 1
                events.append(
                    {
                        "node": dest,
                        "node_id": self.ids[dest],
                        **meta,
                    }
                )
        return maj, events

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

            etat_local = self._digest(idx)
            maj_peer, ev_peer = self._merge_etat(peer, etat_local)
            etat_peer = self._digest(peer)
            maj_local, ev_local = self._merge_etat(idx, etat_peer)

            with self.lock:
                self.tours[idx] += 1
                tour = self.tours[idx]
                valeurs = self._valeurs()
                vcs = [vc_format(v) if v else None for v in self._vcs()]

            self._broadcast(
                {
                    "type": "gossip",
                    "from": idx,
                    "to": peer,
                    "from_id": self.ids[idx],
                    "to_id": self.ids[peer],
                    "tour": tour,
                    "maj_peer": maj_peer,
                    "maj_local": maj_local,
                    "events": ev_peer + ev_local,
                    "valeurs": valeurs,
                    "vcs": vcs,
                }
            )
            self._broadcast(self._snapshot_event())
            self._check_convergence()

            if self._stop.wait(interval):
                break

    def _check_convergence(self) -> None:
        with self.lock:
            if (
                not self.running
                or self.conflict_at is None
                or self.converged_at is not None
            ):
                return
            if self.n == 0 or self.attendu is None:
                return
            valeurs = self._valeurs()
            if any(v is None for v in valeurs):
                return
            if not all(v == self.attendu for v in valeurs):
                return
            gagnant = self.attendu
            self.converged_at = time.time()
            elapsed = self.converged_at - self.conflict_at
            perdant = VAL_B if gagnant == VAL_A else VAL_A
            event = {
                "type": "converged",
                "elapsed": elapsed,
                "tours_estimes": elapsed / self.interval if self.interval else 0,
                "gagnant": gagnant,
                "perdant": perdant,
                "attendu": self.attendu,
                "ok": True,
                "relation": self.relation,
                "conflits_detectes": self.conflits_detectes,
                "theorique": math.ceil(math.log2(self.n)) if self.n > 1 else 0,
                "n": self.n,
                "interval": self.interval,
            }
        self._broadcast(event)
        self._broadcast(self._snapshot_event())


SIM = ClusterVC()


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt: str, *args) -> None:
        print(f"[visu-vc] {self.address_string()} {fmt % args}", flush=True)

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
                self.send_error(404, "visu_lww.html introuvable")
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
            if path == "/api/conflict":
                SIM.conflict(
                    source_a=int(data.get("source_a", 0)),
                    source_b=int(data.get("source_b", 1)),
                    value_a=str(data.get("value_a", VAL_A)),
                    value_b=str(data.get("value_b", VAL_B)),
                )
                self._json(200, {"ok": True, "state": SIM._snapshot_event()})
                return
            if path == "/api/stop":
                SIM.stop()
                self._json(200, {"ok": True, "state": SIM._snapshot_event()})
                return
        except (ValueError, TypeError, RuntimeError) as exc:
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
    parser = argparse.ArgumentParser(
        description="Visualisation conflits / horloges vectorielles"
    )
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    parser.add_argument("--no-browser", action="store_true")
    args = parser.parse_args()

    if not HTML_PATH.is_file():
        raise SystemExit(f"Fichier manquant : {HTML_PATH}")

    server = ThreadingHTTPServer((HOST, args.port), Handler)
    url = f"http://{HOST}:{args.port}/"
    print(f"[visu-vc] ecoute sur {url}", flush=True)
    if not args.no_browser:
        threading.Timer(0.6, lambda: webbrowser.open(url)).start()
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n[visu-vc] arret", flush=True)
        SIM.stop()
        server.shutdown()


if __name__ == "__main__":
    main()
