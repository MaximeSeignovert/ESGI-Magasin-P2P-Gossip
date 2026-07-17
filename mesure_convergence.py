"""
Phase 2 — Mesure de convergence

Lance N nœuds (2_gossip_n.py), écrit une clé sur l'un d'eux, puis sonde
tous les nœuds jusqu'à ce que tous connaissent la clé.

Affiche le temps écoulé et une estimation du nombre de tours (~ temps / T).
À observer : la propagation est exponentielle (~ log2(N) tours), pas linéaire.

Usage :
  python mesure_convergence.py --n 8 --interval 0.5
  python mesure_convergence.py --n 16 --interval 0.3 --base-port 18000
"""

from __future__ import annotations

import argparse
import math
import subprocess
import sys
import time
from pathlib import Path

import rpyc


HOST = "127.0.0.1"
SCRIPT = Path(__file__).resolve().parent / "2_gossip_n.py"


def log(msg: str) -> None:
    print(msg, flush=True)


def attendre_noeud(port: int, timeout: float = 15.0) -> None:
    debut = time.time()
    while time.time() - debut < timeout:
        try:
            conn = rpyc.connect(HOST, port)
            conn.close()
            return
        except (ConnectionRefusedError, OSError, EOFError):
            time.sleep(0.1)
    raise TimeoutError(f"noeud {port} pas pret apres {timeout}s")


def get_cle(port: int, cle: str):
    conn = rpyc.connect(HOST, port)
    try:
        return conn.root.get(cle)
    finally:
        conn.close()


def put_cle(port: int, cle: str, valeur: str) -> float:
    conn = rpyc.connect(HOST, port)
    try:
        return float(conn.root.put(cle, valeur))
    finally:
        conn.close()


def tour_noeud(port: int) -> int:
    conn = rpyc.connect(HOST, port)
    try:
        return int(conn.root.get_tour())
    finally:
        conn.close()


def lancer_cluster(n: int, base_port: int, interval: float) -> list[subprocess.Popen]:
    ports = [base_port + i for i in range(n)]
    procs: list[subprocess.Popen] = []
    for port in ports:
        peers = [p for p in ports if p != port]
        cmd = [
            sys.executable,
            "-u",
            str(SCRIPT),
            "--port",
            str(port),
            "--interval",
            str(interval),
            "--peers",
            *[str(p) for p in peers],
        ]
        # stdout/stderr dispersés : on évite de saturer la console de mesure
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        procs.append(proc)
        log(f"[mesure] lance N-{port} (pid={proc.pid}) peers={peers}")
    return procs


def arreter_cluster(procs: list[subprocess.Popen]) -> None:
    for proc in procs:
        if proc.poll() is None:
            proc.terminate()
    deadline = time.time() + 3.0
    for proc in procs:
        reste = max(0.0, deadline - time.time())
        try:
            proc.wait(timeout=reste)
        except subprocess.TimeoutExpired:
            proc.kill()


def mesurer(n: int, base_port: int, interval: float, cle: str, valeur: str,
            poll: float, timeout: float) -> None:
    ports = [base_port + i for i in range(n)]
    procs = lancer_cluster(n, base_port, interval)
    try:
        log(f"[mesure] attente demarrage des {n} noeuds...")
        for port in ports:
            attendre_noeud(port)
        log("[mesure] cluster pret")

        source = ports[0]
        # tours déjà écoulés avant l'écriture (phase de chauffe)
        tours_avant = {p: tour_noeud(p) for p in ports}

        ts = put_cle(source, cle, valeur)
        t0 = time.time()
        log(f"[mesure] put {cle}={valeur!r} sur {source} (ts={ts:.3f})")

        connus = {source}
        while True:
            elapsed = time.time() - t0
            if elapsed > timeout:
                manquants = [p for p in ports if p not in connus]
                raise TimeoutError(
                    f"pas de convergence apres {timeout}s ; "
                    f"manquants={manquants}"
                )

            for port in ports:
                if port in connus:
                    continue
                try:
                    if get_cle(port, cle) is not None:
                        connus.add(port)
                        log(
                            f"[mesure] + {port} connait '{cle}' "
                            f"({len(connus)}/{n}) t={elapsed:.2f}s"
                        )
                except (ConnectionRefusedError, OSError, EOFError):
                    pass

            if len(connus) == n:
                break
            time.sleep(poll)

        elapsed = time.time() - t0
        tours_apres = {p: tour_noeud(p) for p in ports}
        # tours locaux depuis le put (max parmi les nœuds) = borne haute
        delta_tours = max(tours_apres[p] - tours_avant[p] for p in ports)
        # estimation wall-clock : temps / période T
        tours_estimes = elapsed / interval if interval > 0 else float("inf")
        theorique = math.ceil(math.log2(n)) if n > 1 else 0

        log("")
        log("=== Resultat ===")
        log(f"N                  = {n}")
        log(f"intervalle T       = {interval}s")
        log(f"temps convergence  = {elapsed:.3f}s")
        log(f"tours (temps / T)  = {tours_estimes:.2f}")
        log(f"tours (max delta)  = {delta_tours}")
        log(f"ordre theorique    ~ log2(N) = {theorique}")
        log(
            "Observation attendue : convergence en O(log N) tours, "
            "pas en O(N)."
        )
    finally:
        log("[mesure] arret du cluster...")
        arreter_cluster(procs)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Phase 2 - mesurer la convergence du gossip sur N noeuds"
    )
    parser.add_argument("--n", type=int, default=8, help="Nombre de noeuds (defaut : 8)")
    parser.add_argument(
        "--base-port",
        type=int,
        default=18000,
        help="Premier port du cluster (defaut : 18000)",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=0.5,
        help="Periode de gossip T en secondes (defaut : 0.5)",
    )
    parser.add_argument("--key", type=str, default="seed", help="Cle a propager")
    parser.add_argument("--value", type=str, default="hello", help="Valeur a ecrire")
    parser.add_argument(
        "--poll",
        type=float,
        default=0.05,
        help="Intervalle de sondage des noeuds (defaut : 0.05s)",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=60.0,
        help="Timeout max avant echec (defaut : 60s)",
    )
    args = parser.parse_args()

    if args.n < 2:
        parser.error("--n doit etre >= 2")
    if not SCRIPT.is_file():
        parser.error(f"script introuvable : {SCRIPT}")

    mesurer(
        n=args.n,
        base_port=args.base_port,
        interval=args.interval,
        cle=args.key,
        valeur=args.value,
        poll=args.poll,
        timeout=args.timeout,
    )


if __name__ == "__main__":
    main()
