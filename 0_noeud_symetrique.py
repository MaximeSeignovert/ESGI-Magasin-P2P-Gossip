"""
Phase 0 — Le nœud symétrique

Chaque processus est à la fois serveur RPyC (écoute) et client (contacte ses pairs).
Aucun maître : la communication est bidirectionnelle.

Usage (deux terminaux) :
  python 0_noeud_symetrique.py --port 18000 --peers 18001
  python 0_noeud_symetrique.py --port 18001 --peers 18000

Chaque nœud appelle périodiquement ses pairs ; on voit les requêtes croisées.
"""

from __future__ import annotations

import argparse
import socket
import threading
import time

import rpyc
from rpyc.utils.server import ThreadedServer


HOST = "127.0.0.1"


def log(msg: str) -> None:
    print(msg, flush=True)


class NoeudService(rpyc.Service):
    """Service exposé par le nœud : les pairs appellent ces méthodes."""

    node_id = "N"
    port = 0
    appel_recus = 0

    def on_connect(self, conn):
        log(f"[{self.node_id}] connexion entrante")

    def on_disconnect(self, conn):
        log(f"[{self.node_id}] deconnexion")

    def exposed_whoami(self) -> dict:
        """Identité du nœud distant."""
        NoeudService.appel_recus += 1
        return {
            "id": self.node_id,
            "port": self.port,
            "host": HOST,
            "appels_recus": NoeudService.appel_recus,
        }

    def exposed_ping(self, from_id: str) -> str:
        """Écho simple pour prouver qu'on répond aux pairs."""
        NoeudService.appel_recus += 1
        msg = f"pong de {self.node_id} (port {self.port}) vers {from_id}"
        log(f"[{self.node_id}] ping recu de {from_id}")
        return msg


def contacter_pairs(node_id: str, peers: list[int], interval: float = 2.0) -> None:
    """Boucle client : contacte chaque pair à tour de rôle."""
    time.sleep(1.0)
    i = 0
    while True:
        if not peers:
            time.sleep(interval)
            continue
        port = peers[i % len(peers)]
        i += 1
        try:
            conn = rpyc.connect(HOST, port)
            try:
                info = conn.root.whoami()
                reponse = conn.root.ping(node_id)
                log(
                    f"[{node_id}] -> pair {port} : whoami={info['id']} "
                    f"(appels_recus={info['appels_recus']}) | {reponse}"
                )
            finally:
                conn.close()
        except (ConnectionRefusedError, OSError, EOFError, socket.error) as exc:
            log(f"[{node_id}] pair {port} injoignable ({exc.__class__.__name__})")
        time.sleep(interval)


def main() -> None:
    parser = argparse.ArgumentParser(description="Phase 0 - noeud RPyC symetrique")
    parser.add_argument("--port", type=int, required=True, help="Port d'ecoute (ex. 18000)")
    parser.add_argument(
        "--id",
        type=str,
        default=None,
        help="Identifiant du noeud (defaut : N-<port>)",
    )
    parser.add_argument(
        "--peers",
        type=int,
        nargs="*",
        default=[],
        help="Ports des pairs a contacter (ex. 18001 18002)",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=2.0,
        help="Secondes entre deux contacts client (defaut : 2)",
    )
    args = parser.parse_args()

    node_id = args.id or f"N-{args.port}"
    peers = [p for p in args.peers if p != args.port]

    NoeudService.node_id = node_id
    NoeudService.port = args.port
    NoeudService.appel_recus = 0

    server = ThreadedServer(
        NoeudService,
        hostname=HOST,
        port=args.port,
        protocol_config={"allow_public_attrs": True},
    )

    # Client dans un thread démon : même programme écoute ET contacte
    client_thread = threading.Thread(
        target=contacter_pairs,
        args=(node_id, peers, args.interval),
        daemon=True,
        name=f"client-{node_id}",
    )
    client_thread.start()

    log(f"[{node_id}] serveur RPyC sur {HOST}:{args.port}")
    log(f"[{node_id}] pairs configures : {peers or '(aucun)'}")
    log(f"[{node_id}] Ctrl-C pour arreter")
    try:
        server.start()
    except KeyboardInterrupt:
        log(f"\n[{node_id}] arret")
        server.close()


if __name__ == "__main__":
    main()
