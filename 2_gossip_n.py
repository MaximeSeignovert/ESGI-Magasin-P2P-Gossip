"""
Phase 2 — N nœuds & convergence

Même store + digest/merge (LWW) que la phase 1, mais :
  - chaque tour, un pair est choisi AU HASARD (pas en round-robin)
  - un compteur de tours de gossip est exposé (pour mesurer la convergence)

Usage (ex. 4 nœuds, mesh complet) :
  python 2_gossip_n.py --port 18000 --peers 18001 18002 18003
  python 2_gossip_n.py --port 18001 --peers 18000 18002 18003
  python 2_gossip_n.py --port 18002 --peers 18000 18001 18003
  python 2_gossip_n.py --port 18003 --peers 18000 18001 18002

Puis écrire sur un nœud (autre terminal) :
  python -c "import rpyc; c=rpyc.connect('127.0.0.1',18000); c.root.put('fruit','pomme')"

Ou lancer le cluster + mesurer avec : mesure_convergence.py
"""

from __future__ import annotations

import argparse
import random
import socket
import threading
import time

import rpyc
from rpyc.utils.server import ThreadedServer


HOST = "127.0.0.1"


def log(msg: str) -> None:
    print(msg, flush=True)


class NoeudService(rpyc.Service):
    """Service exposé : put/get, digest/merge, et compteur de tours."""

    node_id = "N"
    port = 0

    store: dict[str, tuple[object, float]] = {}
    verrou = threading.Lock()
    nb_tours = 0  # nombre de tours de gossip déjà effectués

    # ---- API magasin ----

    def exposed_put(self, cle: str, valeur) -> float:
        ts = time.time()
        with NoeudService.verrou:
            NoeudService.store[cle] = (valeur, ts)
        log(f"[{self.node_id}] put {cle}={valeur!r} (ts={ts:.3f})")
        return ts

    def exposed_get(self, cle: str):
        with NoeudService.verrou:
            return NoeudService.store.get(cle)

    def exposed_get_tour(self) -> int:
        with NoeudService.verrou:
            return NoeudService.nb_tours

    def exposed_keys(self) -> list:
        with NoeudService.verrou:
            return list(NoeudService.store.keys())

    # ---- API gossip ----

    def exposed_digest(self) -> list:
        with NoeudService.verrou:
            return [(cle, valeur, ts) for cle, (valeur, ts) in NoeudService.store.items()]

    def exposed_merge(self, etat_recu) -> int:
        return fusionner(self.node_id, etat_recu)


def fusionner(node_id: str, etat_recu) -> int:
    """Fusion last-write-wins."""
    maj = 0
    for cle, valeur, ts in list(etat_recu):
        cle, ts = str(cle), float(ts)
        with NoeudService.verrou:
            local = NoeudService.store.get(cle)
            if local is None or ts > local[1]:
                NoeudService.store[cle] = (valeur, ts)
                maj += 1
                log(
                    f"[{node_id}] merge : {cle}={valeur!r} (ts={ts:.3f}) "
                    f"{'nouvelle cle' if local is None else 'plus recent'}"
                )
    return maj


def copie_store() -> dict:
    with NoeudService.verrou:
        return dict(NoeudService.store)


def etat_en_triplets() -> list:
    with NoeudService.verrou:
        return [(cle, valeur, ts) for cle, (valeur, ts) in NoeudService.store.items()]


def boucle_gossip(node_id: str, peers: list[int], interval: float) -> None:
    """À chaque tour : un pair au hasard, push puis pull."""
    time.sleep(1.0)
    while True:
        if not peers:
            time.sleep(interval)
            continue
        port = random.choice(peers)
        try:
            conn = rpyc.connect(HOST, port)
            try:
                conn.root.merge(etat_en_triplets())
                recus = fusionner(node_id, conn.root.digest())
                with NoeudService.verrou:
                    NoeudService.nb_tours += 1
                    tour = NoeudService.nb_tours
                log(
                    f"[{node_id}] tour={tour} gossip avec {port} : "
                    f"{recus} maj recue(s), store={format_store()}"
                )
            finally:
                conn.close()
        except (ConnectionRefusedError, OSError, EOFError, socket.error) as exc:
            with NoeudService.verrou:
                NoeudService.nb_tours += 1
                tour = NoeudService.nb_tours
            log(
                f"[{node_id}] tour={tour} pair {port} injoignable "
                f"({exc.__class__.__name__})"
            )
        time.sleep(interval)


def format_store() -> str:
    etat = copie_store()
    if not etat:
        return "{}"
    return "{" + ", ".join(f"{c}={v!r}" for c, (v, _) in sorted(etat.items())) + "}"


def main() -> None:
    parser = argparse.ArgumentParser(description="Phase 2 - gossip N noeuds (pair aleatoire)")
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
        help="Ports des pairs a gossiper",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=1.0,
        help="Secondes entre deux tours de gossip (defaut : 1)",
    )
    parser.add_argument(
        "--put",
        type=str,
        nargs="*",
        default=[],
        metavar="CLE=VALEUR",
        help="Ecritures initiales (ex. --put fruit=pomme)",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Graine RNG (reproductibilite des choix de pairs)",
    )
    args = parser.parse_args()

    if args.seed is not None:
        random.seed(args.seed)

    node_id = args.id or f"N-{args.port}"
    peers = [p for p in args.peers if p != args.port]

    NoeudService.node_id = node_id
    NoeudService.port = args.port
    NoeudService.store = {}
    NoeudService.verrou = threading.Lock()
    NoeudService.nb_tours = 0

    for paire in args.put:
        cle, _, valeur = paire.partition("=")
        ts = time.time()
        NoeudService.store[cle] = (valeur, ts)
        log(f"[{node_id}] put initial {cle}={valeur!r} (ts={ts:.3f})")

    server = ThreadedServer(
        NoeudService,
        hostname=HOST,
        port=args.port,
        protocol_config={"allow_public_attrs": True},
    )

    gossip_thread = threading.Thread(
        target=boucle_gossip,
        args=(node_id, peers, args.interval),
        daemon=True,
        name=f"gossip-{node_id}",
    )
    gossip_thread.start()

    log(f"[{node_id}] serveur RPyC sur {HOST}:{args.port}")
    log(f"[{node_id}] pairs configures : {peers or '(aucun)'}")
    log(f"[{node_id}] store initial : {format_store()}")
    log(f"[{node_id}] Ctrl-C pour arreter")
    try:
        server.start()
    except KeyboardInterrupt:
        log(f"\n[{node_id}] arret | store final : {format_store()}")
        server.close()


if __name__ == "__main__":
    main()
