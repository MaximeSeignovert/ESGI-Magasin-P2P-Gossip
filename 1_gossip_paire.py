"""
Phase 1 — Store local + gossip à deux

Chaque nœud tient un dictionnaire clé -> (valeur, horodatage) avec put/get.
Deux méthodes d'échange :
  - digest : donne mon état complet
  - merge  : fusionne l'état reçu (last-write-wins par horodatage)

La boucle de gossip (push puis pull) tourne dans un thread démon :
une écriture faite sur A apparaît sur B au tour suivant.

Usage (deux terminaux) :
  python 1_gossip_paire.py --port 18000 --peers 18001 --put fruit=pomme
  python 1_gossip_paire.py --port 18001 --peers 18000

On voit la clé "fruit" écrite sur A arriver sur B par gossip.
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
    """Service exposé par le nœud : put/get pour les clients, digest/merge pour le gossip."""

    node_id = "N"
    port = 0

    # Store partagé : clé -> (valeur, horodatage). Un seul verrou pour tous
    # (thread serveur et thread gossip y touchent en même temps).
    store: dict[str, tuple[object, float]] = {}
    verrou = threading.Lock()

    # ---- API magasin ----

    def exposed_put(self, cle: str, valeur) -> float:
        """Écrit une valeur, horodatée maintenant. Retourne l'horodatage."""
        ts = time.time()
        with NoeudService.verrou:
            NoeudService.store[cle] = (valeur, ts)
        log(f"[{self.node_id}] put {cle}={valeur!r} (ts={ts:.3f})")
        return ts

    def exposed_get(self, cle: str):
        """Lit une valeur : (valeur, horodatage) ou None si absente."""
        with NoeudService.verrou:
            return NoeudService.store.get(cle)

    # ---- API gossip ----

    def exposed_digest(self) -> list:
        """Donne mon état complet, sous forme de triplets (cle, valeur, ts).

        On échange des tuples de primitifs (passés par valeur par RPyC) plutôt
        qu'un dict, qui arriverait comme netref difficile à copier côté receveur.
        """
        with NoeudService.verrou:
            return [(cle, valeur, ts) for cle, (valeur, ts) in NoeudService.store.items()]

    def exposed_merge(self, etat_recu) -> int:
        """Fusionne l'état reçu dans le mien. Retourne le nombre de clés mises à jour."""
        return fusionner(self.node_id, etat_recu)


def fusionner(node_id: str, etat_recu) -> int:
    """Fusion last-write-wins : on ne garde une entrée reçue que si elle est plus récente.

    `etat_recu` : liste de triplets (cle, valeur, ts), éventuellement un netref RPyC.
    """
    maj = 0
    for cle, valeur, ts in list(etat_recu):
        cle, ts = str(cle), float(ts)
        with NoeudService.verrou:
            local = NoeudService.store.get(cle)
            if local is None or ts > local[1]:
                NoeudService.store[cle] = (valeur, ts)
                maj += 1
                log(f"[{node_id}] merge : {cle}={valeur!r} (ts={ts:.3f}) "
                    f"{'nouvelle cle' if local is None else 'plus recent'}")
    return maj


def copie_store() -> dict:
    with NoeudService.verrou:
        return dict(NoeudService.store)


def etat_en_triplets() -> list:
    """Copie du store sous forme de triplets (cle, valeur, ts), prêts à envoyer."""
    with NoeudService.verrou:
        return [(cle, valeur, ts) for cle, (valeur, ts) in NoeudService.store.items()]


def boucle_gossip(node_id: str, peers: list[int], interval: float) -> None:
    """Boucle de gossip : à chaque tour, push mon état vers un pair puis pull le sien."""
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
                # push : j'envoie mon état, le pair fusionne
                conn.root.merge(etat_en_triplets())
                # pull : je récupère son état et je fusionne chez moi
                recus = fusionner(node_id, conn.root.digest())
                log(f"[{node_id}] gossip avec {port} : {recus} maj recue(s), "
                    f"store={format_store()}")
            finally:
                conn.close()
        except (ConnectionRefusedError, OSError, EOFError, socket.error) as exc:
            # Un pair injoignable n'est pas une erreur fatale : on réessaiera.
            log(f"[{node_id}] pair {port} injoignable ({exc.__class__.__name__})")
        time.sleep(interval)


def format_store() -> str:
    etat = copie_store()
    if not etat:
        return "{}"
    return "{" + ", ".join(f"{c}={v!r}" for c, (v, _) in sorted(etat.items())) + "}"


def main() -> None:
    parser = argparse.ArgumentParser(description="Phase 1 - store local + gossip a deux")
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
        help="Ports des pairs a gossiper (ex. 18001 18002)",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=2.0,
        help="Secondes entre deux tours de gossip (defaut : 2)",
    )
    parser.add_argument(
        "--put",
        type=str,
        nargs="*",
        default=[],
        metavar="CLE=VALEUR",
        help="Ecritures initiales dans le store (ex. --put fruit=pomme prix=3)",
    )
    args = parser.parse_args()

    node_id = args.id or f"N-{args.port}"
    peers = [p for p in args.peers if p != args.port]

    NoeudService.node_id = node_id
    NoeudService.port = args.port
    NoeudService.store = {}
    NoeudService.verrou = threading.Lock()

    # Écritures initiales demandées en ligne de commande
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

    # Boucle gossip dans un thread démon, AVANT de démarrer le serveur (bloquant)
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
