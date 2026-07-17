"""
Phase 3 — Conflits & cohérence à terme (last-write-wins à l'épreuve)

Le merge de la Phase 1 implémente déjà LWW : ici on le met à l'épreuve.
On écrit deux valeurs différentes pour la MÊME clé sur deux nœuds distincts,
presque en même temps, et on observe le gossip faire converger tout le monde
vers une seule valeur : la plus récente selon l'horodatage.

Usage :
  python 3_lww.py                    # joue tout le scénario (lance ses nœuds)
  python 3_lww.py --nodes 4          # idem avec 4 nœuds
  python 3_lww.py --node --port ...  # mode nœud individuel (usage interne)

Règle de convergence : une entrée reçue remplace la locale si son horodatage
est plus grand — et à horodatage égal, la valeur la plus grande gagne.
Cette règle est un ordre TOTAL appliqué partout pareil : les nœuds convergent
au lieu d'osciller, même en cas d'égalité parfaite.
"""

from __future__ import annotations

import argparse
import os
import random
import socket
import subprocess
import sys
import threading
import time

import rpyc
from rpyc.utils.server import ThreadedServer


HOST = "127.0.0.1"
CLE_CONFLIT = "couleur"


def log(msg: str) -> None:
    print(msg, flush=True)


# ---------------------------------------------------------------------------
# Mode nœud : le nœud de la Phase 1 (store + gossip), en plus silencieux
# ---------------------------------------------------------------------------

class NoeudService(rpyc.Service):
    """Store clé -> (valeur, horodatage) : put/get pour les clients, digest/merge pour le gossip."""

    node_id = "N"
    store: dict[str, tuple[object, float]] = {}
    verrou = threading.Lock()

    # Écrit une valeur dans le store avec l'horodatage du moment.
    # Retourne le timestamp pour que le client sache quand c'a ete fait.
    def exposed_put(self, cle: str, valeur) -> float:
        ts = time.time()
        with NoeudService.verrou:
            NoeudService.store[cle] = (valeur, ts)
        log(f"[{self.node_id}] put {cle}={valeur!r} (ts={ts:.6f})")
        return ts

    # Lit une valeur : retourne (valeur, horodatage) ou None si la cle n'existe pas.
    def exposed_get(self, cle: str):
        with NoeudService.verrou:
            return NoeudService.store.get(cle)

    # Donne mon state complet : une liste de triplets (cle, valeur, ts).
    # C'est mon "digest" pour que les autres noeuds voient ce que j'ai.
    def exposed_digest(self) -> list:
        """État complet en triplets (cle, valeur, ts) — passés par valeur par RPyC."""
        with NoeudService.verrou:
            return [(cle, valeur, ts) for cle, (valeur, ts) in NoeudService.store.items()]

    # Reçoit l'état d'un autre noeud et le fusionne dans le mien (last-write-wins).
    def exposed_merge(self, etat_recu) -> int:
        return fusionner(self.node_id, etat_recu)


# LA REGLE LWW : pour chaque cle reçue, on compare les horodatages.
# - Si ts_recu > ts_local → on remplace (le plus recent gagne)
# - Si ts_recu == ts_local → on compare les valeurs (tie-break deterministe)
# - Sinon on garde la notre.
# Resultat : tout le monde converge vers LA MEME valeur, sans osciller.
def fusionner(node_id: str, etat_recu) -> int:
    """Fusion last-write-wins : le plus grand (ts, valeur) gagne, partout pareil.

    Le tie-break sur la valeur rend l'ordre TOTAL : deux nœuds qui appliquent
    cette règle sur les mêmes entrées finissent forcément d'accord.
    """
    maj = 0
    for cle, valeur, ts in list(etat_recu):
        cle, ts = str(cle), float(ts)
        with NoeudService.verrou:
            local = NoeudService.store.get(cle)
            gagne = (
                local is None
                or ts > local[1]
                or (ts == local[1] and str(valeur) > str(local[0]))
            )
            if gagne:
                NoeudService.store[cle] = (valeur, ts)
                maj += 1
                if local is not None:
                    log(f"[{node_id}] LWW : {cle} : {local[0]!r} (ts={local[1]:.6f}) "
                        f"ecrase par {valeur!r} (ts={ts:.6f})")
    return maj


# Copie mon store en triplets (cle, valeur, ts) prets a envoyer par gossip.
def etat_en_triplets() -> list:
    with NoeudService.verrou:
        return [(cle, valeur, ts) for cle, (valeur, ts) in NoeudService.store.items()]


# La boucle infinie du gossip : chaque T secondes, je contacte un pair au hasard,
# je lui envoie mon state (push), puis je recois le sien et le fusionne (pull).
# Un pair injoignable ? On l'ignore, on reessaiera la prochaine fois.
def boucle_gossip(node_id: str, peers: list[int], interval: float) -> None:
    """À chaque tour : un pair AU HASARD, push mon état puis pull le sien."""
    time.sleep(0.5)
    while True:
        if peers:
            port = random.choice(peers)
            try:
                conn = rpyc.connect(HOST, port)
                try:
                    conn.root.merge(etat_en_triplets())
                    fusionner(node_id, conn.root.digest())
                finally:
                    conn.close()
            except (ConnectionRefusedError, OSError, EOFError, socket.error):
                pass  # pair injoignable : on réessaiera au tour suivant
        time.sleep(interval)


# Lance un noeud individuel : configure son store, demarre le serveur RPyC,
# et la boucle gossip en arriere-plan.
def lancer_noeud(args) -> None:
    node_id = args.id or f"N-{args.port}"
    NoeudService.node_id = node_id
    NoeudService.store = {}
    NoeudService.verrou = threading.Lock()
    peers = [p for p in args.peers if p != args.port]

    server = ThreadedServer(
        NoeudService,
        hostname=HOST,
        port=args.port,
        protocol_config={"allow_public_attrs": True},
    )
    threading.Thread(
        target=boucle_gossip,
        args=(node_id, peers, args.interval),
        daemon=True,
        name=f"gossip-{node_id}",
    ).start()
    log(f"[{node_id}] noeud pret sur {HOST}:{args.port} (pairs : {peers})")
    try:
        server.start()
    except KeyboardInterrupt:
        server.close()


# ---------------------------------------------------------------------------
# Mode scénario : provoquer le conflit et observer la convergence
# ---------------------------------------------------------------------------

# Verifie qu'un port n'est pas deja utilise (essaie de se connecter dessus).
def port_occupe(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex((HOST, port)) == 0


# Attend que le noeud demarre : essaie de se connecter jusqu'a timeout.
def attendre_serveur(port: int, timeout: float = 10.0) -> None:
    fin = time.time() + timeout
    while time.time() < fin:
        try:
            rpyc.connect(HOST, port).close()
            return
        except (ConnectionRefusedError, OSError):
            time.sleep(0.1)
    raise RuntimeError(f"le noeud {port} n'a pas demarre")


# Fait un RPC pour lire une cle sur un noeud distant. Retourne (valeur, ts) ou None.
def lire_cle(port: int, cle: str):
    conn = rpyc.connect(HOST, port)
    try:
        return conn.root.get(cle)
    finally:
        conn.close()


# LE SCENARIO COMPLET :
# 1. Lance N noeuds en sous-processus
# 2. Fait deux ecritures concurrentes sur la meme cle (couleur=rouge, couleur=bleu)
# 3. Lit tous les noeuds en boucle pour observer la convergence
# 4. Verifie que tout le monde a converge vers la valeur la plus recente (LWW)
def scenario(nb_noeuds: int, interval: float, base_port: int) -> None:
    ports = [base_port + i for i in range(nb_noeuds)]
    ids = [chr(ord("A") + i) for i in range(nb_noeuds)]
    procs = []

    occupes = [p for p in ports if port_occupe(p)]
    if occupes:
        log(f"ERREUR : ports deja utilises : {occupes} (d'autres noeuds tournent ?)\n"
            f"Fermez-les ou relancez avec --base-port (ex. --base-port {base_port + 100})")
        sys.exit(1)

    log(f"=== Phase 3 : conflit LWW sur {nb_noeuds} noeuds (gossip toutes les {interval}s) ===\n")
    for node_id, port in zip(ids, ports):
        peers = [str(p) for p in ports if p != port]
        procs.append(subprocess.Popen(
            [sys.executable, os.path.abspath(__file__), "--node",
             "--port", str(port), "--id", node_id,
             "--peers", *peers, "--interval", str(interval)],
        ))
    try:
        for port in ports:
            attendre_serveur(port)
        time.sleep(0.5)

        # -- Le conflit : deux écritures concurrentes de la même clé --------
        # Une barrière libère les deux threads en même temps : les puts
        # partent quasi simultanément sur deux nœuds différents.
        log(f"\n--- Ecritures concurrentes de '{CLE_CONFLIT}' sur A et B ---")
        horodatages: dict[str, float] = {}
        barriere = threading.Barrier(2)

        def ecrire(port: int, valeur: str) -> None:
            conn = rpyc.connect(HOST, port)
            try:
                barriere.wait()
                horodatages[valeur] = conn.root.put(CLE_CONFLIT, valeur)
            finally:
                conn.close()

        t1 = threading.Thread(target=ecrire, args=(ports[0], "rouge"))
        t2 = threading.Thread(target=ecrire, args=(ports[1], "bleu"))
        t1.start(); t2.start()
        t1.join(); t2.join()

        ecart = abs(horodatages["rouge"] - horodatages["bleu"]) * 1000
        attendu = max(horodatages, key=lambda v: (horodatages[v], v))
        log(f"\nrouge sur A : ts={horodatages['rouge']:.6f}")
        log(f"bleu  sur B : ts={horodatages['bleu']:.6f}")
        log(f"ecart : {ecart:.3f} ms -> le plus recent est {attendu!r} : "
            f"LWW doit le faire gagner PARTOUT\n")

        # -- Observation : on relit tous les nœuds jusqu'à convergence ------
        log("--- Propagation (lecture de tous les noeuds) ---")
        debut = time.time()
        convergence = None
        while time.time() - debut < 30:
            vals = {}
            for node_id, port in zip(ids, ports):
                entree = lire_cle(port, CLE_CONFLIT)
                vals[node_id] = None if entree is None else entree[0]
            log(f"t=+{time.time() - debut:4.1f}s  " +
                "  ".join(f"{n}={v or '?'}" for n, v in vals.items()))
            uniques = set(vals.values())
            if len(uniques) == 1 and None not in uniques:
                convergence = time.time() - debut
                break
            time.sleep(interval / 2)

        # -- Verdict --------------------------------------------------------
        if convergence is None:
            log("\nECHEC : pas de convergence en 30 s")
            return
        gagnant = vals[ids[0]]
        log(f"\nconvergence en {convergence:.1f} s : tous les noeuds disent "
            f"{CLE_CONFLIT}={gagnant!r}")
        verdict = "OK (le plus recent a gagne)" if gagnant == attendu else "ANOMALIE"
        log(f"attendu {attendu!r} -> {verdict}")
        perdant = "bleu" if attendu == "rouge" else "rouge"
        log(f"\nA retenir : l'ecriture {perdant!r} a disparu PARTOUT, sans erreur ni "
            f"trace.\nC'est la perte silencieuse de LWW : acceptable pour de la "
            f"presence, pas pour de l'argent.")
    finally:
        for proc in procs:
            proc.terminate()
        for proc in procs:
            proc.wait()


# Point d'entree : deux modes
# - Mode scenario (sans --node) : lance le test complet avec N noeuds
# - Mode noeud (--node) : lance un seul noeud, utilise en interne par scenario
def main() -> None:
    parser = argparse.ArgumentParser(description="Phase 3 - conflits & last-write-wins")
    parser.add_argument("--node", action="store_true", help="Mode noeud individuel (usage interne au scenario)")
    parser.add_argument("--nodes", type=int, default=3, help="Nombre de noeuds du scenario (defaut : 3)")
    parser.add_argument("--port", type=int, default=None, help="(mode --node) port d'ecoute")
    parser.add_argument("--id", type=str, default=None, help="(mode --node) identifiant")
    parser.add_argument("--peers", type=int, nargs="*", default=[], help="(mode --node) ports des pairs")
    parser.add_argument("--interval", type=float, default=1.0, help="Secondes entre deux tours de gossip (defaut : 1)")
    parser.add_argument("--base-port", type=int, default=18000, help="Premier port du scenario (defaut : 18000)")
    args = parser.parse_args()

    if args.node:
        if args.port is None:
            parser.error("--node exige --port")
        lancer_noeud(args)
    else:
        scenario(args.nodes, args.interval, args.base_port)


if __name__ == "__main__":
    main()
