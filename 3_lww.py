"""
Phase 3 — Conflits & cohérence à terme, avec HORLOGES VECTORIELLES

Chaque valeur porte une horloge vectorielle {noeud: compteur}. En comparant
deux horloges on sait si :
  - l'une DOMINE l'autre -> pas de conflit, on garde la plus récente ;
  - aucune ne domine     -> VRAI conflit (écritures concurrentes).

Résolution des conflits : le VECTEUR LE PLUS GRAND gagne (somme des
compteurs, puis tie-breaks déterministes). Le gagnant absorbe l'horloge du
perdant (fusion), donc il domine ensuite les deux branches et se propage
partout : le cluster converge vers UNE valeur, automatiquement.

C'est un compromis entre LWW et les siblings de Dynamo :
  - contrairement à LWW, le critère est causal ("qui a vu le plus
    d'écritures"), insensible aux horloges physiques, et le conflit est
    DETECTE (loggé) au lieu d'être invisible ;
  - contrairement aux siblings, une écriture perd quand même — mais la même
    partout, de façon déterministe.

Usage :
  python 3_lww.py                    # joue tout le scénario (lance ses nœuds)
  python 3_lww.py --nodes 4          # idem avec 4 nœuds
  python 3_lww.py --node --port ...  # mode nœud individuel (usage interne)
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
# Horloges vectorielles : de simples dicts {noeud_id: compteur}
# ---------------------------------------------------------------------------

# Compare deux horloges. Retourne :
#   "after"      : a domine b (a connait tout ce que b connait, et plus)
#   "before"     : b domine a
#   "equal"      : identiques
#   "concurrent" : aucune ne domine l'autre -> VRAI conflit
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


# Fusionne deux horloges : max composante par composante.
def vc_merge(a: dict, b: dict) -> dict:
    return {k: max(a.get(k, 0), b.get(k, 0)) for k in set(a) | set(b)}


# Version affichable : {A:1,B:2}
def vc_format(vc: dict) -> str:
    return "{" + ",".join(f"{k}:{v}" for k, v in sorted(vc.items())) + "}"


# Version transportable par RPyC : tuple de tuples ((id, compteur), ...).
# Tout immuable -> passé PAR VALEUR (un dict arriverait en netref piégeux).
def vc_en_tuple(vc: dict) -> tuple:
    return tuple(sorted(vc.items()))


# Poids d'une version pour la résolution de conflit : le "vecteur le plus
# grand" gagne. Ordre TOTAL (appliqué partout pareil -> convergence garantie) :
#   1. somme des compteurs (qui a vu le plus d'écritures)
#   2. à égalité : comparaison lexicographique du vecteur
#   3. à égalité encore : la valeur elle-même
def poids(valeur, vc: dict) -> tuple:
    return (sum(vc.values()), vc_en_tuple(vc), str(valeur))


# ---------------------------------------------------------------------------
# Mode nœud : store versionné + gossip
# ---------------------------------------------------------------------------

class NoeudService(rpyc.Service):
    """Store clé -> (valeur, horloge vectorielle) ; put/get pour les clients, digest/merge pour le gossip."""

    node_id = "N"
    store: dict[str, tuple[object, dict]] = {}
    verrou = threading.Lock()

    # Écrit une valeur. Son horloge = celle de la version connue ici + 1 sur
    # MON compteur : elle domine ce que ce nœud avait vu.
    def exposed_put(self, cle: str, valeur) -> tuple:
        with NoeudService.verrou:
            ancienne = NoeudService.store.get(cle)
            vc = dict(ancienne[1]) if ancienne else {}
            vc[self.node_id] = vc.get(self.node_id, 0) + 1
            NoeudService.store[cle] = (valeur, vc)
        log(f"[{self.node_id}] put {cle}={valeur!r} vc={vc_format(vc)}")
        return vc_en_tuple(vc)

    # Lit une clé : (valeur, horloge_tuple) ou None si absente.
    def exposed_get(self, cle: str):
        with NoeudService.verrou:
            entree = NoeudService.store.get(cle)
            if entree is None:
                return None
            valeur, vc = entree
            return (valeur, vc_en_tuple(vc))

    # Donne mon état complet : une ligne (cle, valeur, horloge_tuple) par clé.
    def exposed_digest(self) -> list:
        with NoeudService.verrou:
            return [
                (cle, valeur, vc_en_tuple(vc))
                for cle, (valeur, vc) in NoeudService.store.items()
            ]

    # Reçoit l'état d'un pair et l'intègre version par version.
    def exposed_merge(self, etat_recu) -> int:
        return fusionner(self.node_id, etat_recu)


# Intègre UNE version reçue. Retourne True si le store a changé. Règles :
#   - reçue égale ou dominée         -> ignorée ;
#   - reçue domine la locale         -> remplacement (mise à jour normale) ;
#   - concurrentes (VRAI conflit)    -> le vecteur le plus grand gagne, et le
#     gagnant ABSORBE l'horloge du perdant (fusion) : il domine désormais les
#     deux branches et balaiera le perdant partout où il passera.
def integrer_version(node_id: str, cle: str, valeur, vc: dict) -> bool:
    with NoeudService.verrou:
        entree = NoeudService.store.get(cle)
        if entree is None:
            NoeudService.store[cle] = (valeur, vc)
            log(f"[{node_id}] merge : {cle}={valeur!r} vc={vc_format(vc)} (nouvelle cle)")
            return True

        valeur_loc, vc_loc = entree
        rel = vc_compare(vc, vc_loc)

        if rel in ("equal", "before"):
            return False  # déjà connue ou obsolète : rien ne change

        if rel == "after":
            NoeudService.store[cle] = (valeur, vc)
            log(f"[{node_id}] merge : {cle}={valeur!r} vc={vc_format(vc)} "
                f"(domine l'ancienne version)")
            return True

        # rel == "concurrent" : conflit détecté -> le plus grand vecteur gagne
        if poids(valeur, vc) > poids(valeur_loc, vc_loc):
            gagnant, vc_gagnant = valeur, vc
        else:
            gagnant, vc_gagnant = valeur_loc, vc_loc
        vc_absorbe = vc_merge(vc, vc_loc)
        NoeudService.store[cle] = (gagnant, vc_absorbe)
        log(f"[{node_id}] CONFLIT sur {cle} : {valeur_loc!r} {vc_format(vc_loc)} "
            f"vs {valeur!r} {vc_format(vc)} -> {gagnant!r} gagne "
            f"(vecteur le plus grand), horloge fusionnee {vc_format(vc_absorbe)}")
        return True


# Intègre un état complet reçu (liste de lignes). Retourne le nb de changements.
def fusionner(node_id: str, etat_recu) -> int:
    maj = 0
    for cle, valeur, vc_tuple in list(etat_recu):
        if integrer_version(node_id, str(cle), valeur, dict(vc_tuple)):
            maj += 1
    return maj


# Copie mon store au format d'échange (une ligne par clé).
def etat_en_lignes() -> list:
    with NoeudService.verrou:
        return [
            (cle, valeur, vc_en_tuple(vc))
            for cle, (valeur, vc) in NoeudService.store.items()
        ]


# La boucle infinie du gossip : toutes les T secondes, un pair AU HASARD,
# push mon état puis pull le sien. Pair injoignable = on réessaiera.
def boucle_gossip(node_id: str, peers: list[int], interval: float) -> None:
    time.sleep(0.5)
    while True:
        if peers:
            port = random.choice(peers)
            try:
                conn = rpyc.connect(HOST, port)
                try:
                    conn.root.merge(etat_en_lignes())
                    fusionner(node_id, conn.root.digest())
                finally:
                    conn.close()
            except (ConnectionRefusedError, OSError, EOFError, socket.error):
                pass
        time.sleep(interval)


# Lance un nœud individuel : store vierge, serveur RPyC + thread gossip démon.
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
# Mode scénario : conflit -> détection -> résolution par le plus grand vecteur
# ---------------------------------------------------------------------------

# Vérifie qu'un port n'est pas déjà pris (par ex. par un nœud d'une autre phase).
def port_occupe(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex((HOST, port)) == 0


# Attend qu'un nœud accepte les connexions (ou lève une erreur après timeout).
def attendre_serveur(port: int, timeout: float = 10.0) -> None:
    fin = time.time() + timeout
    while time.time() < fin:
        try:
            rpyc.connect(HOST, port).close()
            return
        except (ConnectionRefusedError, OSError):
            time.sleep(0.1)
    raise RuntimeError(f"le noeud {port} n'a pas demarre")


# Lit une clé sur un nœud : la valeur (str) ou None si absente.
def lire_valeur(port: int, cle: str):
    conn = rpyc.connect(HOST, port)
    try:
        entree = conn.root.get(cle)
        if entree is None:
            return None
        return str(entree[0])
    finally:
        conn.close()


# Relit la clé sur tous les nœuds toutes les demi-périodes jusqu'à ce qu'ils
# disent tous la valeur attendue. Retourne le temps de convergence (ou None).
def observer(ids, ports, attendu: str, interval: float, timeout: float = 30.0):
    debut = time.time()
    while time.time() - debut < timeout:
        vals = {n: lire_valeur(p, CLE_CONFLIT) for n, p in zip(ids, ports)}
        log(f"t=+{time.time() - debut:4.1f}s  "
            + "  ".join(f"{n}={v or '?'}" for n, v in vals.items()))
        if all(v == attendu for v in vals.values()):
            return time.time() - debut
        time.sleep(interval / 2)
    return None


# LE SCENARIO COMPLET :
# 1. Lance N nœuds en sous-processus
# 2. Deux écritures concurrentes (couleur=rouge sur A, couleur=bleu sur B)
# 3. Vérifie que les horloges {A:1} et {B:1} sont incomparables (vrai conflit)
# 4. Prédit le gagnant avec la règle "vecteur le plus grand" et observe que
#    tout le cluster converge vers lui
def scenario(nb_noeuds: int, interval: float, base_port: int) -> None:
    ports = [base_port + i for i in range(nb_noeuds)]
    ids = [chr(ord("A") + i) for i in range(nb_noeuds)]
    procs = []

    occupes = [p for p in ports if port_occupe(p)]
    if occupes:
        log(f"ERREUR : ports deja utilises : {occupes} (d'autres noeuds tournent ?)\n"
            f"Fermez-les ou relancez avec --base-port (ex. --base-port {base_port + 100})")
        sys.exit(1)

    log(f"=== Phase 3 : conflit + horloges vectorielles sur {nb_noeuds} noeuds "
        f"(gossip toutes les {interval}s) ===\n")
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

        # -- 1. Le conflit : deux écritures concurrentes de la même clé ------
        log(f"\n--- Ecritures concurrentes de '{CLE_CONFLIT}' sur A et B ---")
        horloges: dict[str, tuple] = {}
        barriere = threading.Barrier(2)

        def ecrire(port: int, valeur: str) -> None:
            conn = rpyc.connect(HOST, port)
            try:
                barriere.wait()
                horloges[valeur] = tuple(conn.root.put(CLE_CONFLIT, valeur))
            finally:
                conn.close()

        t1 = threading.Thread(target=ecrire, args=(ports[0], "rouge"))
        t2 = threading.Thread(target=ecrire, args=(ports[1], "bleu"))
        t1.start(); t2.start()
        t1.join(); t2.join()

        vc_rouge, vc_bleu = dict(horloges["rouge"]), dict(horloges["bleu"])
        log(f"\nrouge sur A : vc={vc_format(vc_rouge)}")
        log(f"bleu  sur B : vc={vc_format(vc_bleu)}")
        relation = vc_compare(vc_rouge, vc_bleu)
        log(f"comparaison des horloges : {relation}")
        if relation != "concurrent":
            log("(inattendu : les ecritures ne sont pas concurrentes)")

        # -- 2. Prédiction : la règle du plus grand vecteur -----------------
        if poids("rouge", vc_rouge) > poids("bleu", vc_bleu):
            attendu = "rouge"
        else:
            attendu = "bleu"
        log(f"regle 'vecteur le plus grand' : poids(rouge)={poids('rouge', vc_rouge)} "
            f"vs poids(bleu)={poids('bleu', vc_bleu)}")
        log(f"-> {attendu!r} doit gagner sur TOUS les noeuds (regle deterministe)\n")

        # -- 3. Observation : convergence vers le gagnant -------------------
        log("--- Propagation (lecture de tous les noeuds) ---")
        duree = observer(ids, ports, attendu, interval)
        if duree is None:
            log("\nECHEC : pas de convergence en 30 s")
            return
        log(f"\nconvergence en {duree:.1f} s : tous les noeuds disent "
            f"{CLE_CONFLIT}={attendu!r}")

        # -- Verdict --------------------------------------------------------
        perdant = "bleu" if attendu == "rouge" else "rouge"
        log(f"\nA retenir :")
        log(f"- le conflit a ete DETECTE (horloges incomparables) et logge par les "
            f"noeuds — contrairement a LWW ou il est invisible ;")
        log(f"- la resolution est automatique et deterministe : le vecteur le plus "
            f"grand gagne, le gagnant absorbe l'horloge du perdant et se propage ;")
        log(f"- l'ecriture {perdant!r} a quand meme disparu partout : resoudre "
            f"automatiquement = accepter de perdre une branche. Pour ne rien perdre, "
            f"il faudrait garder les deux valeurs (siblings, facon Dynamo/Riak).")
    finally:
        for proc in procs:
            proc.terminate()
        for proc in procs:
            proc.wait()


# Point d'entrée : deux modes
# - Mode scénario (sans --node) : lance le test complet avec N nœuds
# - Mode nœud (--node) : lance un seul nœud, utilisé en interne par le scénario
def main() -> None:
    parser = argparse.ArgumentParser(
        description="Phase 3 - conflits & horloges vectorielles")
    parser.add_argument("--node", action="store_true",
                        help="Mode noeud individuel (usage interne au scenario)")
    parser.add_argument("--nodes", type=int, default=3,
                        help="Nombre de noeuds du scenario (defaut : 3)")
    parser.add_argument("--port", type=int, default=None, help="(mode --node) port d'ecoute")
    parser.add_argument("--id", type=str, default=None, help="(mode --node) identifiant")
    parser.add_argument("--peers", type=int, nargs="*", default=[],
                        help="(mode --node) ports des pairs")
    parser.add_argument("--interval", type=float, default=1.0,
                        help="Secondes entre deux tours de gossip (defaut : 1)")
    parser.add_argument("--base-port", type=int, default=18000,
                        help="Premier port du scenario (defaut : 18000)")
    args = parser.parse_args()

    if args.node:
        if args.port is None:
            parser.error("--node exige --port")
        lancer_noeud(args)
    else:
        scenario(args.nodes, args.interval, args.base_port)


if __name__ == "__main__":
    main()
