# Magasin P2P Gossip

Dépôt de l’**Atelier 2** (ESGI M2 — Traitements distribués) : *Le magasin pair-à-pair par gossip*.

## Sujet

Construire, pas à pas, un magasin clé-valeur **entièrement pair-à-pair** (nœuds symétriques, sans maître ni SPOF), en enchaînant **gossip**, **anneau cohérent** et **réplication** — l’ossature d’un mini-Cassandra / Dynamo.

## Objectifs pédagogiques

- Nœud symétrique (serveur RPyC + client)
- Dissémination par gossip (anti-entropie push-pull) et mesure de convergence
- Cohérence à terme (last-write-wins) et détection de panne par heartbeats
- Placement / réplication via l’anneau cohérent (facteur R)
- Placement dans le théorème CAP (choix AP)

## Attendu

Travail en groupes de 3–4. **Tout le code est à écrire.** Chaque phase produit un fichier dédié (`0_noeud_symetrique.py` … `6_chaos.py`). Livrables finaux : code de chaque phase + court compte-rendu (hypothèses, mesures de convergence, décision de conception pour la restitution).

Stack : Python 3, `rpyc`, classe `Anneau` de l’atelier hachage. Cluster local (ports 18000+).

## Contenu du dépôt

| Fichier | Rôle |
|---------|------|
| [Atelier2_P2P_gossip_ENONCE_groupes.md](Atelier2_P2P_gossip_ENONCE_groupes.md) | Énoncé complet (version Markdown) |
| Fichiers `N_*.py` (à produire) | Implémentation des phases 0 à 6 |

Détail des phases, pistes et pièges : voir l’énoncé Markdown.
