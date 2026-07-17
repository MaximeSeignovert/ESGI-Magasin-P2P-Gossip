# ESGI · Master 2 — Traitements distribués

## Atelier 2 · Énoncé groupes

### Le magasin pair-à-pair par gossip

Des nœuds symétriques et bidirectionnels — aucun maître, aucun SPOF.

Construire, pas à pas, un mini-Cassandra : gossip + anneau + réplication.

**En groupes de 3 – 4.** Prérequis : ateliers « salade de fruits » (maître-esclave, RPyC) et « hachage » (anneau cohérent).

Guillaume LE LAY — ESGI, École Supérieure de Génie Informatique

---

## 1. Présentation & objectifs

### 1.1 Le virage : rompre l'asymétrie

Le premier atelier a martelé une asymétrie : dans le maître-esclave, seul le client prend l'initiative, le serveur ne fait que répondre, et un coordinateur unique reste un point de défaillance unique (SPOF). Cet atelier casse cette asymétrie. Chaque nœud y est à la fois serveur ET client — il répond aux autres et les contacte de lui-même. Il n'y a plus de maître : les nœuds sont symétriques, la communication est bidirectionnelle de pair à pair, et le système survit à la perte de n'importe lequel d'entre eux.

### 1.2 Ce que vous allez construire

Un magasin clé-valeur entièrement pair-à-pair, tolérant aux pannes, à cohérence à terme. En réutilisant l'anneau de hachage cohérent de l'atelier précédent pour placer les données, on obtient l'ossature exacte de Cassandra et de Dynamo : membership et détection de panne par gossip, placement et réplication par anneau, résolution de conflits par horodatage.

```
AVANT (atelier 1) : maître-esclave, UNIdirectionnel
   esclave --req--> MAÎTRE --rép--> esclave      (SPOF au centre)

MAINTENANT : pair-à-pair, BIdirectionnel, symétrique
   N1  <->  N2
   |    X    |     chaque nœud = serveur + client
   N3  <->  N4     gossip : échange d'état avec un pair au hasard
                   anneau : la clé k vit sur les R nœuds suivants
                   aucun maître, aucun SPOF
```

### 1.3 Objectifs pédagogiques

- Construire un nœud symétrique (serveur RPyC + client) et voir la communication devenir bidirectionnelle.
- Disséminer l'information par gossip (anti-entropie push-pull) et mesurer la convergence.
- Assumer la cohérence à terme : résoudre les conflits par horodatage (last-write-wins), en connaître les limites.
- Détecter les pannes sans détecteur central : heartbeats propagés par gossip (façon SWIM).
- Réutiliser l'anneau cohérent pour placer et répliquer (facteur R) — la brique qui fait le mini-Cassandra.
- Situer le tout dans le théorème CAP : pourquoi ces systèmes choisissent la disponibilité (AP) plutôt que la cohérence forte.

---

## 2. Organisation & mise en place

### 2.1 Format & rôles

Groupes de 3 à 4, rôles tournants : pilote au clavier, navigateur qui lit l'énoncé, testeur qui lance le cluster et chronomètre, scribe qui note hypothèses et mesures. Comme le système est symétrique, faites tourner « qui tue un nœud » : chacun doit voir que tuer n'importe quel nœud ne casse rien.

### 2.2 Mise en place technique

- Python 3 + `pip install rpyc` (comme à l'atelier 1).
- L'anneau cohérent : réimporter la classe `Anneau` de l'atelier hachage (`hashlib`, `bisect` — stdlib).
- Un cluster local : chaque nœud est un processus sur son propre port (18000, 18001, …) ; un script lance N nœuds d'un coup.
- Un mini-client hors cluster pour put/get et pour tuer des nœuds (Ctrl-C ou kill).

### 2.3 Livrables

Vous écrivez tout le code. Chaque phase produit un fichier (voir « Fichier à produire »). Livrables finaux : le code de chaque phase, plus un court compte-rendu (hypothèses, mesures de convergence, et la décision de conception que vous défendrez à la restitution).

---

## 3. Les phases

Chaque phase enrichit la précédente mais reste lançable seule. Pour chacune : un objectif, des pistes de réflexion (à vous de trouver la logique), et le fichier à produire.

### Phase 0 — Le nœud symétrique

**Objectif.** Écrire un nœud qui est à la fois un serveur RPyC et un client d'autres nœuds. Le tester à deux : chacun appelle une méthode de l'autre — la communication va dans les deux sens, sans maître.

**Pistes de réflexion.** Qu'est-ce qui change par rapport au maître-esclave ? (Là-bas, un seul côté prenait l'initiative.) Comment un même programme peut-il écouter ET contacter ? Quel identifiant / port donner à chaque nœud, et comment lui dire qui sont ses pairs ?

**Fichier à produire :** `0_noeud_symetrique.py`

### Phase 1 — Store local + gossip à deux

**Objectif.** Ajouter un dictionnaire clé → (valeur, horodatage), avec put/get. Puis deux méthodes d'échange : digest (donne mon état) et merge (fusionne l'état reçu). Faire gossiper deux nœuds et voir une écriture faite sur A apparaître sur B.

**Pistes de réflexion.** Que faut-il attacher à chaque valeur pour savoir, lors d'une fusion, laquelle est la plus récente ? Le gossip doit-il tout renvoyer ou juste les nouveautés ? (Commencer simple : tout l'état, on optimisera plus tard.)

**Fichier à produire :** `1_gossip_paire.py`

### Phase 2 — N nœuds & convergence

**Objectif.** La boucle de gossip : toutes les T secondes, chaque nœud choisit un pair au hasard et échange son état (push puis pull). Lancer N nœuds, écrire une clé sur l'un, mesurer en combien de tours tous les nœuds la connaissent.

**Pistes de réflexion.** Un seul pair par tour suffit-il à toucher tout le monde ? En combien de tours l'info atteint-elle les N nœuds ? À observer : la propagation est exponentielle (~log N tours), pas linéaire.

**Fichiers à produire :** `2_gossip_n.py` + `mesure_convergence.py`

### Phase 3 — Conflits & cohérence à terme

**Objectif.** Écrire deux valeurs différentes pour la MÊME clé sur deux nœuds distincts, presque en même temps. Observer que le gossip fait converger tout le monde vers une seule valeur : la plus récente selon l'horodatage (last-write-wins).

**Pistes de réflexion.** Sans coordination, comment N nœuds peuvent-ils s'accorder sur UNE valeur ? Quelle règle simple, appliquée partout de la même façon, garantit qu'ils convergent (et non qu'ils oscillent) ?

**Fichier à produire :** `3_lww.py` (le merge de la Phase 1 implémente déjà LWW : ici on le met à l'épreuve)

### Phase 4 — Détection de panne par gossip

**Objectif.** Chaque nœud tient un compteur de battements (heartbeat) qu'il incrémente à chaque tour, et propage la table des compteurs de tous par gossip (fusion = garder le plus grand compteur). Un nœud dont le compteur n'a pas bougé depuis `T_mort` secondes est déclaré mort — sans aucun détecteur central.

**Pistes de réflexion.** Comment savoir qu'un nœud est mort alors que personne ne l'appelle directement ? Indice : ce n'est pas l'absence de réponse à MOI qui compte, c'est l'absence de nouvelles de lui dans les états que je reçois des AUTRES. Pourquoi un compteur, plutôt qu'un simple horodatage émis par le nœud lui-même ?

**Fichier à produire :** `4_membership.py`

### Phase 5 — Anneau + réplication (le mini-Cassandra)

**Objectif.** Réimporter l'anneau cohérent de l'atelier hachage. La liste des nœuds vivants (issue des heartbeats de la Phase 4) peuple l'anneau. Une clé vit sur les R nœuds physiques suivants sur l'anneau (facteur de réplication). Écrire = envoyer aux R répliques ; lire = interroger les R répliques et garder la valeur au plus grand horodatage (read-repair). Faire rejoindre un nœud et constater que peu de clés changent de place.

**Pistes de réflexion.** Qui décide où va une clé, sans maître ? (N'importe quel nœud, en consultant SON anneau — tous ont le même grâce au gossip de membership.) Comment obtenir R nœuds PHYSIQUES distincts sur l'anneau ? Que se passe-t-il pour les clés quand un nœud arrive ou meurt ?

**Fichier à produire :** `5_ring_replication.py`

### Phase 6 — Chaos & théorème CAP

**Objectif.** Tuer des nœuds au hasard pendant que le client lit et écrit. Constater : tant qu'une réplique d'une clé survit, la donnée reste servie ; tuer n'importe quel nœud ne bloque jamais tout le système. Contraster avec le SPOF du maître-esclave.

**Pistes de réflexion.** (à débattre) Que se passe-t-il si le réseau se coupe en deux (partition) et que les deux moitiés acceptent des écritures ? On reste disponible mais on diverge : c'est le choix AP du théorème CAP (disponibilité + tolérance au partitionnement, au prix de la cohérence forte). Un système bancaire ferait le choix inverse (CP). Nommez, pour votre variante, si l'AP est acceptable.

**Fichier à produire :** `6_chaos.py`

### Phase 7 — Restitution & débat

**Objectif.** Présenter une décision de conception et l'hypothèse qui la sous-tend, puis répondre à un mini-quiz oral.

**Questions à préparer.**

- Pourquoi le gossip converge-t-il en ~log N tours et non en N ?
- Pourquoi LWW peut-il perdre une écriture, et qu'est-ce qui le corrige vraiment ?
- Comment détecte-t-on une panne sans détecteur central — et pourquoi n'est-ce jamais une preuve ?
- Où est passé le SPOF de l'atelier 1, et qu'a-t-on payé en échange ?

---

## 4. Extensions bonus (optionnel)

- **E1** — Écriture / lecture à quorum (R + W > N) : retrouver la cohérence forte à la demande.
- **E2** — Horloges vectorielles à la place de LWW : détecter les vrais conflits, plus de perte silencieuse.
- **E3** — Anti-entropie par arbres de Merkle : ne transférer que les plages de clés qui diffèrent.
- **E4** — Réglage de la dissémination (fan-out & période T) : tracer convergence vs charge réseau.

---

## 5. Pièges à garder en tête

- **Serveur bloquant** — lancer la boucle gossip dans un thread démon AVANT de démarrer le serveur.
- **Le même verrou pour tous** — thread serveur et thread gossip touchent le store en même temps (piège de l'atelier 1).
- **Toujours fusionner par « plus récent »** — sinon les nœuds oscillent au lieu de converger.
- **Un pair injoignable n'est pas une erreur fatale** — on ignore et on réessaiera au tour suivant.
- **Détecter ≠ prouver** — un heartbeat manquant est une suspicion, jamais une certitude (mort ou lent : on ne sait pas).
- **LWW perd des écritures** — acceptable pour de la présence, pas pour de l'argent ; sinon, horloges vectorielles.
- **Gossiper tout le store ne passe pas à l'échelle** — les vraies bases comparent d'abord des résumés (Merkle).
