# PRD — Interface Pyperun (UI + surface fonctionnelle)

> Document de travail. On valide **les fonctions d'abord** (§3), **l'UI ensuite** (§5).
> Statut : 🟢 existe · 🟡 partiel · 🔴 à construire · ❓ à décider ensemble.

---

## 1. Intention

Pyperun est un **agent** : un pipeline IoT time-series exposé de façon transparente,
pilotable par des humains **et** par d'autres agents. La même logique (`core/api.py`)
est servie par trois façades qui partagent un seul process serveur :

```
                    ┌──────────── 1 process ASGI (uvicorn) ───────────┐
   navigateur  ───► │  /            UI statique (HTML/JS)             │
   scripts/cron ──► │  /api/*       REST                              │ ──► core/api.py
   agents LLM  ───► │  /mcp         MCP (SSE)                         │      (source unique)
                    └──────────────────────────────────────────────────┘
                                       │ déclenche
                                       ▼
                    flow A · flow B · flow C   (subprocess.Popen, parallèles, isolés)
```

**Principe directeur** : tout ce que l'UI sait faire, un agent doit pouvoir le faire
via MCP, et inversement. L'UI n'est qu'une vue confortable sur les fonctions ; elle
n'introduit aucune capacité cachée.

## 2. Personas

| Persona | Besoin | Surface |
|---------|--------|---------|
| **Opérateur** (toi) | superviser, lancer/rejouer, éditer un flow, diagnostiquer une erreur | UI |
| **Agent LLM** | interroger l'état, lancer un backfill, lire les logs d'un run raté | MCP |
| **Script / cron externe** | déclencher, vérifier le statut | REST |

## 3. Fonctions — **à valider en premier**

Regroupées par domaine. La colonne « API » indique si la fonction existe déjà dans
`core/api.py` / MCP / REST.

### 3.1 Observer (lecture seule)
| # | Fonction | API | Notes |
|---|----------|-----|-------|
| O1 | Lister les flows (nom, dataset, description, n_steps) | 🟢 `list_flows` | |
| O2 | Statut pipeline par flow (up-to-date / incomplete / no-dataset, fichiers par étape) | 🟢 `get_status` | |
| O3 | Dernier résumé de run par flow (O(1), triage) | 🟢 `list_flow_summaries` / `get_flow_summary` | lit `latest.json` |
| O4 | Détail des évènements d'un run (par étape, durées, erreurs) | 🟢 `get_run_events` | lit le `.jsonl` |
| O5 | Historique des runs d'un flow (liste paginée) | 🟡 `list_runs` existe, à exposer proprement | |
| O6 | Décrire un treatment (params, formats in/out) | 🟢 `describe_treatment` / `list_treatments` | nourrit l'Editor |
| O7 | **Qu'est-ce qui tourne maintenant ?** (flows actifs + run_id + depuis quand) | 🔴 | dérivable des lockfiles PID — **à exposer** |
| O8 | **Progression intra-run** (« resample 68% ») | 🔴❓ | les mockups l'affichent ; les logs n'ont pas de %. Option : se contenter de « étape courante / N » sans pourcentage. **À décider.** |

### 3.2 Agir (écriture)
| # | Fonction | API | Notes |
|---|----------|-----|-------|
| A1 | Lancer un flow (non bloquant → retourne `run_id`) | 🟡 REST oui (Thread), MCP **bloquant** | aligner MCP sur le modèle async |
| A2 | Lancer avec fenêtre temporelle `--from/--to` (backfill historique) | 🟢 paramètres présents | |
| A3 | Lancer un sous-ensemble d'étapes (`--step` / `--from-step` / `--to-step`) | 🟢 | |
| A4 | `output_mode` : replace / reset | 🟢 | |
| A5 | **Stopper un run en cours** | 🔴 | mockup « Stop »/« Retry ». Faisable via le PID du lockfile (SIGTERM). **À valider : on l'ajoute ?** |
| A6 | Rejouer / retry un run raté | 🟡 | = A1 avec les mêmes params ; trivial une fois A1 async |
| A7 | Init d'un dataset (scaffold dirs + flow depuis preset) | 🟢 `init_dataset` | |
| A8 | Supprimer un dataset | 🟡 REST `DELETE` existe | destructif → confirmation UI |

### 3.3 Configurer
| # | Fonction | API | Notes |
|---|----------|-----|-------|
| C1 | Lire la config JSON d'un flow | 🟢 `get_flow_config` | |
| C2 | Écrire / écraser la config d'un flow | 🟢 `set_flow_config` | valide `steps[]` |
| C3 | Éditer les params d'une étape (formulaire guidé par `describe_treatment`) | 🟡 | l'UI compose C1+O6+C2 |
| C4 | Créer un flow depuis un preset (full/csv/parquet) | 🟢 via `init_dataset` | |
| C5 | Valider un flow avant sauvegarde (lint : étapes connues, chaînage in/out cohérent) | 🔴❓ | confort UI/agent. **Utile ?** |

### 3.4 Planifier
| # | Fonction | API | Notes |
|---|----------|-----|-------|
| S1 | Lister les schedules (cron, tz, enabled) | 🟢 `list_schedules` | |
| S2 | Ajouter / modifier un schedule | 🟢 `upsert_schedule` | |
| S3 | Supprimer / désactiver un schedule | 🟢 `remove_schedule` / `enabled:false` | |
| S4 | Scheduler in-process (tick périodique dans l'ASGI) vs subprocess `pyperun tick` | 🟡❓ | aujourd'hui : conteneur `while true; pyperun tick`. **À trancher** : replier le tick dans le process ASGI (un seul conteneur) ou garder un service séparé. |

### 3.5 Sécurité / accès
| # | Fonction | API | Notes |
|---|----------|-----|-------|
| K1 | Token bearer unique dans `.env` (+ email associé, loggé) | 🟡 REST a `PYPERUN_API_KEY` | étendre à MCP + UI |
| K2 | Page d'erreur 401 propre (compatible fail2ban via les logs) | 🔴 | log structuré des refus avec IP |
| K3 | Redaction des secrets (mots de passe Postgres) dans les réponses/logs | 🟢 `_redact_params` | déjà côté REST |

> **À cocher ensemble** : lesquelles de O7, O8, A5, C5, S4 entrent dans le périmètre v1 ?
> Mon avis : **O7 oui** (essentiel), **A5 oui** (simple via PID, valeur forte), **O8 = version simple** (« étape k/N » sans %), **C5 plus tard**, **S4 = replier dans l'ASGI** (un seul conteneur, plus simple).

## 4. Contraintes non-fonctionnelles

- **Non bloquant** : tout déclenchement rend la main immédiatement avec un `run_id` ; le suivi se fait par polling (`get_flow_summary` / `get_run_events`). Un backfill de plusieurs heures ne bloque personne.
- **Parallélisme** : N flows différents = N subprocess isolés. Le lockfile PID empêche seulement le *même* flow de se chevaucher.
- **Transparence** : état 100% sur disque dans le répertoire monté (`flows/ datasets/ logs/ schedules.json`). Conteneur jetable, aucun état caché. `ps`, `cat logs/...` suffisent à tout comprendre.
- **Zéro dépendance lourde** : pas de Redis/Celery/queue. subprocess Unix + fichiers JSON.
- **Un conteneur, un port** : reverse-proxy externe (Caddy/nginx) pour TLS + domaine.

## 5. UI — proposition (à valider APRÈS §3)

Base : prototypes `ui_tests/` (Tailwind, 5 vues, sidebar). SPA statique servie par l'ASGI,
qui ne parle qu'à `/api/*`.

| Vue | Rôle | Fonctions consommées |
|-----|------|----------------------|
| **Dashboard / Flows** | liste des flows + état (running/ok/failed/never-run), actions Run/Stop/Edit | O1, O3, O7, A1, A5 |
| **Monitor** | suivi live d'un run : pipeline étape par étape, logs, erreurs | O4, O7, O8, A5 |
| **Editor** | éditer un flow : étapes, params (formulaire guidé), validation | O6, C1, C2, C3, C5 |
| **Catalog** | référence des treatments (params, formats in/out) + datasets | O6, A7, A8 |
| **History** | runs passés d'un flow, drill-down vers les évènements | O5, O4 |

**Écarts mockups ↔ capacités actuelles** (à arbitrer en §3) :
- Dashboard affiche `Stop` → dépend de **A5**.
- Dashboard/Monitor affichent une **progression %** → dépend de **O8** (sinon : « étape 3/9 »).
- Bandeau « 1 running / 4 completed / 1 failed » → dépend de **O7** + agrégation de O3.

**Auth UI** : petite page de login → token stocké (localStorage) → envoyé en
`Authorization: Bearer` sur chaque `fetch`. Pas de session serveur, pas de cookie.

## 6. Déploiement cible

```
pyperun/                 ← répertoire monté (l'état vit ici)
  flows/                 ← définitions de flows (peuvent contenir des creds → :ro)
  datasets/<DS>/00_raw/  ← données poussées par rsync
  logs/                  ← latest.json + daily .jsonl
  schedules.json         ← planification
  .env                   ← PYPERUN_TOKEN, PYPERUN_EMAIL, URLs

# 1 conteneur = 1 process ASGI (UI + REST + MCP + tick) ; flows = subprocess enfants
docker compose up -d
```

Routage (reverse-proxy → un seul port interne) :
```
https://pyperun.example.fr/        → UI
https://pyperun.example.fr/api/*   → REST
https://pyperun.example.fr/mcp     → MCP (agents)
```
Variables `.env` : `PYPERUN_TOKEN`, `PYPERUN_EMAIL`, et optionnellement
`PYPERUN_BASE_PATH` si l'app est servie sous un sous-chemin.

## 7. Décisions

- ✅ **Périmètre v1** : O7 (qui tourne maintenant) **inclus**, A5 (Stop) **inclus**, O8 = **« étape k/N » sans pourcentage**.
- ✅ **Progression** : version simple (étape courante / N), pas d'instrumentation %.
- ✅ **Scheduler** : `tick` **replié dans le process ASGI** → un seul conteneur.

- ✅ **Stop** : **SIGTERM** sur le PID du lockfile. Le run s'arrête entre deux étapes ; l'étape en cours peut finir. Le `finally` supprime le lockfile.
- ✅ **Auth** : **un seul token partagé** — `PYPERUN_TOKEN` + `PYPERUN_EMAIL` dans `.env`. Le même token garde UI + REST + MCP.
- ✅ **Routage** : **path-based** sur un domaine/port unique — `/` (UI), `/api/*` (REST), `/mcp` (MCP).
```
