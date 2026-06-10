# ISSUES — Audit PYPERUN

> Généré le 2026-06-09. Audit en lecture seule — aucune modification du code.
> Tests : **179 passed**, 3 skipped, 1 warning. Ruff : **3 erreurs**.

---

## Résumé

Le codebase est fonctionnel et bien structuré, mais la documentation publique traîne sur d'anciennes conventions tandis que le code a évolué (nommage parquet, logging, scheduler, MCP). Quelques bugs réels affectent les pipelines en production (`normalize.columns`, `steps_total` en erreur, port Docker MCP).

---

## Critique

### 1. `normalize.columns` — type `dict` avec default `[]` (liste)

**Fichiers :** `pyperun/treatments/normalize/treatment.json` (l.30–33), `pyperun/core/validator.py` (l.58–60), `pyperun/core/api.py` (l.673)

**Problème :** Le schéma déclare `type: "dict"` mais `default: []`. `merge_params()` lève `TypeError: Param 'columns' expected dict, got list` quand le default est utilisé via `run_treatment()`. `init_dataset()` injecte explicitement tous les defaults dans le flow JSON → **le preset `full` casse à l'étape `normalize`**.

Les tests appellent `run()` directement (`test_normalize.py`), jamais via le runner — le bug n'est pas détecté.

**Solutions :**
- **A (recommandée)** : Changer le type en `"list"` dans `treatment.json` (le `run.py` accepte déjà `list | dict`).
- **B** : Supporter un type union (`list|dict`) dans `validator.py`.
- **C** : Changer le default en `{}` si seul le mode dict est voulu (casse le sémantique `[] = toutes les colonnes`).

---

### 2. Port MCP Docker incohérent (5001 vs 8000) — ✅ RÉSOLU (2026-06-09)

> Migré vers le serveur unifié `pyperun serve` (port **8000**). `Dockerfile` (`CMD pyperun serve`, `EXPOSE 8000`, deps `[server,duckdb]`), `docker-compose.yml` (un seul service `pyperun`, scheduler folded-in, plus de conteneur scheduler séparé), `.mcp.json` → `http://localhost:8000/mcp/sse`. L'incohérence de port disparaît : MCP est monté sur `/mcp` du même process. `docker compose config` validé.

**Fichiers :** `Dockerfile` (l.11, l.13), `docker-compose.yml` (l.8–9), `.mcp.json` (l.5), `pyperun/mcp.py` (l.361)

**Problème :** Docker expose et mappe le port **5001**, `.mcp.json` pointe vers `http://localhost:5001/sse`, mais `mcp.run(transport="sse")` est appelé **sans `port=5001`**. FastMCP écoute par défaut sur le port **8000**. Le service MCP Docker est donc inaccessible sur 5001.

**Solutions :**
- **A (recommandée)** : `mcp.run(transport="sse", port=5001)` dans `mcp.py` (ou variable d'environnement `MCP_PORT`).
- **B** : Aligner Docker et `.mcp.json` sur le port 8000.

---

## Haute priorité

### 3. `steps_total` incorrect dans `latest.json` en cas d'erreur

**Fichier :** `pyperun/core/flow.py` (l.298–300 vs l.305–306)

**Problème :** En succès, `write_flow_summary` reçoit `len(steps)`. En erreur, il reçoit `i` (index de l'étape courante). Les agents MCP/UI qui lisent `steps_total` sur un échec obtiennent une valeur fausse.

**Solutions :**
- Passer `len(steps)` aussi dans le bloc `except` (l.299).
- Ajouter un test d'intégration flow qui échoue volontairement et vérifie `latest.json`.

---

### 4. Commande `pyperun run` documentée mais absente du CLI

**Fichiers :** `README.md` (l.118–125), `pyperun/cli.py` (pas de subcommand `run`), `pyperun/core/runner.py` (`main()` standalone)

**Problème :** La doc décrit `pyperun run <treatment>`, mais seul `python -m pyperun.core.runner` ou `pyperun flow --step` permettent d'exécuter un traitement isolé.

**Solutions :**
- **A** : Ajouter `pyperun run` dans `cli.py` (wrapper autour de `run_treatment()`).
- **B** : Mettre à jour toute la doc pour pointer vers `pyperun flow --step` ou `python -m pyperun.core.runner`.

---

### 5. Convention de nommage parquet — docs ≠ code

**Fichiers :** `pyperun/core/filename.py`, `README.md`, `CLAUDE.md`, tous les `treatment.json` (`input_format`/`output_format`)

**Problème :**

| Documentation | Code réel |
|---------------|-----------|
| `<source>__<domain>__<YYYY-MM-DD>.parquet` | `domain=<domain>/<experience>__<device_id>__<step>__<YYYY-MM-DD>.parquet` |
| Agrégé : `__<window>` en suffixe | `__aggregated__<window>__<day>` (5 segments) |

**Solutions :**
- Mettre à jour README, CLAUDE.md, PYPERUN_GUIDE.md et les `treatment.json` pour refléter le layout `domain=*/` et le format à 4–5 segments.
- Ou migrer le code vers l'ancienne convention (breaking change pour les datasets existants).

---

### 6. Entrée `aggregate` — docs sautent l'étape `normalize`

**Fichiers :** `pyperun/core/pipeline.py` (l.10–11), `README.md` (l.181, l.234)

**Problème :** Le registre pipeline définit `aggregate` avec entrée `35_normalized`, mais README montre `aggregate` depuis `30_transform` (sans `normalize`). Un flow copié depuis la doc échouera silencieusement ou produira des données incorrectes.

**Solutions :**
- Corriger les exemples de flow dans README et PYPERUN_GUIDE.
- Marquer `normalize` comme obligatoire dans la doc (pas « optionnel » si le pipeline built-in l'inclut).

---

### 7. Preset `duckdb` absent du CLI, présent dans l'API

**Fichiers :** `pyperun/cli.py` (l.219–232, `_BUILTIN_PRESETS`), `pyperun/core/api.py` (l.545–548)

**Problème :** `api.list_presets()` expose `duckdb`, mais `cli._BUILTIN_PRESETS` ne le contient pas. `pyperun init --preset duckdb` échoue côté CLI alors que MCP/API l'acceptent.

**Solutions :**
- Supprimer `_BUILTIN_PRESETS` du CLI et déléguer entièrement à `api.list_presets()`.
- Ou ajouter `duckdb` dans `cli._BUILTIN_PRESETS`.

---

## Priorité moyenne

### 8. Système de logging obsolète dans la documentation

**Fichiers :** `README.md` (l.466, l.493, l.540), `tests/test_runner.py` (l.86, docstring « pyperun.log »)

**Problème :** La doc référence `logs/pyperun.log`. Le code écrit dans `logs/flows/<flow>/YYYY-MM-DD.jsonl`, `logs/misc/`, et `logs/flows/<flow>/latest.json`. Aucun `pyperun.log` n'est créé.

**Solutions :**
- Réécrire la section logging du README pour décrire l'architecture 2 couches (`latest.json` + `.jsonl` quotidiens).
- Corriger la docstring du test `test_log_contains_success`.

---

### 9. Deux mécanismes de planification coexistants

**Fichiers :** `pyperun/core/scheduler.py`, `schedules.json`, `scripts/run_scheduled_flows.sh`, `scripts/scheduled_flows.txt`, `docker-compose.yml`

**Problème :** L'ancien système (cron + `scheduled_flows.txt` + shell) coexiste avec le nouveau (`schedules.json` + `pyperun tick` + Docker scheduler). Pas de doc unifiée sur lequel utiliser.

**Solutions :**
- Documenter `schedules.json` comme mécanisme canonique.
- Marquer `scripts/run_scheduled_flows.sh` comme legacy ou le faire appeler `pyperun tick`.
- Ajouter un exemple `schedules.json` dans le repo (template, pas gitignored).

---

### 10. `describe_treatment` / `list_treatments` ignorent les traitements locaux

**Fichiers :** `pyperun/core/api.py` (l.102–130), `pyperun/core/runner.py` (l.20–30, `resolve_treatment_dir`)

**Problème :** `resolve_treatment_dir()` cherche d'abord dans `./treatments/` (local), puis built-in. Mais `list_treatments()` et `describe_treatment()` scannent uniquement `TREATMENTS_ROOT` (built-in). Un traitement custom local est exécutable mais invisible à la découverte.

**Solutions :**
- Factoriser la logique de découverte : scanner `./treatments/` puis built-in, avec priorité locale.
- Exposer la même logique dans CLI, MCP et `api_server.py`.

---

### 11. Fonctions API non exposées (CLI / MCP / Flask) — ✅ RÉSOLU (2026-06-09)

> `launch_flow`, `list_running`, `stop_flow` sont désormais exposés via MCP (`pyperun/mcp.py`) **et** REST (`pyperun/server.py` : `POST /api/run/{flow}`, `GET /api/running`, `POST /api/stop/{flow}`). `api_server.py` (Flask bloquant) est remplacé par le serveur ASGI unifié non-bloquant. `list_presets` exposé en REST (`GET /api/presets`) et aligné CLI/api (preset `duckdb` ajouté côté CLI).

**Fichiers :** `pyperun/core/api.py` (`launch_flow`, `list_running`, `stop_flow`), `pyperun/mcp.py`, `api_server.py`

**Problème :**

| Fonction | CLI | MCP | Flask |
|----------|-----|-----|-------|
| `launch_flow` (async) | ✗ | ✗ | ✗ (thread sync) |
| `list_running` | ✗ | ✗ | ✗ |
| `stop_flow` | ✗ | ✗ | ✗ |
| `list_presets` | partiel | ✗ (mentionné en doc, pas de tool) | ✓ |

`api_server.py` lance `run_flow` dans un thread (bloquant) et catch `SystemExit` (l.262) — mais `run_flow` lève `RuntimeError`, pas `SystemExit`. Le catch est inefficace.

**Solutions :**
- Exposer `launch_flow`, `list_running`, `stop_flow` dans MCP et CLI.
- Migrer `api_server.py` vers `launch_flow` + polling.
- Corriger la docstring de `api.run_flow` (l.191 : annonce `SystemExit`, lève `RuntimeError`).

---

### 12. `transform` — fonction `identity` documentée mais non implémentée

**Fichiers :** `pyperun/treatments/transform/treatment.json` (l.16), `pyperun/treatments/transform/run.py` (l.9–13)

**Problème :** Le schéma liste `identity` comme fonction valide, mais `TRANSFORMS` ne contient que `sqrt_inv`, `cbrt_inv`, `log`. Utiliser `identity` provoque une erreur à l'exécution.

**Solutions :**
- Ajouter `"identity": lambda s: s` dans `TRANSFORMS`.
- Ou retirer `identity` de la description du schéma.

---

### 13. `to_postgres` — param `mode` documenté mais absent

**Fichiers :** `README.md` (l.310), `pyperun/treatments/to_postgres/treatment.json`, `pyperun/treatments/to_postgres/run.py`

**Problème :** README documente `mode: append | replace | reset`. Ni le schéma ni `run.py` n'implémentent ce paramètre. Le comportement delete+insert est codé en dur.

**Solutions :**
- Implémenter `mode` dans `to_postgres/run.py` et `treatment.json`.
- Ou retirer `mode` de la documentation.

---

### 14. Scheduler — robustesse et race conditions

**Fichiers :** `pyperun/core/scheduler.py`

**Problèmes :**
- Aucune validation JSON : `entry["flow"]` / `entry["schedule"]` → `KeyError` possible (l.59–61).
- Premier tick sans historique (`last_run_utc is None`) → lancement immédiat (l.45–46), potentiellement non désiré.
- Fenêtre entre `is_locked()` et écriture du `.lock` par `run_flow` : double lancement possible.
- `stdout.log` en mode `"w"` (écrasé) vs `"ab"` dans `api.launch_flow` (l.265).
- **0 test** unitaire.

**Solutions :**
- Valider le schéma `schedules.json` (pydantic ou check explicite).
- Option `run_on_first_tick: false` par défaut.
- Écrire un lock « pending » avant `Popen`, ou réutiliser `api.launch_flow()`.
- Harmoniser le mode d'ouverture des logs (`"ab"`).
- Ajouter `tests/test_scheduler.py`.

---

### 15. Bind-mount `schedules.json` Docker — piège répertoire

**Fichier :** `docker-compose.yml` (l.14, l.28)

**Problème :** Si `schedules.json` n'existe pas sur l'hôte au premier `docker compose up`, Docker Linux crée un **répertoire** nommé `schedules.json`. `pyperun tick` échouera ensuite.

**Solutions :**
- Créer un `schedules.json` template dans le repo (ex. `schedules.json.example`).
- Documenter `touch schedules.json` avant le premier lancement Docker.
- Utiliser un bind-mount sur un répertoire `config/` plutôt qu'un fichier unique.

---

### 16. Valeurs par défaut divergentes entre docs et `treatment.json`

| Paramètre | Documentation | Code (`treatment.json`) |
|-----------|---------------|------------------------|
| `resample.max_gap_fill_s` | `2` (CLAUDE.md) | `20` |
| `aggregate.windows` | `["10s","60s","5min","1h"]` (CLAUDE.md) | `["1s","10s","60s","5min","1h"]` |
| `normalize` méthode | « Min-max » (README) | `percentile` (P2/P98) par défaut |

**Solutions :**
- Aligner la documentation sur les valeurs réelles des `treatment.json`.
- Ou ajuster les defaults si la doc reflète l'intention métier.

---

### 17. Nombre d'étapes pipeline — « 9 étapes » vs 10 traitements

**Fichiers :** `CLAUDE.md`, `pyperun/core/pipeline.py`

**Problème :** La doc parle de « pipeline 9 étapes ». Le registre contient 10 traitements (ajout de `exportduckdb`). Le stage `63_exportduckdb` est absent des conventions documentées.

**Solutions :**
- Mettre à jour la doc : « 10 traitements » avec tableau complet incluant `exportduckdb`.
- Distinguer « pipeline core » (7 étapes disque) vs « exports » (3 traitements externes/disque).

---

## Priorité basse

### 18. Erreurs Ruff (3)

| Fichier | Code | Détail |
|---------|------|--------|
| `pyperun/cli.py:373` | F821 | `"Path"` en annotation sans import module-level |
| `tests/test_cli_logs.py:19` | F841 | Variable `exc` inutilisée |
| `tests/test_normalize.py:271` | F841 | Variable `out` inutilisée |

**Solutions :** Import `Path` au niveau module ; préfixer par `_` ou supprimer les variables inutilisées.

---

### 19. Dépendances non déclarées dans `pyproject.toml`

**Fichiers :** `pyproject.toml`, `api_server.py`, `pyperun/treatments/transform/run.py`, `pyperun/treatments/normalize/run.py`

**Problème :**
- `flask` / `gunicorn` utilisés par `api_server.py` — install manuelle.
- `numpy` importé directement dans transform/normalize — transitif via pandas, non explicite.
- Optionnels (`mcp`, `duckdb`, `scheduler`) non installés par défaut — comportement attendu mais à documenter clairement.

**Solutions :**
- Ajouter `[api]` extra : `flask`, `gunicorn`.
- Déclarer `numpy` explicitement dans les deps core.
- Documenter les extras dans README (`pip install pyperun[mcp,duckdb,scheduler,api]`).

---

### 20. Code mort / duplication

| Élément | Détail |
|---------|--------|
| `flow.py:main()` | Entry point argparse dupliqué, non exposé CLI |
| `runner.py:main()` | Remplace le `pyperun run` manquant |
| `cli._load_presets()` | Duplique partiellement `api.list_presets()` |
| Lock PID | Logique dupliquée entre `scheduler.is_locked` et `api._pid_alive` |
| Lancement subprocess | `scheduler.tick` vs `api.launch_flow` — patterns similaires, logs différents |
| `pyperun/__init__.py` | Vide |

**Solutions :**
- Centraliser presets, lock, et lancement subprocess dans `api.py`.
- Exposer ou supprimer les `main()` orphelins.

---

### 21. Gestion d'erreurs silencieuse

**Fichiers :** `pyperun/core/api.py` (multiples `except Exception: pass/continue`), `pyperun/cli.py` (`cmd_delete`, `cmd_export`), `pyperun/core/flow.py` (`_print_dry_run`)

**Problème :** Erreurs JSON, résolution de params, et scans de flows sont avalés silencieusement. Difficile à diagnostiquer en production.

**Solutions :**
- Logger les exceptions au niveau `warning` minimum.
- Propager les erreurs critiques au lieu de `pass`.

---

### 22. Statuts `running` / `stopped` non reflétés partout

**Fichiers :** `pyperun/core/logger.py`, `pyperun/mcp.py` (l.87), `pyperun/core/api.py` (`get_status`)

**Problème :** `latest.json` peut avoir `status: running | stopped`, mais `get_status()` retourne seulement `up-to-date | incomplete | no-dataset`. La doc MCP annonce `'success' | 'error'` uniquement.

**Solutions :**
- Étendre `get_status()` pour signaler un run en cours.
- Mettre à jour la doc MCP et README.

---

### 23. `cmd_upgrade` — `pip install --break-system-packages`

**Fichier :** `pyperun/cli.py` (l.685)

**Problème :** Flag spécifique Debian/Ubuntu récents (PEP 668). Échoue ou est ignoré sur d'autres distributions.

**Solutions :**
- Détecter l'environnement (venv vs system) et adapter la commande.
- Recommander `pip install -e ".[dev]"` dans un venv.

---

### 24. `set_flow_config` MCP — écrit toujours dans `./flows/` local

**Fichier :** `pyperun/mcp.py` (l.263–266)

**Problème :** Pas de priorité built-in vs local cohérente avec `get_flows_root()` utilisé ailleurs.

**Solutions :** Utiliser `get_flows_root()` pour la lecture, écrire dans le répertoire projet local par convention explicite.

---

## Lacunes de tests

| Module / zone | Couverture |
|---------------|------------|
| `clean`, `resample`, `aggregate`, `exportparquet` | **Aucun test** |
| `flow.py` (filtrage, dry-run, reset, SIGTERM) | **Aucun test** |
| `validator.py` | **Aucun test** |
| `scheduler.py` / `tick()` | **Aucun test** |
| `mcp.py` | **Aucun test** |
| `normalize` via `run_treatment()` / flow | **Aucun test** (bug #1 non détecté) |
| `cli.py` (flow, init, export/import, tick, status) | Partielle (`test_cli_logs.py` seulement) |
| `api.py` (init, delete, launch_flow, stop_flow) | Partielle |
| `api_server.py`, Docker | Non testés |

**Solutions :**
- Prioriser tests `normalize` via runner + test `steps_total` en erreur.
- Ajouter tests scheduler (cron, lock, dry-run).
- Tests d'intégration flow minimal (2–3 étapes avec données synthétiques).

---

## Incohérences documentation interne (CLAUDE.md / PYPERUN_GUIDE.md)

Ces fichiers sont gitignored mais servent de référence aux agents :

- Sous-commandes CLI incomplètes (`new`, `describe`, `delete`, `export`, `import`, `logs`, `upgrade`, `tick`, `help`).
- `scheduler.py`, `launch_flow`, `stop_flow`, `write_flow_progress` non documentés.
- Référence à `LOG_PATH` / `pyperun.log` inexistant.
- `exportduckdb` et stage `63_exportduckdb` absents.
- Mention « pipeline 9 étapes » (voir #17).

**Solution :** Rafraîchir CLAUDE.md et PYPERUN_GUIDE.md en une passe alignée sur le code actuel.

---

## Matrice de priorités

| # | Issue | Sévérité | Effort |
|---|-------|----------|--------|
| 1 | `normalize.columns` type/default | Critique | Faible |
| 2 | Port MCP Docker | Critique | Faible |
| 3 | `steps_total` en erreur | Haute | Faible |
| 4 | `pyperun run` absent | Haute | Moyen |
| 5 | Convention parquet docs | Haute | Moyen |
| 6 | `aggregate` input docs | Haute | Faible |
| 7 | Preset `duckdb` CLI | Haute | Faible |
| 8 | Logging obsolète | Moyenne | Moyen |
| 9 | Double scheduling | Moyenne | Moyen |
| 10 | Traitements locaux invisibles | Moyenne | Moyen |
| 11 | API async non exposée | Moyenne | Moyen |
| 12 | `identity` transform | Moyenne | Faible |
| 13 | `to_postgres.mode` | Moyenne | Moyen |
| 14 | Scheduler robustesse | Moyenne | Moyen |
| 15 | Docker schedules.json | Moyenne | Faible |
| 16 | Defaults divergents | Moyenne | Faible |
| 17 | 9 vs 10 étapes | Basse | Faible |
| 18 | Ruff 3 erreurs | Basse | Faible |
| 19 | Deps manquantes | Basse | Faible |
| 20 | Code mort / duplication | Basse | Moyen |
| 21 | Erreurs silencieuses | Basse | Moyen |
| 22 | Statuts running/stopped | Basse | Moyen |
| 23 | `cmd_upgrade` fragile | Basse | Faible |
| 24 | MCP set_flow_config | Basse | Faible |
