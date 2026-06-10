# Pyperun UI v2 — Prototypes HTML / Tailwind / HTMX

Proposition d'interface opérateur basée sur [PRD_ui.md](../../PRD_ui.md).

## Stack

| Couche | Choix | Rôle |
|--------|-------|------|
| Structure | HTML statique | Servi par l'ASGI sur `/` |
| Style | Tailwind CDN + `css/theme.css` | Thème sombre « salle de contrôle » |
| Interactivité | [HTMX 2](https://htmx.org) | Polling, actions REST, fragments partiels |
| Graphiques | [Plotly.js](https://plotly.com/javascript/) | Donuts, barres, sparklines, timeline |
| Auth | `js/pyperun.js` | Token Bearer → localStorage |

## Vues

| Fichier | PRD §5 | Fonctions |
|---------|--------|-----------|
| `login.html` | Auth K1 | Token partagé UI/REST/MCP |
| `dashboard.html` | Dashboard | O1, O2, O3, O7, A1, A5 |
| `monitor.html` | Monitor | O2, O4, O7, O8 (k/N), A5 |
| `editor.html` | Editor | C1–C3, O6 |
| `catalog.html` | Catalog | O6, A7, A8 |
| `history.html` | History | O4, O5, A6 |
| `schedules.html` | (§3.4) | S1–S3 |

## Prévisualisation locale

```bash
cd ui_tests/version2
python -m http.server 8080
# → http://localhost:8080/login.html
```

Les fragments HTMX dans `fragments/` simulent les réponses API.
En production, les `hx-get` pointent vers `/api/*` (même origine ASGI).

## Patterns HTMX clés

```html
<!-- Polling bandeau statuts (5s) -->
<div hx-get="/api/summary" hx-trigger="every 5s" hx-swap="innerHTML">

<!-- Lancer un flow (non bloquant → run_id) -->
<button hx-post="/api/run/stab97" hx-vals='{"from":"…","to":"…"}'>

<!-- Stop via SIGTERM sur lockfile PID -->
<button hx-delete="/api/runs/stab97/stop" hx-confirm="Arrêter ?">

<!-- Progression étape k/N (2s) -->
<div hx-get="/api/runs/{run_id}/progress" hx-trigger="every 2s">
```

## Plotly — graphiques

| ID | Page | Usage |
|----|------|-------|
| `chart-status-donut` | Dashboard | Répartition ok/running/error/idle |
| `chart-run-durations` | Dashboard | Durées derniers runs |
| `chart-step-waterfall` | Monitor | Temps par étape du run actif |
| `chart-duration-sparkline` | History | Tendance durées 14j |
| `chart-run-timeline` | History | Timeline Gantt des runs |

## Écarts v1 → v2

- **HTMX** : polling réel, actions POST/DELETE branchées sur `/api/*`
- **Plotly** : visualisation des durées et statuts
- **Thème sombre** industriel (IBM Plex, ambre/teal)
- **O8 simplifié** : « étape 3/9 » sans pourcentage (décision PRD §7)
- **Schedules** : vue dédiée planification cron
- **Fragments** : modèle de réponses partielles serveur

## Intégration ASGI cible

```
/              → ui_tests/version2/ (ou build copié)
/api/*         → REST (core/api.py)
/mcp           → MCP SSE
```

L'UI n'introduit aucune capacité cachée : tout passe par la même API que les agents.
