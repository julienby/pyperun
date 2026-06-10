#!/usr/bin/env bash
#
# Pyperun installer — one-liner, multi-instance, data-safe.
#
#   curl -fsSL https://raw.githubusercontent.com/julienby/pyperun/master/install.sh | bash -s -- <instance>
#
# Re-running on an existing instance UPDATES the code and restarts it; your
# flows/ datasets/ logs/ schedules.json are never overwritten.
#
# Layout (per host):
#   ~/.pyperun/.build/          shared git clone → builds the image  pyperun:latest
#   ~/.pyperun/<instance>/
#     ├── data/{flows,datasets,logs}/  schedules.json   ← your persistent data
#     ├── .env                         ← PORT / TOKEN / EMAIL (preserved on update)
#     └── docker-compose.yml           ← references pyperun:latest
#
# Config (interactive, with sane defaults — press Enter to accept):
#   PORT   auto-picked free port starting at 8000
#   TOKEN  random 24-byte hex (gates UI + REST + MCP)
#   EMAIL  optional contact shown on the 401 page
#
# Non-interactive override: PORT=… TOKEN=… EMAIL=… curl … | bash -s -- <instance>
#
set -euo pipefail

REPO_URL="${PYPERUN_REPO:-https://github.com/julienby/pyperun.git}"
REF="${PYPERUN_REF:-master}"
REPO_URL_RAW="${PYPERUN_REPO_RAW:-https://raw.githubusercontent.com/julienby/pyperun/$REF/install.sh}"
PYPERUN_HOME="${PYPERUN_HOME:-$HOME/.pyperun}"
IMAGE="pyperun:latest"
BUILD_DIR="$PYPERUN_HOME/.build"

# --- pretty output --------------------------------------------------------
c_accent=$'\033[33m'; c_ok=$'\033[32m'; c_err=$'\033[31m'; c_dim=$'\033[2m'; c_off=$'\033[0m'
say()  { printf '%s\n' "$*"; }
info() { printf '%s›%s %s\n' "$c_accent" "$c_off" "$*"; }
ok()   { printf '%s✓%s %s\n' "$c_ok" "$c_off" "$*"; }
die()  { printf '%s✗%s %s\n' "$c_err" "$c_off" "$*" >&2; exit 1; }

# A tty for prompts even when the script is piped from curl.
if [ -r /dev/tty ]; then TTY=/dev/tty; else TTY=; fi
ask() { # ask <prompt> <default> -> echoes answer
  local prompt="$1" default="$2" reply=""
  if [ -n "$TTY" ]; then
    printf '%s %s[%s]%s ' "$prompt" "$c_dim" "$default" "$c_off" > "$TTY"
    read -r reply < "$TTY" || true
  fi
  printf '%s' "${reply:-$default}"
}

# --- preflight ------------------------------------------------------------
command -v docker >/dev/null 2>&1 || die "Docker introuvable. Installe Docker d'abord : https://docs.docker.com/get-docker/"
docker compose version >/dev/null 2>&1 || die "'docker compose' (plugin v2) requis. Mets Docker à jour."
docker info >/dev/null 2>&1 || die "Le daemon Docker ne répond pas (démarre-le, ou ajoute ton user au groupe docker)."

# --- instance name --------------------------------------------------------
INSTANCE="${1:-}"
[ -n "$INSTANCE" ] || INSTANCE="$(ask 'Nom de l'\''instance ?' 'default')"
# normalise: lowercase, only [a-z0-9-]
INSTANCE="$(printf '%s' "$INSTANCE" | tr '[:upper:] ' '[:lower:]-' | tr -cd 'a-z0-9-')"
[ -n "$INSTANCE" ] || die "Nom d'instance invalide."
PROJECT="pyperun-$INSTANCE"
INST_DIR="$PYPERUN_HOME/$INSTANCE"

say ""
info "Instance : ${c_accent}${INSTANCE}${c_off}   →   ${INST_DIR}"

# --- shared build clone ---------------------------------------------------
mkdir -p "$PYPERUN_HOME"
if [ -d "$BUILD_DIR/.git" ]; then
  info "Mise à jour du code (git pull, ref=$REF)…"
  git -C "$BUILD_DIR" fetch --quiet origin "$REF"
  git -C "$BUILD_DIR" checkout --quiet "$REF"
  git -C "$BUILD_DIR" reset --hard --quiet "origin/$REF"
else
  info "Clone du dépôt ($REPO_URL, ref=$REF)…"
  git clone --quiet --branch "$REF" --depth 1 "$REPO_URL" "$BUILD_DIR"
fi
ok "Code prêt : $BUILD_DIR"

# --- build the shared image ----------------------------------------------
info "Build de l'image $IMAGE (mutualisée entre toutes les instances)…"
docker build --quiet -t "$IMAGE" "$BUILD_DIR" >/dev/null
ok "Image construite : $IMAGE"

# --- instance data (NEVER overwritten) ------------------------------------
NEW_INSTANCE=1
[ -d "$INST_DIR" ] && NEW_INSTANCE=0
mkdir -p "$INST_DIR/data/flows" "$INST_DIR/data/datasets" "$INST_DIR/data/logs"
[ -f "$INST_DIR/data/schedules.json" ] || printf '[]\n' > "$INST_DIR/data/schedules.json"

# --- config (.env) : preserve existing, prompt only for new ---------------
ENV_FILE="$INST_DIR/.env"
if [ -f "$ENV_FILE" ]; then
  info "Config existante conservée ($ENV_FILE)."
  # shellcheck disable=SC1090
  set -a; . "$ENV_FILE"; set +a
  PORT="${PYPERUN_PORT:-${PORT:-8000}}"
else
  # default free port: first unused from 8000 up
  pick_port() {
    local p="${PORT:-8000}"
    while ss -ltn 2>/dev/null | grep -q ":$p " || docker ps --format '{{.Ports}}' | grep -q ":$p->"; do
      p=$((p+1))
    done
    printf '%s' "$p"
  }
  DEF_PORT="$(pick_port)"
  DEF_TOKEN="${TOKEN:-$( (openssl rand -hex 24 2>/dev/null) || head -c18 /dev/urandom | od -An -tx1 | tr -d ' \n')}"
  say ""
  info "Configuration de l'instance (Entrée = valeur par défaut) :"
  PORT="$(ask '  Port HTTP             :' "$DEF_PORT")"
  TOKEN="$(ask '  Token (UI/REST/MCP)   :' "$DEF_TOKEN")"
  EMAIL="$(ask '  Email contact (option):' "${EMAIL:-}")"
  umask 077
  cat > "$ENV_FILE" <<EOF
# Pyperun instance config — edit then re-run install.sh (or: docker compose up -d)
PYPERUN_PORT=$PORT
PYPERUN_TOKEN=$TOKEN
PYPERUN_EMAIL=$EMAIL
PYPERUN_TICK_INTERVAL=60
EOF
  ok "Config écrite : $ENV_FILE"
fi

# --- per-instance compose (regenerated; references the shared image) ------
cat > "$INST_DIR/docker-compose.yml" <<EOF
# Generated by install.sh — instance "$INSTANCE". Safe to regenerate.
services:
  pyperun:
    image: $IMAGE
    container_name: $PROJECT
    restart: always
    env_file: .env
    ports:
      - "\${PYPERUN_PORT}:8000"
    volumes:
      - ./data/flows:/app/flows:ro            # may hold credentials → read-only
      - ./data/datasets:/app/datasets
      - ./data/logs:/app/logs
      - ./data/schedules.json:/app/schedules.json
EOF

# --- launch ---------------------------------------------------------------
info "Démarrage du conteneur…"
( cd "$INST_DIR" && docker compose -p "$PROJECT" up -d )

PORT="$(grep -E '^PYPERUN_PORT=' "$ENV_FILE" | cut -d= -f2)"
TOKEN="$(grep -E '^PYPERUN_TOKEN=' "$ENV_FILE" | cut -d= -f2)"
URL="http://localhost:$PORT"

say ""
if [ "$NEW_INSTANCE" -eq 1 ]; then ok "Instance « $INSTANCE » installée."; else ok "Instance « $INSTANCE » mise à jour (données intactes)."; fi
say ""
say "  ${c_accent}URL${c_off}    $URL/?token=$TOKEN"
say "  ${c_accent}Token${c_off}  $TOKEN"
say "  ${c_dim}Données : $INST_DIR/data   ·   Config : $ENV_FILE${c_off}"
say ""
say "  ${c_dim}Logs   :${c_off} docker compose -p $PROJECT logs -f"
say "  ${c_dim}Stop   :${c_off} docker compose -p $PROJECT down"
say "  ${c_dim}Update :${c_off} curl -fsSL $REPO_URL_RAW | bash -s -- $INSTANCE"
say ""
