#!/bin/bash
set -e

# ---------- INPUT ARGUMENTS ----------
SLEEP_SEC=${1:-15}     # Default: 15 seconds
DRIFT_PCT=${2:-5}      # Default: ±5%

BASE_TICK=10000
MAX_DRIFT=$((BASE_TICK * DRIFT_PCT / 100))

echo "[*] Disabling external time synchronization"
timedatectl set-ntp false
systemctl stop systemd-timesyncd || true

while true; do
    DRIFT=$((RANDOM % (2 * MAX_DRIFT + 1) - MAX_DRIFT))
    NEW_TICK=$((BASE_TICK + DRIFT))
    adjtimex --tick $NEW_TICK

    # Occasional random time jump
    if (( RANDOM % 4 == 0 )); then
        OFFSET=$((RANDOM % 10 - 5))
        echo "[*] Time jump: ${OFFSET}s"
        date -s "$OFFSET seconds"
    fi

    # Randomize again every 15 seconds
    sleep "$SLEEP_SEC"
done