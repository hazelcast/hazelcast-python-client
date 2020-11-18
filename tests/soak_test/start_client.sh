#!/bin/sh

PID=$$
ADDRESSES=$1
HOUR=${2:-48.0}

mkdir -p "client_logs"

python map_soak_test.py --hour "$HOUR" --addresses "$ADDRESSES" --log "client_logs/client-${PID}" &