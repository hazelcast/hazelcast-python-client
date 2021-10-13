#!/bin/bash

ADDRESS=$1
DURATION=${2:-48.0}

mkdir -p "client_logs"

for i in {0..9}
do
    python map_soak_test.py \
        --duration "$DURATION" \
        --address "$ADDRESS" \
        --log-file client_logs/client-"$i" &
done
