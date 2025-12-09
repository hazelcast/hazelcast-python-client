#!/bin/bash

ADDRESS=$1
DURATION=${2:-48.0}

mkdir -p "client_logs"

declare -a pids

cleanup () {
  for pid in "${pids[@]}"; do
    echo "Stopping $pid"
    kill "$pid"
  done
}

trap cleanup EXIT

for i in {1..5}; do
  python map_soak_test_asyncio.py \
      --duration "$DURATION" \
      --address "$ADDRESS" \
      --log-file client_logs/client-asyncio-"$i" &
  pid=$!
  echo "$pid running"
  pids+=("$pid")
done

for i in {1..5}; do
  python map_soak_test.py \
      --duration "$DURATION" \
      --address "$ADDRESS" \
      --log-file client_logs/client-"$i" &
  pid=$!
  echo "$pid running"
  pids+=("$pid")
done

wait
