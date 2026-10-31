#!/usr/bin/env bash
# End-to-end check against the docker compose stack: a transfer made through
# the REST API must come out of the notifier, which proves the whole path
# ledger -> outbox -> relay -> Kafka -> notifier works.
set -euo pipefail

LEDGER=${LEDGER:-http://localhost:8080}
NOTIFIER=${NOTIFIER:-http://localhost:8081}
RELAY=${RELAY:-http://localhost:8082}

wait_for() {
  for _ in $(seq 1 90); do
    if curl -fs "$1" > /dev/null; then return 0; fi
    sleep 1
  done
  echo "timeout waiting for $1" >&2
  exit 1
}

json() { python3 -c "import sys, json; print(json.load(sys.stdin)$1)"; }

wait_for "$LEDGER/readyz"
wait_for "$NOTIFIER/healthz"
wait_for "$RELAY/healthz"

suffix=$RANDOM
alice=$(curl -fs -X POST "$LEDGER/api/v1/accounts" -d "{\"owner_id\":\"alice-$suffix\",\"currency\":\"KZT\"}" | json '["id"]')
bob=$(curl -fs -X POST "$LEDGER/api/v1/accounts" -d "{\"owner_id\":\"bob-$suffix\",\"currency\":\"KZT\"}" | json '["id"]')

curl -fs -X POST "$LEDGER/api/v1/deposits" -H "Idempotency-Key: dep-$suffix" \
  -d "{\"account_id\":\"$alice\",\"amount\":1000000}" > /dev/null

code=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$LEDGER/api/v1/deposits" -H "Idempotency-Key: dep-$suffix" \
  -d "{\"account_id\":\"$alice\",\"amount\":1000000}")
[ "$code" = "200" ] || { echo "replayed deposit returned $code, want 200" >&2; exit 1; }

curl -fs -X POST "$LEDGER/api/v1/transfers" -H "Idempotency-Key: tr-$suffix" \
  -d "{\"from_account_id\":\"$alice\",\"to_account_id\":\"$bob\",\"amount\":250050}" > /dev/null

balance=$(curl -fs "$LEDGER/api/v1/accounts/$alice" | json '["balance"]')
[ "$balance" = "749950" ] || { echo "alice balance $balance, want 749950" >&2; exit 1; }

for _ in $(seq 1 30); do
  n=$(curl -fs "$NOTIFIER/api/v1/notifications?owner_id=bob-$suffix" | json '["notifications"].__len__()')
  if [ "$n" = "1" ]; then
    curl -fs "$NOTIFIER/api/v1/notifications?owner_id=bob-$suffix"
    echo
    echo "e2e OK"
    exit 0
  fi
  sleep 1
done
echo "notification for bob did not arrive" >&2
docker compose logs relay notifier | tail -50 >&2 || true
exit 1
