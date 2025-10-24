#!/bin/bash

set -e

if [ "$#" -lt 6 ]; then
    echo "Usage: $0 CLIENT_NAME CLIENT_PUBLIC_KEY WG_CONFIG_FILE DOCKER_CONTAINER SERVER_ID OWNER_SLUG"
    exit 1
fi

CLIENT_NAME="$1"
CLIENT_PUBLIC_KEY="$2"
WG_CONFIG_FILE="$3"
DOCKER_CONTAINER="$4"
SERVER_ID="$5"
OWNER_SLUG="$6"

if [ -z "$OWNER_SLUG" ]; then
    OWNER_SLUG="$CLIENT_NAME"
fi

pwd=$(pwd)
DATA_DIR="$pwd/data"
SERVER_DATA_DIR="$DATA_DIR/servers/$SERVER_ID"
PROFILE_DIR="$DATA_DIR/profiles/$SERVER_ID/$OWNER_SLUG/$CLIENT_NAME"

mkdir -p "$SERVER_DATA_DIR"

SERVER_CONF_PATH="$SERVER_DATA_DIR/server.conf"
CLIENTS_TABLE_PATH="$SERVER_DATA_DIR/clientsTable"
CLIENT_CONFIG_PATH="$PROFILE_DIR/$CLIENT_NAME.conf"
TRAFFIC_FILE="$PROFILE_DIR/traffic.json"

docker exec -i "$DOCKER_CONTAINER" cat "$WG_CONFIG_FILE" > "$SERVER_CONF_PATH"

awk -v pubkey="$CLIENT_PUBLIC_KEY" '
BEGIN {in_peer=0; skip=0}
/^\[Peer\]/ {
    in_peer=1
    peer_block = $0 "\n"
    next
}
in_peer == 1 {
    peer_block = peer_block $0 "\n"
    if ($0 ~ /^PublicKey\s*=/) {
        split($0, a, " = ")
        if (a[2] == pubkey) {
            skip=1
        }
    }
    if ($0 ~ /^\[Peer\]/ || $0 ~ /^\[Interface\]/) {
        if (skip == 1) {
            skip=0
            in_peer=0
            next
        } else {
            print peer_block
            in_peer=0
        }
    }
    if ($0 == "") {
        if (skip == 1) {
            skip=0
            in_peer=0
            next
        } else {
            print peer_block
            in_peer=0
        }
    }
    next
}
{
    print
}
END {
    if (in_peer == 1 && skip == 1) {
    } else if (in_peer ==1 ) {
        print peer_block
    }
}
' "$SERVER_CONF_PATH" > "$SERVER_CONF_PATH.tmp"

mv "$SERVER_CONF_PATH.tmp" "$SERVER_CONF_PATH"

docker exec -i "$DOCKER_CONTAINER" wg-quick strip "$WG_CONFIG_FILE" > /dev/null

docker cp "$SERVER_CONF_PATH" "$DOCKER_CONTAINER":"$WG_CONFIG_FILE"

docker exec -i "$DOCKER_CONTAINER" sh -c "wg-quick down '$WG_CONFIG_FILE' && wg-quick up '$WG_CONFIG_FILE'"

rm -f "$CLIENT_CONFIG_PATH"
rm -f "$TRAFFIC_FILE"
if [ -d "$PROFILE_DIR" ]; then
    rmdir "$PROFILE_DIR" 2>/dev/null || true
fi

OWNER_DIR="$DATA_DIR/profiles/$SERVER_ID/$OWNER_SLUG"
if [ -d "$OWNER_DIR" ]; then
    rmdir "$OWNER_DIR" 2>/dev/null || true
fi

SERVER_PROFILE_DIR="$DATA_DIR/profiles/$SERVER_ID"
if [ -d "$SERVER_PROFILE_DIR" ]; then
    rmdir "$SERVER_PROFILE_DIR" 2>/dev/null || true
fi

docker exec -i "$DOCKER_CONTAINER" cat /opt/amnezia/awg/clientsTable > "$CLIENTS_TABLE_PATH" || echo "[]" > "$CLIENTS_TABLE_PATH"

if [ -f "$CLIENTS_TABLE_PATH" ]; then
    jq --arg clientId "$CLIENT_PUBLIC_KEY" 'del(.[] | select(.clientId == $clientId))' "$CLIENTS_TABLE_PATH" > "$CLIENTS_TABLE_PATH.tmp"
    mv "$CLIENTS_TABLE_PATH.tmp" "$CLIENTS_TABLE_PATH"
    docker cp "$CLIENTS_TABLE_PATH" "$DOCKER_CONTAINER":/opt/amnezia/awg/clientsTable
fi

echo "Client $CLIENT_NAME успешно удален из WireGuard"
