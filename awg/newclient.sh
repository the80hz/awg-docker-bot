#!/bin/bash

set -e

if [ "$#" -lt 6 ]; then
    echo "Usage: $0 CLIENT_NAME ENDPOINT WG_CONFIG_FILE DOCKER_CONTAINER SERVER_ID OWNER_SLUG"
    exit 1
fi

CLIENT_NAME="$1"
ENDPOINT="$2"
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
mkdir -p "$PROFILE_DIR"

CONFIG_FILE="$DATA_DIR/setting.ini"
if [ ! -f "$CONFIG_FILE" ]; then
    echo "Error: Configuration file not found"
    exit 1
fi

IS_REMOTE=$(awk -F "=" '/is_remote/ {print $2}' "$CONFIG_FILE" | tr -d ' ')
SERVER_CONF_PATH="$SERVER_DATA_DIR/server.conf"
CLIENTS_TABLE_PATH="$SERVER_DATA_DIR/clientsTable"
CLIENT_CONFIG_PATH="$PROFILE_DIR/$CLIENT_NAME.conf"
TRAFFIC_FILE="$PROFILE_DIR/traffic.json"

if [ "$IS_REMOTE" = "true" ]; then
    REMOTE_HOST=$(awk -F "=" '/remote_host/ {print $2}' "$CONFIG_FILE" | tr -d ' ')
    REMOTE_USER=$(awk -F "=" '/remote_user/ {print $2}' "$CONFIG_FILE" | tr -d ' ')
    REMOTE_PORT=$(awk -F "=" '/remote_port/ {print $2}' "$CONFIG_FILE" | tr -d ' ')
    
    remote_cmd() {
        ssh -p "$REMOTE_PORT" "$REMOTE_USER@$REMOTE_HOST" "$1"
    }
    
    docker_cmd() {
        remote_cmd "docker $1"
    }
else
    docker_cmd() {
        docker $1
    }
fi

if [[ ! "$CLIENT_NAME" =~ ^[a-zA-Z0-9_-]+$ ]]; then
    echo "Error: Invalid CLIENT_NAME. Only letters, numbers, underscores, and hyphens are allowed."
    exit 1
fi

pwd=$(pwd)
if [ "$IS_REMOTE" = "true" ]; then
    key=$(docker_cmd "exec -i $DOCKER_CONTAINER wg genkey")
    psk=$(docker_cmd "exec -i $DOCKER_CONTAINER wg genpsk")
    
    docker_cmd "exec -i $DOCKER_CONTAINER cat $WG_CONFIG_FILE" > "$SERVER_CONF_PATH"
else
    key=$(docker exec -i $DOCKER_CONTAINER wg genkey)
    psk=$(docker exec -i $DOCKER_CONTAINER wg genpsk)
    
    docker exec -i $DOCKER_CONTAINER cat $WG_CONFIG_FILE > "$SERVER_CONF_PATH"
fi

SERVER_PRIVATE_KEY=$(awk '/^PrivateKey\s*=/ {print $3}' "$SERVER_CONF_PATH")

if [ "$IS_REMOTE" = "true" ]; then
    SERVER_PUBLIC_KEY=$(echo "$SERVER_PRIVATE_KEY" | docker_cmd "exec -i $DOCKER_CONTAINER wg pubkey")
else
    SERVER_PUBLIC_KEY=$(echo "$SERVER_PRIVATE_KEY" | docker exec -i $DOCKER_CONTAINER wg pubkey)
fi

LISTEN_PORT=$(awk '/ListenPort\s*=/ {print $3}' "$SERVER_CONF_PATH")
ADDITIONAL_PARAMS=$(awk '/^Jc\s*=|^Jmin\s*=|^Jmax\s*=|^S1\s*=|^S2\s*=|^H[1-4]\s*=/' "$SERVER_CONF_PATH")

octet=2
while grep -E "AllowedIPs\s*=\s*10\.8\.1\.$octet/32" "$SERVER_CONF_PATH" > /dev/null; do
    (( octet++ ))
done

if [ "$octet" -gt 254 ]; then
    echo "Error: WireGuard internal subnet 10.8.1.0/24 is full"
    exit 1
fi

CLIENT_IP="10.8.1.$octet/32"
ALLOWED_IPS="$CLIENT_IP"

if [ "$IS_REMOTE" = "true" ]; then
    CLIENT_PUBLIC_KEY=$(echo "$key" | docker_cmd "exec -i $DOCKER_CONTAINER wg pubkey")
else
    CLIENT_PUBLIC_KEY=$(echo "$key" | docker exec -i $DOCKER_CONTAINER wg pubkey)
fi

cat << EOF >> "$SERVER_CONF_PATH"
[Peer]
# $CLIENT_NAME
PublicKey = $CLIENT_PUBLIC_KEY
PresharedKey = $psk
AllowedIPs = $ALLOWED_IPS

EOF

if [ "$IS_REMOTE" = "true" ]; then
    scp -P "$REMOTE_PORT" "$SERVER_CONF_PATH" "$REMOTE_USER@$REMOTE_HOST:/tmp/server.conf"
    remote_cmd "docker cp /tmp/server.conf $DOCKER_CONTAINER:$WG_CONFIG_FILE"
    remote_cmd "rm /tmp/server.conf"
    docker_cmd "exec -i $DOCKER_CONTAINER sh -c \"wg-quick down $WG_CONFIG_FILE && wg-quick up $WG_CONFIG_FILE\""
else
    docker cp "$SERVER_CONF_PATH" $DOCKER_CONTAINER:$WG_CONFIG_FILE
    docker exec -i $DOCKER_CONTAINER sh -c "wg-quick down $WG_CONFIG_FILE && wg-quick up $WG_CONFIG_FILE"
fi

cat << EOF > "$CLIENT_CONFIG_PATH"
[Interface]
Address = $CLIENT_IP
DNS = 1.1.1.1, 1.0.0.1
PrivateKey = $key
$ADDITIONAL_PARAMS
[Peer]
PublicKey = $SERVER_PUBLIC_KEY
PresharedKey = $psk
AllowedIPs = 0.0.0.0/0
Endpoint = $ENDPOINT:$LISTEN_PORT
PersistentKeepalive = 25
EOF

if [ "$IS_REMOTE" = "true" ]; then
    docker_cmd "exec -i $DOCKER_CONTAINER cat /opt/amnezia/awg/clientsTable" > "$CLIENTS_TABLE_PATH" || echo "[]" > "$CLIENTS_TABLE_PATH"
else
    docker exec -i $DOCKER_CONTAINER cat /opt/amnezia/awg/clientsTable > "$CLIENTS_TABLE_PATH" || echo "[]" > "$CLIENTS_TABLE_PATH"
fi

CREATION_DATE=$(date)
if [ -f "$CLIENTS_TABLE_PATH" ]; then
    jq --arg clientId "$CLIENT_PUBLIC_KEY" \
    --arg clientName "$CLIENT_NAME" \
    --arg creationDate "$CREATION_DATE" \
    '. += [{"clientId": $clientId, "userData": {"clientName": $clientName, "creationDate": $creationDate}}]' \
    "$CLIENTS_TABLE_PATH" > "$CLIENTS_TABLE_PATH.tmp"
    mv "$CLIENTS_TABLE_PATH.tmp" "$CLIENTS_TABLE_PATH"
else
    jq -n --arg clientId "$CLIENT_PUBLIC_KEY" \
    --arg clientName "$CLIENT_NAME" \
    --arg creationDate "$CREATION_DATE" \
    '[{"clientId": $clientId, "userData": {"clientName": $clientName, "creationDate": $creationDate}}]' \
    > "$CLIENTS_TABLE_PATH"
fi

if [ "$IS_REMOTE" = "true" ]; then
    scp -P "$REMOTE_PORT" "$CLIENTS_TABLE_PATH" "$REMOTE_USER@$REMOTE_HOST:/tmp/clientsTable"
    remote_cmd "docker cp /tmp/clientsTable $DOCKER_CONTAINER:/opt/amnezia/awg/clientsTable"
    remote_cmd "rm /tmp/clientsTable"
else
    docker cp "$CLIENTS_TABLE_PATH" $DOCKER_CONTAINER:/opt/amnezia/awg/clientsTable
fi

echo '{
"total_incoming": 0,
"total_outgoing": 0,
"last_incoming": 0,
"last_outgoing": 0
}' > "$TRAFFIC_FILE"

echo "Client $CLIENT_NAME successfully added to WireGuard"
