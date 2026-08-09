#!/bin/bash
#
# Setup a Blind Peer server on a VPS for ZappMessaging
#
# The blind peer stores encrypted Hypercore blocks and serves them to peers
# that come online later. It CANNOT read any message data -- it only holds
# encrypted blocks that only conversation participants can decrypt.
#
# Prerequisites:
#   - Node.js >= 18
#   - A VPS with a public IP (e.g. DigitalOcean, AWS EC2, Hetzner)
#   - Port 49737 open (UDP + TCP) for DHT connections
#
# Usage:
#   chmod +x scripts/setup-blind-peer.sh
#   ./scripts/setup-blind-peer.sh
#
# After setup, hand the printed public key to the app as BLIND_PEER_KEYS
# (Android) or ZappMessagingConfig.blindPeerKeys (iOS). Both reach the
# worklet as the --blind-peer-keys argv flag.
#

set -e

STORAGE_DIR="${BLIND_PEER_STORAGE:-./blind-peer-data}"
PORT="${BLIND_PEER_PORT:-49737}"
MAX_STORAGE="${BLIND_PEER_MAX_STORAGE:-10gb}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SERVER_DIR="$SCRIPT_DIR/../server"

echo "=== ZappMessaging Blind Peer Setup ==="
echo ""

# Check Node.js
if ! command -v node &> /dev/null; then
    echo "Error: Node.js is required. Install it first:"
    echo "  curl -fsSL https://deb.nodesource.com/setup_20.x | sudo -E bash -"
    echo "  sudo apt-get install -y nodejs"
    exit 1
fi

NODE_VERSION=$(node -v)
echo "Node.js: $NODE_VERSION"

# Install the pinned mirror, connection-relay and invite-mailbox dependencies
echo ""
echo "Installing blind-peer server dependencies..."
npm install --omit=dev --prefix "$SERVER_DIR"

# Create storage directory
mkdir -p "$STORAGE_DIR"

echo ""
echo "=== Starting Blind Peer Server ==="
echo "Storage:     $STORAGE_DIR"
echo "Port:        $PORT"
echo "Max storage: $MAX_STORAGE"
echo ""
echo "IMPORTANT: Copy the public key printed below into the app's blind-peer"
echo "key list. There is no hardcoded list in the JS: the keys are injected as"
echo "--blind-peer-keys at worklet start. Android resolves BLIND_PEER_KEYS from"
echo "local.properties, then a Gradle property, then the environment; iOS sets"
echo "ZappMessagingConfig.blindPeerKeys. The blind_peer.set_keys IPC command"
echo "replaces the list on a running worklet."
echo ""
echo "---"

# Run the core mirror, the HyperDHT `blind-relay` protocol and the bootstrap
# invite mailbox under the same persistent key. A stock blind-peer-cli process
# only provides core mirroring, so Hyperswarm relayThrough connections otherwise
# time out and a peer with no conversations has nowhere to collect its first
# invite. The mailbox also listens on loopback HTTP; put a TLS terminator in
# front of it to serve clients whose network drops UDP. See server/README.md.
export BLIND_PEER_STORAGE="$STORAGE_DIR"
export BLIND_PEER_PORT="$PORT"
export BLIND_PEER_MAX_STORAGE="$MAX_STORAGE"
exec node "$SERVER_DIR/blind-peer-relay.js"
