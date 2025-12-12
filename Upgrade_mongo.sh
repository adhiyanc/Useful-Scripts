#!/bin/bash  
# ================================================  
# Local MongoDB Sharded Cluster Manager  
# ================================================  
# Usage:  
#   ./mongo_cluster.sh start  
#   ./mongo_cluster.sh stop [new_mongo_build]  
#  
# If [new_mongo_build] is a .tgz, it will be extracted.  
# If [new_mongo_build] is a directory, it is copied over.  
# Old bin dir is backed up with timestamp.  
# ================================================  
  
set -e  
  
# ===== USER CONFIG =====  
CUSTOM_MONGO_BIN_DIR="$HOME/mongodb-custom/mdb4.4.s8-rhel-8/bin"  
BASE_DIR="$HOME/s28test"  
  
# ===== Functions =====  
start_cluster() {  
    echo "=== Starting Config Servers ==="  
    $CUSTOM_MONGO_BIN_DIR/mongod --configsvr --replSet configRS --port 27019 --dbpath "$BASE_DIR/config/cfg1" --logpath "$BASE_DIR/config/cfg1.log" --fork  
    $CUSTOM_MONGO_BIN_DIR/mongod --configsvr --replSet configRS --port 27020 --dbpath "$BASE_DIR/config/cfg2" --logpath "$BASE_DIR/config/cfg2.log" --fork  
    $CUSTOM_MONGO_BIN_DIR/mongod --configsvr --replSet configRS --port 27021 --dbpath "$BASE_DIR/config/cfg3" --logpath "$BASE_DIR/config/cfg3.log" --fork  
  
    echo "Waiting for config servers..."  
    sleep 5  
  
    echo "=== Starting Shard Servers ==="  
    $CUSTOM_MONGO_BIN_DIR/mongod --replSet rs0 --shardsvr --port 27022 --dbpath "$BASE_DIR/shard1" --logpath "$BASE_DIR/shard1.log" --fork  
    $CUSTOM_MONGO_BIN_DIR/mongod --replSet rs1 --shardsvr --port 27023 --dbpath "$BASE_DIR/shard2" --logpath "$BASE_DIR/shard2.log" --fork  
  
    sleep 5  
  
    echo "=== Starting Mongos Router ==="  
    $CUSTOM_MONGO_BIN_DIR/mongos --configdb configRS/localhost:27019,localhost:27020,localhost:27021 --port 27017 --logpath "$BASE_DIR/mongos/mongos.log" --fork  
  
    echo "=== Cluster Started ==="  
}  
  
stop_cluster() {  
    echo "=== Stopping Mongos ==="  
    pkill -TERM -f "$CUSTOM_MONGO_BIN_DIR/mongos" || true  
  
    echo "=== Stopping Shards ==="  
    $CUSTOM_MONGO_BIN_DIR/mongo --port 27022 --eval "db.shutdownServer()" || true  
    $CUSTOM_MONGO_BIN_DIR/mongo --port 27023 --eval "db.shutdownServer()" || true  
  
    echo "=== Stopping Config Servers ==="  
    $CUSTOM_MONGO_BIN_DIR/mongo --port 27019 --eval "db.shutdownServer()" || true  
    $CUSTOM_MONGO_BIN_DIR/mongo --port 27020 --eval "db.shutdownServer()" || true  
    $CUSTOM_MONGO_BIN_DIR/mongo --port 27021 --eval "db.shutdownServer()" || true  
  
    echo "=== Cluster Stopped ==="  
  
    # Now optionally update binaries if new path provided  
    NEW_BUILD_PATH="$1"  
    if [ -n "$NEW_BUILD_PATH" ]; then  
        echo "=== Updating MongoDB binaries ==="  
        OLD_BIN_PARENT=$(dirname "$CUSTOM_MONGO_BIN_DIR")  
        TIMESTAMP=$(date +"%Y%m%d-%H%M%S")  
        BACKUP_DIR="${OLD_BIN_PARENT}-backup-${TIMESTAMP}"  
  
        echo "Backing up current binaries from $OLD_BIN_PARENT to $BACKUP_DIR"  
        mv "$OLD_BIN_PARENT" "$BACKUP_DIR"  
  
        if [[ "$NEW_BUILD_PATH" == *.tgz ]]; then  
            echo "Extracting tarball $NEW_BUILD_PATH..."  
            tar -xvf "$NEW_BUILD_PATH" -C "$(dirname "$OLD_BIN_PARENT")"  
            EXTRACTED_DIR=$(tar -tzf "$NEW_BUILD_PATH" | head -1 | cut -f1 -d"/")  
            mv "$(dirname "$OLD_BIN_PARENT")/$EXTRACTED_DIR" "$OLD_BIN_PARENT"  
        elif [ -d "$NEW_BUILD_PATH" ]; then  
            echo "Copying binaries from $NEW_BUILD_PATH..."  
            cp -r "$NEW_BUILD_PATH" "$OLD_BIN_PARENT"  
        else  
            echo "Error: $NEW_BUILD_PATH is not a .tgz or directory"  
            exit 1  
        fi  
  
        echo "Binaries updated at $CUSTOM_MONGO_BIN_DIR"  
    fi  
}  
  
# ===== Main =====  
MODE=$1  
OPTION=$2  
  
if [ "$MODE" == "start" ]; then  
    start_cluster  
elif [ "$MODE" == "stop" ]; then  
    stop_cluster "$OPTION"  
else  
    echo "Usage: $0 {start|stop} [new_mongo_build]"  
    exit 1  
fi  
