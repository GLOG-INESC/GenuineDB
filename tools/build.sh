#!/bin/bash

SRC_DIR="$(pwd)"
BUILD_DIR="$SRC_DIR/build"
EXEC_DIR="$SRC_DIR/execs"

usage() {
    echo "Usage: $0 build_dir LOCK_MODE SYSTEM REPLICATION EXECUTION --event"
    echo "Options:"
    echo "  build_dir     Name of build folder to which compile the system"
    echo "  LOCK_MODE     Lock module to compile with (RMA or DDR)"
    echo "  SYSTEM        System to compile (GLOG or DETOCK)"
    echo "  REPLICATION   Replication scheme to use (FULL or PARTIAL)"
    echo "  EXECUTION     Execution scheme to use (FULL or PARTIAL)"
    echo "  EVENT_BUILD   Activate events or not"
    echo "  DEBUG_BUILD   Build in debug mode for profiling"
    exit 1
}

if [ "$#" -lt 5 ] || [ "$#" -gt 7 ]; then
  echo "Wrong number of arguments"
  echo $#
  usage
fi

SPECIFIC_BUILD_DIR=$1
LOCK_MODE=$2
SYSTEM=$3
REPLICATION=$4
EXECUTION=$5

EVENTS=OFF
BUILD_TYPE=release

if [ "$#" -gt 5 ]; then
  EVENTS=$6
  BUILD_TYPE=$7
fi

# Create build folder if it does not exist
mkdir -p $EXEC_DIR
mkdir -p $BUILD_DIR

mkdir -p $BUILD_DIR/$SPECIFIC_BUILD_DIR \
        && cd $BUILD_DIR/$SPECIFIC_BUILD_DIR \
        && cmake $SRC_DIR -DLOCK_MANAGER=$LOCK_MODE -DSYSTEM=$SYSTEM -DREPLICATION_SCHEME=$REPLICATION -DEXECUTION_SCHEME=$EXECUTION -DBUILD_SLOG_TESTS=OFF -DCMAKE_BUILD_TYPE=$BUILD_TYPE -DENABLE_TXN_EVENT_RECORDING=$EVENTS \
        && make -j$(nproc) \
        && cd $SRC_DIR

rm -rf $EXEC_DIR/$SPECIFIC_BUILD_DIR
mkdir $EXEC_DIR/$SPECIFIC_BUILD_DIR

executables=("slog" "zip_order" "client" "benchmark" "global_log" "tiga")

for exec in "${executables[@]}"; do
  cp $BUILD_DIR/$SPECIFIC_BUILD_DIR/$exec $EXEC_DIR/$SPECIFIC_BUILD_DIR
done
