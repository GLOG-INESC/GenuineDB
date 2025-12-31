#!/bin/bash

#BUILD_DIR=~/GeoZip
#SRC_DIR=~/GeoZip
#EXEC_DIR=~/GeoZip/execs

SRC_DIR="$(pwd)"
BUILD_DIR="$SRC_DIR/build"
EXEC_DIR="$SRC_DIR/execs"

CLEAN=false

while [ $# -gt 0 ]; do
    case "$1" in
        --clean)
            CLEAN=true
            ;;
        --src-dir)
            SRC_DIR="$2"
            shift 2
            ;;
        --build-dir)
            BUILD_DIR="$2"
            shift 2
            ;;
        *)
    esac
    shift
done

if $CLEAN; then
  rm -rf "$BUILD_DIR" && rm -rf "$EXEC_DIR"
fi

mkdir -p $EXEC_DIR
mkdir -p $BUILD_DIR

systems=("glog RMA GLOG PARTIAL PARTIAL"
         "detock DDR_OPT DETOCK PARTIAL PARTIAL"
         "slog RMA DETOCK PARTIAL PARTIAL"
         "tiga RMA TIGA PARTIAL PARTIAL"
         )


executables=("slog" "zip_order" "client" "benchmark" "global_log" "tiga")

for sys in "${systems[@]}"; do
  read dir_name lock_manager sys_name replication execution <<< "$sys"
  mkdir -p $BUILD_DIR/$dir_name \
    && cd $BUILD_DIR/$dir_name \
    && cmake $SRC_DIR -DLOCK_MANAGER=$lock_manager -DSYSTEM=$sys_name -DREPLICATION_SCHEME=$replication -DEXECUTION_SCHEME=$execution -DBUILD_SLOG_TESTS=OFF -DCMAKE_BUILD_TYPE=release \
    && make -j$(nproc)

  rm -rf $EXEC_DIR/$dir_name && mkdir -p $EXEC_DIR/$dir_name

  for exec in "${executables[@]}"; do
    cp $BUILD_DIR/$dir_name/$exec $EXEC_DIR/$dir_name
  done

done
