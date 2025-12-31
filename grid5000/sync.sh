#!/bin/bash

if [ -z "$1" ]; then
	echo "Missing front end of grid5000"
	exit 1
fi

rsync -av --exclude .venv --exclude geozip_image* --exclude .idea --exclude venv --exclude tools/proto --exclude .git --exclude cmake-build-asan/ --exclude cmake-build-debug/ --exclude cmake-build-release/ --exclude cmake-build-remote/ --exclude backup/ --exclude .github/ --exclude .vscode/ . $1.g5k:~/GenuineDB/
