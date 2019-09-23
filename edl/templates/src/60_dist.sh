#!/bin/bash

# -----------------------------------------------------------------------------
# 60_dist.sh : create distribution to archive
# -----------------------------------------------------------------------------

mkdir -p ./dist/zip
mkdir -p ./dist/db
cp -rv ./zip/*.zip ./dist/zip/.
cp -rv ./zip/state.txt ./dist/zip/.
cp -v ./db/*.db ./dist/db/.
pigz ./dist/db/*.db
