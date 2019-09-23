#!/bin/bash

# -----------------------------------------------------------------------------
# 60_dist.sh : create distribution to archive
# -----------------------------------------------------------------------------

mkdir -p ./dist/zip
mkdir -p ./dist/db
cp -r ./zip/*.zip ./dist/zip/.
cp -r ./zip/state.txt ./dist/zip/.
cp ./db/*.db ./dist/db/.
pigz ./dist/db/*.db
