#!/usr/bin/env bash
set -euo pipefail
SF=${1:-1}

echo "Generating TPC-DS database with scale factor $SF"

mkdir -p "data/tpcds/sf$SF"
cd "data/tpcds/"

# Reuse existing datasets
if [ -z "$(ls -A "sf$SF")" ]; then
  (
    if [ ! -d tpcds-kit ]; then
      echo "Downloading tpcds-kit..."
      curl -L -o tpcds-kit.zip https://github.com/lingo-db/tpcds-kit/archive/refs/heads/master.zip
      unzip tpcds-kit.zip
      mv tpcds-kit-master tpcds-kit
      rm tpcds-kit.zip
    else
      echo "tpcds-kit already exists. Skipping download."
    fi

    cd tpcds-kit/tools
    rm -rf ./*.dat

    if [[ "$OSTYPE" == "darwin"* ]]; then
      make OS=MACOS MACOS_CFLAGS="-O3 -Wall -std=gnu90" -sj $(sysctl -n hw.logicalcpu) dsdgen # macOS
    else
      make OS=LINUX LINUX_CFLAGS="-O3 -Wall -std=gnu90" -sj $(nproc) dsdgen # Linux
    fi

    ./dsdgen -FORCE -SCALE "$SF"
    for table in ./*.dat; do
      # sed behaves differently on macOS and linux. Currently, there is no stable, portable command that works on both.
      if [[ "$OSTYPE" == "darwin"* ]]; then
        sed -i '' 's/|$//' "$table"  # macOS
      else
        sed -i 's/|$//' "$table"     # Linux
      fi
      mv "$table" "../../sf$SF/$table"
    done
  )
fi
