#!/usr/bin/env bash
set -euo pipefail
set -x
SF=${1:-1}

echo "Generating TPC-H database with scale factor $SF"

mkdir -p "data/tpch/sf$SF"
cd "data/tpch/"

# Reuse existing datasets
if [ -z "$(ls -A "sf$SF")" ]; then
  (
    wget -q https://github.com/electrum/tpch-dbgen/archive/32f1c1b92d1664dba542e927d23d86ffa57aa253.zip -O tpch-dbgen.zip
    unzip -q -u tpch-dbgen.zip
    rm tpch-dbgen.zip
    cd tpch-dbgen-32f1c1b92d1664dba542e927d23d86ffa57aa253/
    rm -rf ./*.tbl
    if [[ "$OSTYPE" == "darwin"* ]]; then
      make MACHINE=MAC -sj "$(sysctl -n hw.logicalcpu)" dbgen
    else
      make MACHINE=LINUX -sj "$(nproc)" dbgen
    fi

    ./dbgen -f -s $SF
    for table in ./*.tbl; do
      chmod +r "$table"
      # sed behaves differently on macOS and linux. Currently, there is no stable, portable command that works on both.
      if [[ "$OSTYPE" == "darwin"* ]]; then
        sed -i '' 's/|$//' "$table"  # macOS
      else
        sed -i 's/|$//' "$table"     # Linux
      fi
      mv "$table" "../sf$SF/$table"
    done
  )
fi