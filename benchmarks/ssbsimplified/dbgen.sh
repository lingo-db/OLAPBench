#!/usr/bin/env bash
set -euo pipefail
SF=${1:-1}

# Directory of this script (holds the simplification helper).
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "Generating simplified Star Schema Benchmark with scale factor $SF"

mkdir -p data/ssbsimplified/
cd data/ssbsimplified/

# Reuse existing datasets
if [ ! -d "sf$SF" ]; then
  (
    # Simplified SSB uses the lingo-db fork of ssb-dbgen, which replaces the
    # string nation/region/brand columns with integer ids (GPU-friendly).
    if [ ! -d "ssb-dbgen" ]; then
      git clone https://github.com/lingo-db/ssb-dbgen.git
    fi
    cmake -B ssb-dbgen-build ssb-dbgen
    cmake --build ssb-dbgen-build

    WORK="$(mktemp --directory)"
    cp "$SCRIPT_DIR/ssb_convert_to_simplified.py" "$WORK/ssb_convert_to_simplified.py"
    cp ssb-dbgen-build/dbgen "$WORK/dbgen"
    # dbgen reads its distribution file from the cwd.
    cp ssb-dbgen/dists.dss "$WORK/dists.dss"

    (
      cd "$WORK"
      ./dbgen -f -T c -s "$SF"
      ./dbgen -qf -T d -s "$SF"
      ./dbgen -qf -T p -s "$SF"
      ./dbgen -qf -T s -s "$SF"
      ./dbgen -q -T l -s "$SF"
      chmod +r ./*.tbl
      # Strip the trailing pipe that ssb-dbgen appends to every row.
      for table in ./*.tbl; do
        sed -i 's/|$//' "$table"
      done
      # Replace nation/region/brand strings with integer ids in place.
      python3 ssb_convert_to_simplified.py ./
    )

    mkdir -p "sf$SF"
    for table in "$WORK"/*.tbl; do
      mv "$table" "sf$SF/$(basename "$table")"
    done
    rm -rf "$WORK"
  )
fi
