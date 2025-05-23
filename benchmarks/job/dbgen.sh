#!/usr/bin/env bash
set -euo pipefail

echo "Generating JOB database"

mkdir -p data
cd data

# Reuse existing datasets
if [ ! -d "job" ]; then
  (
    mkdir job
    cd job

    if [[ "$OSTYPE" == "darwin"* ]]; then
      if [ ! -f imdb.tzst ] || ! [ "$(md5 -q imdb.tzst 2>/dev/null)" = "552a24e5bbe0b9bd727649200294bbac" ]; then
        curl -OL https://db.in.tum.de/~schmidt/dbgen/job/imdb.tzst
      fi
      [ "$(md5 -q imdb.tzst)" = "552a24e5bbe0b9bd727649200294bbac" ] || { echo "MD5 mismatch"; exit 1; }
      tar -xf imdb.tzst
    else
      echo '552a24e5bbe0b9bd727649200294bbac imdb.tzst' | md5sum --check --status 2>/dev/null || curl -OL https://db.in.tum.de/~schmidt/dbgen/job/imdb.tzst
      echo '552a24e5bbe0b9bd727649200294bbac imdb.tzst' | md5sum --check --status
      tar --skip-old-files -xf imdb.tzst
    fi
    rm imdb.tzst

    if [[ "$OSTYPE" == "darwin"* ]]; then
      for table in ./*.csv; do
        sed -E -i '' 's/([^\\])(\\{2})*(\\)(")/\1\2""/g' "$table"
        sed -E -i '' 's/([^\\])(\\{2})*(\\)(")/\1\2""/g' "$table"
      done
    else
      for table in ./*.csv; do
        sed -E -i 's/([^\\])(\\{2})*(\\)(")/\1\2""/g' "$table"
        sed -E -i 's/([^\\])(\\{2})*(\\)(")/\1\2""/g' "$table"
      done
    fi
  )
fi
