#!/bin/sh

set -e

root_dir="$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")"

mkdir "$1"
cd "$1"

for i in 0 1 2 3 4 5 6 7 8; do
    n=$(( 2 ** i))
    nx=$(( 2 ** ((i+2)/3) ))
    ny=$(( 2 ** ((i+1)/3) ))
    nz=$(( 2 ** ((i+0)/3) ))

    for c in 10; do
        time USE_HDF=0 TERRA_PATH=$root_dir/../liszt-legion/include/?.t SAVEOBJ=1 OBJNAME=taylor-512x512x256-dop"$n" $root_dir/../regent.py $root_dir/../soleil-x/src/soleil-x.t -i $root_dir/../soleil-x/testcases/taylor_green_vortex/taylor_green_vortex_512_512_256.lua -fopenmp 1 -fflow-spmd 1 -fflow-spmd-shardsize 1 -fparallelize-dop "$nx","$ny","$nz" -fcuda 0 &
        sleep 3
    done
done

wait

cp $root_dir/../../bindings/terra/liblegion_terra.so .
cp $root_dir/../soleil-x/src/libsoleil_mapper.so .

cp $root_dir/../scripts/*_soleil*.sh .
