#!/bin/sh
#SBATCH --nodes=8
#SBATCH --constraint=gpu
#SBATCH --time=00:30:00
#SBATCH --mail-type=ALL

root_dir="$PWD"

export LD_LIBRARY_PATH="$PWD"

if [[ ! -d tracing ]]; then mkdir tracing; fi
pushd tracing

for n in 8 4 2 1; do
    for r in 0 1 2 3 4; do
        if [[ ! -f out_"$n"x10_r"$r".log ]]; then
            echo "Running $n""x10_r$r""..."
            srun -n $n -N $n --ntasks-per-node 1 --cpu_bind none /lib64/ld-linux-x86-64.so.2 "$root_dir/circuit.spmd10" -npp 2500 -wpp 10000 -l 100 -p $(( 64 * 10 )) -ll:csize 30000 -ll:rsize 512 -ll:gsize 0 -ll:cpu 10 -ll:io 1 -ll:util 2 -ll:dma 2 -lg:prof 4 -lg:prof_logfile prof_"$n"x10_r"$r"_%.gz | tee out_"$n"x10_r"$r".log
        fi
    done
done

popd
