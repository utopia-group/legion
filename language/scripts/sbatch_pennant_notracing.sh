#!/bin/sh
#SBATCH --nodes=64
#SBATCH --constraint=gpu
#SBATCH --time=00:30:00
#SBATCH --mail-type=ALL

root_dir="$PWD"

export LD_LIBRARY_PATH="$PWD"

if [[ ! -d notracing ]]; then mkdir notracing; fi
pushd notracing

for n in 64 32 16; do
    for r in 0 1 2 3 4; do
        if [[ ! -f out_"$n"x10_r"$r".log ]]; then
            echo "Running $n""x10_r$r""..."
            srun -n $n -N $n --ntasks-per-node 1 --cpu_bind none /lib64/ld-linux-x86-64.so.2 "$root_dir/pennant.spmd10" "$root_dir/pennant.tests/leblanc_long4x30/leblanc.pnt" -npieces $(( $n * 10 )) -numpcx 1 -numpcy $(( $n * 10 )) -seq_init 0 -par_init 1 -print_ts 1 -prune 5 -ll:cpu 10 -ll:io 1 -ll:util 1 -ll:dma 1 -ll:ht_sharing 0 -ll:csize 30000 -ll:rsize 512 -ll:gsize 0 -lg:no_physical_tracing | tee out_"$n"x10_r"$r".log
            # -lg:prof 4 -lg:prof_logfile prof_"$n"x10_r"$r"_%.gz
        fi
    done
done

popd
