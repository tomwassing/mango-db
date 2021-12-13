#!/bin/bash

# cores
for n in 5, 7, 9; do

    # runs
    for i in {1..10}; do

        # read_heavy or write_heavy
        for j in 0 1; do

            # order_on_write or order_on_read 
            for k in 0 1; do
                echo "run $i, read_heavy $j, order_on_write $k, nodes $n"
                prun -v -1 -np $n python3 ./perf_exp_2_das.py $j $k
                sleep 1
            done
        done
    done
done
