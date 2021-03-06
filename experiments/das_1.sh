#!/bin/bash

# cores
for n in 5 9 3; do

    # runs
    for i in {1..5}; do
        # order_on_write or order_on_read 
        for k in 0 1; do
            echo "run $i, order_on_write $k, nodes $n"
            prun -v -1 -np $n python3 ./perf_exp_1_das.py $j $k
            sleep 1
        done
    done
done
