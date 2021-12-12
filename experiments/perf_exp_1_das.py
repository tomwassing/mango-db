from datetime import datetime
import socket
import os
import pandas as pd
from time import time

from experiment import Experiment
from experiments.system import DasSystem


import perf_exp_1

def main():
    hostnames = os.getenv('HOSTNAMES').split()
    hostname = socket.gethostname()

    if hostname != hostnames[0]:
        return

    experiment = Experiment(
        experiment_name='Performance Experiment 1',
        systems=[DasSystem(num_clients=1, port=25000)],
        n_writes=100000,
        n_reads=100000,
    )

    # Run experiment 5 times
    print("{}".format(experiment.__str__()))
    start = time()
    results = []
    checkpoint = 4000000

    intermediate_df = None

    for result in experiment.run(perf_exp_1.experiment_func, repeat=1):
        results.append(result)

        if len(results) % checkpoint == 0:
            print("Checkpoint ", len(results)/checkpoint)
            intermediate_df = pd.DataFrame(
                columns=["system_name", "run_id", "latency", "operation", "on_leader", "n_nodes", "n_clients",
                         "order_on_write"],
                data=results
            )
            intermediate_df.to_csv(
                "./results/checkpoint_experiment1_part_{}.csv".format(int(len(results)/checkpoint)),
                index=False
            )

    results = pd.DataFrame(
        columns=["system_name", "run_id", "latency", "operation", "on_leader", "n_nodes", "n_clients", "order_on_write"],
        data=results
    )
    end = time()
    print('Time: ', end-start)

    # Save experiment results
    results.to_csv("./results/experiment1_{}.csv".format(datetime.today().strftime("%Y%m%d%H%M%S")), index=False)

if __name__ == '__main__':
    main()
