from datetime import datetime
import socket
import os
import csv
from time import time

from experiment import Experiment
from system import DasSystem


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

    results = list(experiment.run(perf_exp_1.experiment_func, repeat=1))
    columns = ["system_name", "run_id", "latency", "operation", "on_leader", "n_nodes", "n_clients", "order_on_write"]
    filename = "./results/experiment1_{}.csv".format(datetime.today().strftime("%Y%m%d%H%M%S"))

    with open(filename, 'w', encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(columns)
        writer.writerows(results)

    end = time()
    print('Time: ', end-start)


if __name__ == '__main__':
    main()
