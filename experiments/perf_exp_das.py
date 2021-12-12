from datetime import datetime
import socket
import os
import csv
import logging
from time import time

from experiment import Experiment
from system import DasSystem


import perf_exp_2

def main():
    logging.basicConfig(format='%(asctime)s.%(msecs)03d - %(levelname)-8s: %(message)s',
                        level=logging.DEBUG,datefmt='%d-%m-%y %H:%M:%S')
    hostnames = os.getenv('HOSTS').split()
    hostname = socket.gethostname()

    system = DasSystem(num_clients=1, port=25000)
    if hostname != hostnames[0]:
        system.start()
        return

    experiment = Experiment(
        experiment_name='Performance Experiment 1',
        systems=[system],
        n_writes=100000,
        n_reads=100000,
    )


    # Run experiment 5 times
    print("{}".format(experiment.__str__()))
    start = time()

    results = list(experiment.run(perf_exp_2.read_heave_exp_func, repeat=1))
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
