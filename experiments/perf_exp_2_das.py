from datetime import datetime
import socket
import os
import csv
import logging
import time
import sys

from experiment import Experiment
from system import DasSystem


import perf_exp_2

def main(order_on_write, read_heavy):
    logging.basicConfig(format='%(asctime)s.%(msecs)03d - %(levelname)-8s: %(message)s',
                        level=logging.WARNING,datefmt='%d-%m-%y %H:%M:%S')
    
    hostnames = os.getenv('HOSTS').split()
    hostname = socket.gethostname()

    system = DasSystem(num_clients=10, port=25000, order_on_write=order_on_write)
    if hostname != hostnames[0]:
        system.start()
        return

    time.sleep(15)

    read_factor = 2 if read_heavy else 1
    write_factor = 1 if read_heavy else 2

    experiment = Experiment(
        experiment_name='Performance Experiment 2',
        systems=[system],
        n_writes=100000 * write_factor,
        n_reads=100000 * read_factor,
    )

    # Run experiment 5 times
    print("{}".format(experiment.__str__()))
    start = time.perf_counter()

    exp_func = perf_exp_2.read_heave_exp_func if read_heavy else perf_exp_2.write_heavy_exp_func

    results = list(experiment.run_multi_client(exp_func, repeat=1))[0]
    columns = ["system_name", "run_id", "latency", "operation", "on_leader", "n_nodes", "n_clients", "order_on_write"]
    filename = "./results/experiment2_{}_{}_{}.csv".format(order_on_write, read_heavy, datetime.today().strftime("%Y%m%d%H%M%S"))

    with open(filename, 'w', encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(columns)
        writer.writerows(results)

    end = time.perf_counter()
    print('Time: ', end-start)

if __name__ == '__main__':
    order_on_write = len(sys.argv) > 1 and int(sys.argv[1]) == 1
    read_heavy = len(sys.argv) > 2 and int(sys.argv[2]) == 1
    main(order_on_write, read_heavy)
