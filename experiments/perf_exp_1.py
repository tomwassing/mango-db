from datetime import datetime
import pandas as pd
import numpy as np
import random

from experiment import Experiment
from system import System


def experiment_func():

    # Alternate the reads and writes
    experiment.reset()
    n_writes = experiment.n_writes
    n_reads = experiment.n_reads
    system_name = experiment._current_system

    while n_writes != 0 and n_reads != 0:
        latency, operation, on_leader = None, None, None
        read = True if random.random() <= 0.5 else False

        if (not read and n_writes != 0) or n_writes == experiment.n_writes:
            latency, on_leader = experiment.client_write(client_idx=0)
            operation = 'write'
            n_writes -= 1

        if read and n_reads != 0 and n_writes != experiment.n_writes:
            latency, on_leader = experiment.client_read(client_idx=0)
            operation = 'read'
            n_reads -= 1

        if latency is None or operation is None or on_leader is None:
            raise Exception('invalid result')

        yield latency, operation, on_leader

    return results


if __name__ == '__main__':

    systems = [
        System(name='ordering_after_write_2_nodes_1_client', num_nodes=2, num_clients=1, port=27000),
        System(name='ordering_before_write_2_nodes_1_client', num_nodes=2, num_clients=1, port=27000, order_on_write=True),
        System(name='ordering_after_write_4_nodes_1_client', num_nodes=4, num_clients=1, port=27000),
        System(name='ordering_before_write_4_nodes_1_client', num_nodes=4, num_clients=1, port=27000, order_on_write=True),
        System(name='ordering_after_write_8_nodes_1_client', num_nodes=8, num_clients=1, port=27000),
        System(name='ordering_before_write_8_nodes_1_client', num_nodes=8, num_clients=1, port=27000, order_on_write=True),
    ]

    experiment = Experiment(
        experiment_name='Performance Experiment 1',
        systems=systems,
        n_writes=100,
        n_reads=100,
    )

    # Run experiment 5 times
    results = pd.DataFrame(columns=["system_name", "run_id", "latency", "operation", "on_leader", "n_nodes", "n_clients"])

    print("{}".format(experiment.__str__()))

    for result in experiment.run(experiment_func, repeat=10):
        results.loc[results.shape[0]] = result

    # Save experiment results
    results.to_csv("./results/experiment1_{}.csv".format(datetime.today().strftime("%Y%m%d%H%M%S")), index=False)
