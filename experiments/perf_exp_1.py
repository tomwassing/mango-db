from datetime import datetime
from time import perf_counter
import random

from experiment import Experiment
from system import System


def experiment_func(experiment, client_idx=0):

    # Alternate the reads and writes
    experiment.reset()
    remaining_writes = experiment.n_writes
    remaining_reads = experiment.n_reads
    while remaining_writes != 0 or remaining_reads != 0:
        latency, operation, on_leader = None, None, None

        if remaining_writes == 0:
            latency, on_leader = experiment.client_read(client_idx=client_idx)
            operation = 'read'
            remaining_reads -= 1
            yield latency, operation, on_leader
            continue

        if remaining_reads == 0:
            latency, on_leader = experiment.client_write(client_idx=client_idx)
            operation = 'write'
            remaining_writes -= 1
            yield latency, operation, on_leader
            continue

        read = True if random.random() <= 0.5 else False

        if read and remaining_writes < experiment.n_writes:
            latency, on_leader = experiment.client_read(client_idx=client_idx)
            operation = 'read'
            remaining_reads -= 1
        else:
            latency, on_leader = experiment.client_write(client_idx=client_idx)
            operation = 'write'
            remaining_writes -= 1

        if latency is None or operation is None or on_leader is None:
            raise Exception('invalid result')

        yield latency, operation, on_leader


if __name__ == '__main__':
    import pandas as pd

    systems = [
        # System(name='ordering_after_write_2_nodes_1_client', num_nodes=2, num_clients=1, port=27000),
        # System(name='ordering_before_write_2_nodes_1_client', num_nodes=2, num_clients=1, port=28000, order_on_write=True),
        # System(name='ordering_after_write_4_nodes_1_client', num_nodes=4, num_clients=1, port=29000),
        # System(name='ordering_before_write_4_nodes_1_client', num_nodes=4, num_clients=1, port=30000, order_on_write=True),
        # System(name='ordering_after_write_8_nodes_1_client', num_nodes=8, num_clients=1, port=31000),
        # System(name='ordering_before_write_8_nodes_1_client', num_nodes=8, num_clients=1, port=32000, order_on_write=True),
        System(name='ordering_after_write_16_nodes_1_client', num_nodes=16, num_clients=1, port=33000),
        System(name='ordering_before_write_16_nodes_1_client', num_nodes=16, num_clients=1, port=34000, order_on_write=True),
    ]

    experiment = Experiment(
        experiment_name='Performance Experiment 1',
        systems=systems,
        n_writes=100000,
        n_reads=100000,
    )

    # Run experiment 5 times
    print("{}".format(experiment.__str__()))
    start = perf_counter()
    results = []
    checkpoint = 4000000

    intermediate_df = None
    for result in experiment.run(experiment_func, repeat=10):
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
    end = perf_counter()
    print('Time: ', end-start)

    # Save experiment results
    results.to_csv("./results/experiment1_{}.csv".format(datetime.today().strftime("%Y%m%d%H%M%S")), index=False)
