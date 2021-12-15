from datetime import datetime
import random
from time import perf_counter

from experiment import Experiment
from system import System


def read_heave_exp_func(experiment):
    # Alternate the reads and writes
    experiment.reset()
    remaining_writes = experiment.n_writes
    remaining_reads = experiment.n_reads
    while remaining_writes != 0 or remaining_reads != 0:
        latency, operation, on_leader = None, None, None

        if remaining_writes == 0:
            latency, on_leader = experiment.client_read(client_idx=0)
            operation = 'read'
            remaining_reads -= 1
            yield latency, operation, on_leader
            continue

        if remaining_reads == 0:
            latency, on_leader = experiment.client_write(client_idx=0)
            operation = 'write'
            remaining_writes -= 1
            yield latency, operation, on_leader
            continue

        read = True if random.random() <= 0.66 else False

        if read and remaining_writes < experiment.n_writes:
            latency, on_leader = experiment.client_read(client_idx=0)
            operation = 'read'
            remaining_reads -= 1
        else:
            latency, on_leader = experiment.client_write(client_idx=0)
            operation = 'write'
            remaining_writes -= 1

        # if latency is None or operation is None or on_leader is None:
        #     raise Exception('invalid result')

        yield latency, operation, on_leader


def write_heavy_exp_func(experiment):
    # Alternate the reads and writes
    experiment.reset()
    remaining_writes = experiment.n_writes
    remaining_reads = experiment.n_reads
    while remaining_writes != 0 or remaining_reads != 0:
        latency, operation, on_leader = None, None, None

        if remaining_writes == 0:
            latency, on_leader = experiment.client_read(client_idx=0)
            operation = 'read'
            remaining_reads -= 1
            yield latency, operation, on_leader
            continue

        if remaining_reads == 0:
            latency, on_leader = experiment.client_write(client_idx=0)
            operation = 'write'
            remaining_writes -= 1
            yield latency, operation, on_leader
            continue

        read = True if random.random() <= 0.33 else False

        if read and remaining_writes < experiment.n_writes:
            latency, on_leader = experiment.client_read(client_idx=0)
            operation = 'read'
            remaining_reads -= 1
        else:
            latency, on_leader = experiment.client_write(client_idx=0)
            operation = 'write'
            remaining_writes -= 1

        # if latency is None or operation is None or on_leader is None:
        #     raise Exception('invalid result')

        yield latency, operation, on_leader

def run_experiment(experiment, experiment_func, result_filename, checkpoint):
    import pandas as pd
    # Run experiment 5 times
    print("{}".format(experiment.__str__()))
    start = perf_counter()
    results = []

    intermediate_df = None
    for result in experiment.run(experiment_func, repeat=10):
        results.append(result)

        if len(results) % checkpoint == 0:
            print("Checkpoint ", len(results) / checkpoint)
            intermediate_df = pd.DataFrame(
                columns=["system_name", "run_id", "latency", "operation", "on_leader", "n_nodes", "n_clients",
                         "order_on_write"],
                data=results
            )
            intermediate_df.to_csv(
                "./results/checkpoint_{}_{}".format(result_filename, int(len(results) / checkpoint)),
                index=False
            )

    results = pd.DataFrame(
        columns=["system_name", "run_id", "latency", "operation", "on_leader", "n_nodes", "n_clients",
                 "order_on_write"],
        data=results
    )
    end = perf_counter()
    print('Time: ', end - start)

    # Save experiment results
    results.to_csv("./results/{}_{}.csv".format(result_filename, datetime.today().strftime("%Y%m%d%H%M%S")), index=False)


if __name__ == '__main__':

    systems = [
        System(name='ordering_after_write_4_nodes_1_client', num_nodes=4, num_clients=1, port=27000),
        System(name='ordering_before_write_4_nodes_1_client', num_nodes=4, num_clients=1, port=28000, order_on_write=True),
    ]

    # Read-heavy
    read_heavy_experiment = Experiment(
        experiment_name='Performance Experiment 2 (read-heavy)',
        systems=systems,
        n_writes=100000,
        n_reads=200000,
    )

    write_heavy_experiment = Experiment(
        experiment_name='Performance Experiment 2 (write-heavy)',
        systems=systems,
        n_writes=200000,
        n_reads=100000,
    )

    run_experiment(read_heavy_experiment, read_heave_exp_func, "read_heavy_perf_exp", checkpoint=6000000)
    run_experiment(write_heavy_experiment, write_heavy_exp_func, "write_heavy_perf_exp", checkpoint=6000000)

