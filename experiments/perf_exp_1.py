from datetime import datetime
import pandas as pd
import numpy as np

from experiment import Experiment
from system import System


def experiment_func():
    # Writes in sequence
    write_latencies = []
    for i in range(experiment.n_writes):
        latency = experiment.client_write(client_idx=0)
        write_latencies.append(latency)

    read_latencies = []
    for i in range(experiment.n_reads):
        latency = experiment.client_read(client_idx=0)
        read_latencies.append(latency)

    return write_latencies, read_latencies


if __name__ == '__main__':

    our_system = System(name='ordering_after_write', num_nodes=2, num_clients=1, port=25000)

    # TODO: implement actual benchmark system
    benchmark_system = System(name='ordering_before_write', num_nodes=2, num_clients=1, port=26000)

    experiment = Experiment(
        experiment_name='Performance Experiment 1',
        systems=[our_system, benchmark_system],
        n_writes=100,
        n_reads=100,
        p_key_repeat=0
    )

    # Run experiment 5 times
    results = pd.DataFrame()
    print("{}".format(experiment.__str__()))

    for system_results in experiment.run(experiment_func, repeat=10):
        system_name = system_results.get("system")
        mean_w_lats = system_results.get("mean_w_lats")
        mean_r_lats = system_results.get("mean_r_lats")

        if not (system_name and mean_w_lats and mean_r_lats): raise Exception('Missing values in experiment results.')

        results["write_latencies_{}".format(system_name)] = pd.Series(mean_w_lats, dtype=np.float64)
        results["read_latencies_{}".format(system_name)] = pd.Series(mean_r_lats,  dtype=np.float64)

    # Save experiment results
    results.to_csv("./results/experiment1_{}.csv".format(datetime.today().strftime("%Y%m%d%H%M%S")))
