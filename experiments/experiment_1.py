from experiment import Experiment
import pandas as pd
import numpy as np

if __name__ == '__main__':

    experiment = Experiment(
        experiment_name='Performance Experiment 1',
        num_nodes=2,
        num_clients=1,
        port=25000,
        n_keys=100,
        p_key_repeat=0
    )

    experiment.start_system()

    def experiment_1():
        # Writes in sequence
        write_latencies = []
        for i in range(experiment.n_keys):
            latency = experiment.client_write(client_idx=0)
            write_latencies.append(latency)

        read_latencies = []
        for i in range(experiment.n_keys):
            latency = experiment.client_read(client_idx=0)
            read_latencies.append(latency)

        return write_latencies, read_latencies

    # Repeat the expriment 5 times
    results = pd.DataFrame()
    for write_latencies, read_latencies in experiment.run(experiment_1):
        results['write_latencies'].append(np.mean(write_latencies))
        results['read_latencies'].append(np.mean(read_latencies))

    results.to_csv('experiment1.csv')
