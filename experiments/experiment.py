import random
from time import perf_counter_ns, time, time_ns
import numpy as np


class Experiment:

    def __init__(self, experiment_name, systems, n_reads, n_writes):

        self.experiment_name = experiment_name
        self.systems = systems
        self._current_system = None

        self.n_reads = n_reads
        self.n_writes = n_writes

        self._used_keys = []

    def _setup(self):
        self._used_keys = []

    def _reset_after_run(self):
        self._setup()

    def __str__(self):
        return "{}\n\tn_reads: {}\n\tn_writes: {}".format(
            self.experiment_name, self.n_reads, self.n_writes
        )

    def _run(self, experiment_func, repeat):
        for run_id in range(repeat):
            print('run: {}'.format(run_id))
            self._current_system.start()
            for latency, operation, on_leader in experiment_func():
                system_name = self._current_system.name
                n_nodes = self._current_system.num_nodes
                n_clients = self._current_system.num_clients
                order_on_write = self._current_system.order_on_write
                yield [system_name, run_id, latency, operation, on_leader, n_nodes, n_clients, order_on_write]

            self._current_system.shutdown()

    def run(self, experiment_func, repeat=1):
        print("Start experiment")

        for system in self.systems:
            print(system.name)
            self._current_system = system
            for result in self._run(experiment_func, repeat):
                yield result
            self._current_system = None

        print("Experiment completed\n\n")

    def reset(self):
        self._setup()

    def _get_key_value_pair(self):
        value = random.random()
        key = str(np.random.zipf(1.1))
        self._used_keys.append(key)

        return key, value

    def client_write(self, client_idx):
        key, value = self._get_key_value_pair()

        start = perf_counter_ns()
        node_port = self._current_system.clients[client_idx].write(key, value)
        end = perf_counter_ns()

        latency = end-start
        write_on_leader = self._is_leader(node_port)
        return latency, write_on_leader

    def client_read(self, client_idx):
        key = random.choice(self._used_keys)

        start = perf_counter_ns()
        node_port = self._current_system.clients[client_idx].read(key)['port']
        end = perf_counter_ns()

        latency = end - start
        read_on_leader = self._is_leader(node_port)
        return latency, read_on_leader

    def _is_leader(self, port):
        return self._current_system.ports[-1] == port