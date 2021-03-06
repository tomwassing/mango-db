import random
from threading import Thread
from time import perf_counter
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

    def _run(self, experiment_func, repeat, client_idx=None):
        for run_id in range(repeat):
            print('run: {}'.format(run_id))
            self._current_system.start()

            for latency, operation, on_leader in experiment_func(self):
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

    def run_multi_client(self, experiment_func, repeat=1):
        print("Start experiment")

        self._current_system = self.systems[0]
        self.n_reads = self.n_reads // self._current_system.num_clients
        self.n_writes = self.n_writes // self._current_system.num_clients

        client_resuts = [[]]*self._current_system.num_clients

        def client_run(i):
            client_resuts[i] = list(self._run(experiment_func, repeat, client_idx=i))

        for system in self.systems:
            threads = [Thread(target=client_run, args=(i,)) for i in range(system.num_clients)]

            for thread in threads:
                thread.start()

            for thread in threads:
                thread.join()

            self._current_system = None

            yield list(sum(client_resuts, []))

        print("Experiment completed\n\n")

    def reset(self):
        self._setup()

    def _get_key_value_pair(self):
        value = random.random()
        key = str(np.random.zipf(1.1))
        self._used_keys.append(key)

        return key, value

    def client_write(self, client_idx):
            try:
                key, value = self._get_key_value_pair()
                
                start = perf_counter()
                node_host = self._current_system.clients[client_idx].write(key, value)
                end = perf_counter()

                latency = end-start
                write_on_leader = self._is_leader(node_host)
                return latency, write_on_leader
            except Exception as e:
                print(e)
                return None, False

    def client_read(self, client_idx):
        try:
            key = random.choice(self._used_keys)

            start = perf_counter()
            node_host = self._current_system.clients[client_idx].read(key)['host']
            end = perf_counter()

            latency = end - start
            read_on_leader = self._is_leader(node_host)
            return latency, read_on_leader
        except Exception as e:
            print(e)
            return None, False

    def _is_leader(self, host):
        return self._current_system.node_hosts[-1] == host