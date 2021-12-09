from uuid import uuid4
import random
from time import time


class Experiment:

    def __init__(self, experiment_name, systems, n_reads, n_writes, p_key_repeat):

        self.experiment_name = experiment_name
        self.systems = systems
        self._current_system = None

        self.n_reads = n_reads
        self.n_writes = n_writes
        self.keys = [str(uuid4()) for _ in range(n_reads)]
        self.p_key_repeat = p_key_repeat

        self._setup()

    def _setup(self):
        self._unused_keys = self.keys.copy()
        self._used_keys = []

    def _reset_after_run(self):
        self._setup()

    def __str__(self):
        return "{}\n\tn_reads: {}\n\tn_writes: {}\n\tp_key_repeat: {}".format(
            self.experiment_name, self.n_reads, self.n_writes, self.p_key_repeat
        )

    def _run(self, experiment_func, repeat):
        for run_id in range(repeat):
            print('run: {}'.format(run_id))
            self._current_system.start()
            for latency, operation, on_leader in experiment_func():
                system_name = self._current_system.name
                yield [system_name, run_id, latency, operation, on_leader]

            self._current_system.shutdown()
            self._reset_after_run()

            # yield run_ids, latencies, operations, on_leader

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
        repeat_key = random.random() <= self.p_key_repeat # make distribution
        value = len(self.keys)

        if repeat_key and len(self._used_keys) > 0:
            key = random.choice(self._used_keys)
        else:
            if len(self._unused_keys) == 0:
                raise Exception('no unused key to write on')
            key = self._unused_keys.pop()
            self._used_keys.append(key)

        return key, value

    def client_write(self, client_idx):
        key, value = self._get_key_value_pair()

        start = time()
        node_port = self._current_system.clients[client_idx].write(key, value)
        end = time()

        latency = end-start
        write_on_leader = self._is_leader(node_port)
        return latency, write_on_leader

    def client_read(self, client_idx):
        key = random.choice(self._used_keys)

        start = time()
        node_port = self._current_system.clients[client_idx].read(key)
        end = time()

        latency = end - start
        read_on_leader = self._is_leader(node_port)
        return latency, read_on_leader

    def _is_leader(self, port):
        return False
        if self._current_system.ports[-1] == port:
            return True

        return False