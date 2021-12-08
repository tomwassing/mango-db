from uuid import uuid4
import random
from time import time
import numpy as np


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
        # this could be helpful for functional test to verify the content
        # self.data = {key: 0 for key in self.keys}

    def _reset_after_run(self):
        self._setup()

    def __str__(self):
        return "{}\n\tn_reads: {}\n\tn_writes: {}\n\tp_key_repeat: {}".format(
            self.experiment_name, self.n_reads, self.n_writes, self.p_key_repeat
        )

    def _run(self, experiment_func, repeat):
        for i in range(repeat):
            print('run: {}'.format(i))
            self._current_system.start()
            write_latencies, read_latencies = experiment_func()
            self._current_system.shutdown()
            self._reset_after_run()
            yield write_latencies, read_latencies

    def run(self, experiment_func, repeat=1):
        print("Start experiment")

        for system in self.systems:
            print(system.name)
            self._current_system = system
            mean_w_lats, mean_r_lats = [], []

            for write_latencies, read_latencies in self._run(experiment_func, repeat):
                mean_w_lats.append(np.mean(write_latencies))
                mean_r_lats.append(np.mean(read_latencies))

            self._current_system = None
            yield {
                "system": system.name,
                "mean_w_lats": mean_w_lats,
                "mean_r_lats": mean_r_lats
            }

        print("Experiment completed\n\n")

    def _get_key_value_pair(self):
        repeat_key = random.random() <= self.p_key_repeat
        value = len(self.keys)

        if repeat_key and len(self._used_keys) > 0:
            key = random.choice(self._used_keys)
        else:
            key = self._unused_keys.pop()
            self._used_keys.append(key)

        return key, value

    def client_write(self, client_idx):
        key, value = self._get_key_value_pair()

        start = time()
        self._current_system.clients[client_idx].write(key, value)
        end = time()

        return end-start

    def client_read(self, client_idx):
        key = random.choice(self._used_keys)

        start = time()
        self._current_system.clients[client_idx].read(key)
        end = time()

        return end - start
