from uuid import uuid4
import random
from system import System
from time import time


class Experiment(System):

    def __init__(self, experiment_name, num_nodes, num_clients, port, n_keys, p_key_repeat):

        self.experiment_name = experiment_name
        self.n_keys = n_keys
        self.keys = [str(uuid4()) for _ in range(n_keys)]
        self._used_keys = []
        self.p_key_repeat = p_key_repeat

        # this could be helpful for functional test to verify the content
        # self.data = {key: 0 for key in self.keys}

        super().__init__(num_nodes, num_clients, port)

    def _reset(self):
        self._used_keys = []

    def run(self, function, repeat=1):
        print("Running: {}".format(self.experiment_name))

        for i in range(repeat):
            print('run: {}'.format(i))
            self.start_system()
            write_latencies, read_latencies = function()
            self._reset()
            self.shutdown_system()
            yield write_latencies, read_latencies

    def _get_key_value_pair(self):
        repeat_key = random.random() <= self.p_key_repeat
        value = random.randint(0, self.n_keys)

        if repeat_key and len(self._used_keys) > 0:
            key = random.choice(self._used_keys)
        else:
            key = self.keys.pop()
            self._used_keys.append(key)

        return key, value

    def client_write(self, client_idx):
        key, value = self._get_key_value_pair()

        start = time()
        self.clients[client_idx].write(key, value)
        end = time()

        return end-start

    def client_read(self, client_idx):
        key = random.choice(self.keys)

        start = time()
        self.clients[client_idx].read(key)
        end = time()

        return end - start