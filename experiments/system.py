import sys 
sys.path.append('..')
from leader import Leader
from follower import Follower
from client import Client

from threading import Thread


class System:

    def __init__(self, name, num_nodes, num_clients, port, order_on_write=False):
        self.name = name
        self.num_nodes = num_nodes
        self.num_clients = num_clients
        self.ports = list(range(port, port + num_nodes))
        self.order_on_write = order_on_write

        self.leader = None
        self.followers = None
        self.clients = None
        self.threads = None

    def start(self):
        self._startup_nodes()
        self._make_clients()

    def shutdown(self):
        for client in self.clients: client.exit()
        for thread in self.threads: thread.join()

    def _startup_nodes(self):
        self.leader = Leader(self.ports[-1], self.ports[:-1], self.ports[-1], self.order_on_write)
        self.followers = [Follower(port, [p for p in self.ports if p != port], self.ports[-1], self.order_on_write) for port in self.ports[:-1]]

        self.threads = [Thread(target=node.run) for node in [self.leader, *self.followers]]

        for thread in self.threads:
            thread.start()

    def _make_clients(self):
        self.clients = [Client(self.ports) for _ in range(self.num_clients)]
