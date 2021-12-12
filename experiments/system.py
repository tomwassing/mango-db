import sys
import os
import logging
import socket
import time

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
        self.node_hosts = [("127.0.0.1", port) for port in self.ports]

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
        self.followers = [Follower(("127.0.0.1", port), [h for h in self.node_hosts if h[1] != port], self.node_hosts[-1]) for port in self.ports[:-1]]
        self.leader = Leader(self.node_hosts[-1], self.node_hosts[:-1], self.node_hosts[-1])
        self.threads = [Thread(target=node.run) for node in [self.leader, *self.followers]]

        for thread in self.threads:
            thread.start()

    def _make_clients(self):
        self.clients = [Client(self.node_hosts) for _ in range(self.num_clients)]

class DasSystem(System):

    def __init__(self, num_clients, port, order_on_write=False):
        self.hostname = socket.gethostname()
        self.hostnames = os.getenv('HOSTS').split()
        self.port = port

        super().__init__('DAS', len(self.hostnames) - 1, num_clients, port, order_on_write)
        self.node_hosts = list(zip(self.hostnames, self.ports))


    
    def _startup_nodes(self):
        host = (self.hostname, self.port)
        is_client = self.hostname == self. hostnames[0]
        is_leader = self.hostname == self.hostnames[-1]

        logging.info('Starting experiment system on {}'.format(self.hostname))
        logging.info("Found {} hosts: {}".format(len(self.hostnames), self.hostnames))

        host = (self.hostname, self.ports[0])
        if is_leader:
            leader = Leader(host, [(h, self.port) for h in self.hostnames[1:] if h != self.hostname], host)
            leader.run() 
        elif not is_client:
            follower = Follower(host, [(h, self.port) for h in self.hostnames[1:] if h != self.hostname], (self.hostnames[-1], self.port))
            follower.run()

    def shutdown(self):
        for client in self.clients: 
            client.exit()
