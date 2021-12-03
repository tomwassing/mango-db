import threading
from follower import Follower
from leader import Leader
from client import Client
import random
import multiprocessing
import pytest
from unittest.mock import Mock

def setup(num_nodes, num_clients, start_port=25000):
    node_ports = list(range(start_port, start_port + num_nodes + num_clients))
    nodes = [Follower(port, [p for p in node_ports if p != port], node_ports[-1]) for port in node_ports[:-1]]
    leader = Leader(node_ports[-1], node_ports[:-1], node_ports[-1])
    processes = [threading.Thread(target=node.run) for node in [leader, *nodes]]
    clients = [Client(node_ports) for _ in range(num_clients)]

    for process in processes:
        process.start()

    return node_ports, nodes, leader, clients, processes

class TestBasic:

    def setup_method(self, method):
        _, nodes, leader, clients, processes = setup(3, 5)
        self.nodes = nodes
        self.leader = leader
        self.clients = clients
        self.processes = processes

    def teardown_method(self):
        for client in self.clients:
            client.exit()
        for process in self.processes:
            process.join()

    def test_read_after_write(self):
        client = self.clients[0]
        client.write("World!", 'Hello?')

        assert client.read('World!')["value"] == 'Hello?'

    def test_read_after_five_writes(self):
        client = self.clients[0]
        client.write("World!", 'Hello1?')
        client.write("World!", 'Hello2?')
        client.write("World!", 'Hello3?')
        client.write("World!", 'Hello4?')
        client.write("World!", 'Hello5?')

        assert client.read('World!')["value"] == 'Hello5?'

    def test_read_on_all_clients(self):
        write_client = random.choice(self.clients)
        write_client.write("World!", 'Hello')
        for client in self.clients:
            assert client.read('World!')["value"] == 'Hello'

    def test_write_read_different_client(self):
        write_client, read_client = random.sample(self.clients, 2)
        write_client.write("World!", 'Hello')
        assert read_client.read('World!')["value"] == 'Hello'

class TestDurability:
    def setup_method(self, method):
        node_ports, nodes, leader, clients, processes = setup(5, 1)
        self.node_ports = node_ports
        self.nodes = nodes
        self.leader = leader
        self.clients = clients
        self.processes = processes

    def teardown_method(self):
        for client in self.clients:
            client.exit()
        for process in self.processes:
            process.join()

    def test_connection_lost_write(self):
        client = self.clients[0]
        node_number = random.randint(0, len(self.nodes) - 1)
        error_node = self.nodes[node_number]

        print(error_node.port)

        client.write("World!", 'Hello?', port=error_node.port, blocking=False)
        client.exit_single(error_node.port)

        # Wait till thread is done.
        self.processes[node_number + 1].join()

        # after timeout, client sends to different follower.
        client.write_recv("World!", 'Hello?', port=error_node.port)
        new_port = random.choice([x for x in self.node_ports if x != error_node.port])
        print(new_port)
        assert client.read('World!', port=new_port)["value"] == 'Hello?'

        # assert client.read('World!')["value"] == 'Hello?'


