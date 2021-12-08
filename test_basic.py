import threading
import random
import pytest
from follower import Follower
from leader import Leader
from client import Client


def setup(num_nodes, num_clients, start_port=25000):
    node_ports = list(range(start_port, start_port + num_nodes + num_clients))
    nodes = [Follower(port, [p for p in node_ports if p != port], node_ports[-1]) for port in node_ports[:-1]]
    leader = Leader(node_ports[-1], node_ports[:-1], node_ports[-1])
    processes = [threading.Thread(target=node.run) for node in [leader, *nodes]]
    clients = [Client(node_ports) for _ in range(num_clients)]

    for process in processes:
        process.start()

    return node_ports, nodes, leader, clients, processes

class TestSimpleTest:
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
        read_value = client.read('World!')["value"]
        order_index = client.read('World!')["order_index"]

        assert read_value == 'Hello?' and order_index == 0

    def test_read_after_five_writes(self):
        client = self.clients[0]
        client2 = self.clients[1]
        client.write("World!", 'Hello1?')
        client2.write("World!", 'Hello2?')
        client.write("World!", 'Hello3?')
        client2.write("World!", 'Hello4?')
        client.write("World!", 'Hello5?')

        read_value = client.read('World!')["value"]
        order_index = client.read('World!')["order_index"]

        assert read_value == 'Hello5?' and order_index == 4

    def test_multi_sync(self):
        client = self.clients[0]
        for i in range(100):
            client.write("World!", f"Hello{i}?")

        read_value = client.read('World!')["value"]
        order_index = client.read('World!')["order_index"]

        assert read_value == 'Hello99?' and order_index == 99

    def test_write_read_different_client(self):
        write_client, read_client = random.sample(self.clients, 2)
        write_client.write("World!", 'Hello')

        assert read_client.read('World!')["value"] == 'Hello'

class TestDurability:

    def setup_method(self, method):
        node_ports, nodes, leader, clients, processes = setup(5, 2)
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

    @pytest.mark.parametrize('execution_number', range(10))
    def test_fe1(self, execution_number):
        values = []
        client = self.clients[0]
        client.write("World!", 'Hello?')
        for port in self.node_ports:
            values.append(client.read('World!', port=port)["value"])

        assert len(set(values)) == 1

    @pytest.mark.parametrize('execution_number', range(10))
    def test_fe2(self, execution_number):
        values = []
        client = self.clients[0]
        client.write("World!", 'Hello?')
        for port in self.node_ports:
            values.append(client.read('World!', port=port)["value"])

        if len(set(values)) == 1:
            client.write("World!", 'Bye!')

            values = []
            for port in self.node_ports:
                values.append(client.read('World!', port=port)["value"])

            assert len(set(values)) == 1 and list(set(values))[0] == 'Bye!'

    @pytest.mark.parametrize('execution_number', range(10))
    def test_fe3(self, execution_number):
        values = []
        client = self.clients[0]
        read_client = self.clients[1]
        for i in range(100):
            client.write("World!", f"Hello{i}?", blocking=False)

        for i in range(100):
            tmp = client.write_recv()

        for port in self.node_ports:
            values.append(read_client.read('World!', port=port)["value"])

        assert len(set(values)) == 1

class TestConsistency:

    def setup_method(self, method):
        node_ports, nodes, leader, clients, processes = setup(5, 4)
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

    @pytest.mark.parametrize('execution_number', range(10))
    def test_multi_async_single_client(self, execution_number):
        client = self.clients[0]
        for i in range(100):
            client.write("World!", f"Hello{i}?", blocking=False)

        for i in range(100):
            client.write_recv()

        assert client.read('World!')["order_index"] ==  99

    @pytest.mark.parametrize('execution_number', range(10))
    def test_multi_async_multi_client(self, execution_number):
        clients = [self.clients[x] for x in range(4)]
        for i in range(100):
            client = clients[i%4]
            client.write("World!", f"Hello{i}?", blocking=False)

        for i in range(100):
            client = clients[i%4]
            client.write_recv()

        assert self.clients[random.choice([x for x in range(3)])].read('World!')["order_index"] ==  99