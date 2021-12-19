'''
Test the functional requirements of MangoDB. These include:
   - Tests of basic functionalities (read, write)
   - Durability tests (FE1, FE2, FE3)
   - Consistency tests (FE4, FE5)
Tests in pytest are executed ten times to account for randomness.
The randomness is derived from the fact that we chose random follower nodes.

Please run with `pytest -v`
'''

import threading
import random
import pytest
from follower import Follower
from leader import Leader
from client import Client


def setup(num_nodes, num_clients, start_port=25000, delayed=False):
    '''
    Create the nodes, clients and threads necessary to run MangoDB.
    '''
    node_ports = list(range(start_port, start_port + num_nodes))
    node_hosts = [("127.0.0.1", port) for port in node_ports]
    nodes = [Follower(("127.0.0.1", port), [h for h in node_hosts if h[1] != port], node_hosts[-1]) for port in node_ports[:-1]]
    leader = Leader(node_hosts[-1], node_hosts[:-1], node_hosts[-1])
    threads = [threading.Thread(target=node.run) for node in [leader, *nodes]]
    clients = [Client(node_hosts) for _ in range(num_clients)]

    threads = []
    for i, node in enumerate([leader, *nodes]):
        if i == 1 and delayed:
            threads.append(threading.Thread(target=node.run_delayed))
        else:
            threads.append(threading.Thread(target=node.run))

    clients = [Client(node_hosts) for _ in range(num_clients)]

    for thread in threads:
        thread.start()

    return node_hosts, nodes, leader, clients, threads

class TestSimpleTest:
    '''
    Class that contains the simple tests that test the basic functionalities of MangoDB.
    (multiple write/read tests)
    '''
    def setup_method(self, method):
        '''
        Create 3 follower nodes, a leader and 5 clients.
        '''
        _, nodes, leader, clients, threads = setup(3, 5)
        self.nodes = nodes
        self.leader = leader
        self.clients = clients
        self.threads = threads

    def teardown_method(self):
        '''
        Safely close all connected clients and running threads.
        '''
        for client in self.clients:
            client.exit()
        for thread in self.threads:
            thread.join()

    @pytest.mark.parametrize('execution_number', range(10))
    def test_read_after_write(self, execution_number):
        '''
        Write values to multiple keys and attempt to retrieve a value.
        '''
        client = self.clients[0]
        client.write(["World!", "keyTest"], ['Hello?', "valueTest"])
        read_value = client.read('World!')["value"]
        order_index = client.read('World!')["order_index"]

        assert read_value == 'Hello?' and order_index == 1

    @pytest.mark.parametrize('execution_number', range(10))
    def test_read_after_write_2(self, execution_number):
        '''
        Write values to multiple keys and attempt to retrieve a value.
        Now look at the value of the second key that was added.
        '''
        client = self.clients[0]
        client.write(["World!", "keyTest"], ['Hello?', "valueTest"])
        read_value = client.read('keyTest')["value"]
        order_index = client.read('keyTest')["order_index"]
        print(read_value, order_index)
        assert read_value == "valueTest" and order_index == 1

    @pytest.mark.parametrize('execution_number', range(10))
    def test_read_after_five_writes(self, execution_number):
        '''
        Read the value of the `world!` key after its value has been changed a few times.
        '''
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

    @pytest.mark.parametrize('execution_number', range(10))
    def test_multiple_reads(self, execution_number):
        '''
        Test a transaction filled with only reads
        '''
        client = self.clients[0]
        client2 = self.clients[1]
        client.write("World1!", 'Hello1?')
        client2.write("World2!", 'Hello2?')
        client.write("World3!", 'Hello3?')
        client2.write("World4!", 'Hello4?')
        client.write("World5!", 'Hello5?')

        read_value = client.read(["World1!", "World2!", "World3!","World4!", "World5!"])["value"]
        order_index = client.read(["World1!", "World2!", "World3!","World4!", "World5!"])["order_index"]
        assert read_value == ['Hello1?', 'Hello2?', 'Hello3?', 'Hello4?','Hello5?'] and order_index == [0,1,2,3,4]

    @pytest.mark.parametrize('execution_number', range(10))
    def test_multi_sync(self, execution_number):
        '''
        Extention of the previous test. We change the value of a single key 100 times and see if we can retrieve the
        correct value.
        '''
        client = self.clients[0]
        for i in range(100):
            client.write("World!", "Hello{}?".format(i))

        read_value = client.read('World!')["value"]
        order_index = client.read('World!')["order_index"]

        assert read_value == 'Hello99?' and order_index == 99

    @pytest.mark.parametrize('execution_number', range(10))
    def test_write_read_different_client(self, execution_number):
        '''
        Check if client2 can read a value that client1 has put into the system.
        '''
        write_client, read_client = random.sample(self.clients, 2)
        write_client.write("World!", 'Hello')

        assert read_client.read('World!')["value"] == 'Hello'


class TestDurability:
    '''
    Durability tests for the functional experiments, described in FE1, FE2 and FE3.
    '''
    def setup_method(self, method):
        '''
        Create 5 follower nodes, a leader and 2 clients.
        '''
        node_hosts, nodes, leader, clients, threads = setup(5, 2)
        self.node_hosts = node_hosts
        self.nodes = nodes
        self.leader = leader
        self.clients = clients
        self.threads = threads

    def teardown_method(self):
        '''
        Safely close all connected clients and running threads.
        '''
        for client in self.clients:
            client.exit()
        for thread in self.threads:
            thread.join()

    @pytest.mark.parametrize('execution_number', range(10))
    def test_fe1(self, execution_number):
        '''
        Check if all follower nodes present in the system have the same value for a given key (replication).
        '''
        values = []
        client = self.clients[0]
        client.write("World!", 'Hello?')
        for host in self.node_hosts:
            values.append(client.read('World!', host=host)["value"])

        assert len(set(values)) == 1

    @pytest.mark.parametrize('execution_number', range(10))
    def test_fe2(self, execution_number):
        '''
        Check if this value also can be updated and is it then also propagated amongs te nodes correctly.
        '''
        values = []
        client = self.clients[0]
        client.write("World!", 'Hello?')
        for host in self.node_hosts:
            values.append(client.read('World!', host=host)["value"])

        if len(set(values)) == 1:
            client.write("World!", 'Bye!')

            values = []
            for host in self.node_hosts:
                values.append(client.read('World!', host=host)["value"])

            assert len(set(values)) == 1 and list(set(values))[0] == 'Bye!'

    @pytest.mark.parametrize('execution_number', range(10))
    def test_fe3(self, execution_number):
        '''
        Asyn send 100 write operations and check if the final value is all the same among all participating nodes.
        '''
        values = []
        client = self.clients[0]
        read_client = self.clients[1]
        for i in range(100):
            client.write("World!", "Hello{}?".format(i), blocking=False)

        for i in range(100):
            tmp = client.write_recv()

        for host in self.node_hosts:
            values.append(read_client.read('World!', host=host)["value"])

        assert len(set(values)) == 1


class TestConsistency:
    '''
    Consistency tests for the functional experiments, described in FE4, FE5.
    '''
    def setup_method(self, method):
        '''
        Create 5 follower nodes, a leader and 4 clients.
        '''
        node_hosts, nodes, leader, clients, threads = setup(5, 4)
        self.node_hosts = node_hosts
        self.nodes = nodes
        self.leader = leader
        self.clients = clients
        self.threads = threads

    def teardown_method(self):
        '''
        Safely close all connected clients and running threads.
        '''
        for client in self.clients:
            client.exit()
        for thread in self.threads:
            thread.join()

    @pytest.mark.parametrize('execution_number', range(10))
    def test_multi_async_single_client(self, execution_number):
        '''
        Check if the last value in a set of async write operations is shared among all nodes.
        '''
        values = []
        client = self.clients[0]
        for i in range(100):
            client.write("World!", "Hello{}?".format(i), blocking=False)

        for i in range(100):
            client.write_recv()

        for host in self.node_hosts:
            values.append((client.read('World!', host=host)["value"], client.read('World!', host=host)["order_index"]))

        value_set = set(values)
        length = len(value_set)
        if length == 1:
            order_index = value_set.pop()

            assert order_index[1] == 99

    @pytest.mark.parametrize('execution_number', range(10))
    def test_multi_async_multi_client(self, execution_number):
        '''
        Check if the last value in a set of async write operations is shared among all nodes.
        But now extended that multiple clients execute these read and write operations.
        '''
        values = []
        clients = [self.clients[x] for x in range(4)]
        for i in range(100):
            client = clients[i%4]
            client.write("World!", "Hello{}?".format(i), blocking=False)

        for i in range(100):
            client = clients[i%4]
            client.write_recv()

        for host in self.node_hosts:
            values.append((client.read('World!', host=host)["value"], client.read('World!', host=host)["order_index"]))

        value_set = set(values)
        length = len(value_set)
        if length == 1:
            order_index = value_set.pop()

            assert order_index[1] == 99


class TestConsistency_delay:
    '''
    Consistency tests for the functional experiments, described in FE6.
    '''
    def setup_method(self, method):
        '''
        Create 5 follower nodes, a leader and 4 clients.
        Delayed is now enabled.
        '''
        node_hosts, nodes, leader, clients, threads = setup(5, 4, delayed=True)
        self.node_hosts = node_hosts
        self.nodes = nodes
        self.leader = leader
        self.clients = clients
        self.threads = threads

    def teardown_method(self):
        '''
        Safely close all connected clients and running threads.
        '''
        for client in self.clients:
            client.exit()
        for thread in self.threads:
            thread.join()

    @pytest.mark.parametrize('execution_number', range(10))
    def test_out_of_order(self, execution_number):
        '''
        Try to add 5 values to a single key, but delay value `Hello2?` such that this value is now the last
        value to arrive.
        '''
        values = []
        client = self.clients[0]
        print(self.node_hosts)
        for i in range(5):
            if i == 2:
                client.write("World!", f"Hello{i}?", host=self.node_hosts[0], blocking=False)
            else:
                client.write("World!", f"Hello{i}?", host=self.node_hosts[1], blocking=False)
        for i in range(5):
            client.write_recv()

        for host in self.node_hosts:
            values.append((client.read('World!', host=host)["value"], client.read('World!', host=host)["order_index"]))

        value_set = set(values)
        length = len(value_set)
        if length == 1:
            order_index = value_set.pop()
            assert order_index[1] == 4 and order_index[0] == 'Hello2?'
