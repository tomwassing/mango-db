import threading
from follower import Follower
from leader import Leader
from client import Client
import multiprocessing

def setup(num_nodes, start_port=25000):
    node_ports = list(range(start_port, start_port + num_nodes))
    nodes = [Follower(port, [p for p in node_ports if p != port], node_ports[-1]) for port in node_ports[:-1]]
    leader = Leader(node_ports[-1], node_ports[:-1], node_ports[-1])
    processes = [threading.Thread(target=node.run) for node in [leader, *nodes]]
    client = Client(node_ports)
    
    for process in processes:
        process.start()

    return nodes, leader, client, processes

class TestBasic:

    def setup_method(self, method):
        nodes, leader, client, processes = setup(3)
        self.nodes = nodes
        self.leader = leader
        self.client = client
        self.processes = processes

    def teardown_method(self, method):
        self.client.exit()
        for process in self.processes:
            process.join()

    def test_read_after_write(self):
        self.client.write("World!", 'Hello?')

        assert self.client.read('World!')["value"] == 'Hello?'

    def test_read_after_five_writes(self):
        self.client.write("World!", 'Hello1?')
        self.client.write("World!", 'Hello2?')
        self.client.write("World!", 'Hello3?')
        self.client.write("World!", 'Hello4?')
        self.client.write("World!", 'Hello5?')

        assert self.client.read('World!')["value"] == 'Hello5?'
