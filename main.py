from client import Client
from follower import Follower
import signal
import logging
from threading import Thread

from leader import Leader

'''
TODO: 
    - Shutdown correctly using Ctrl+C
    - READ operation
    - Ordering of messages
'''

def main(num_nodes, start_port=25000):
    logging.basicConfig(format='%(asctime)s.%(msecs)03d - %(levelname)-8s: %(message)s',
                        level=logging.DEBUG,datefmt='%d-%m-%y %H:%M:%S')

    node_ports = list(range(start_port, start_port + num_nodes))
    nodes = [Follower(port, [p for p in node_ports if p != port], node_ports[-1]) for port in node_ports[:-1]]
    leader = Leader(node_ports[-1], node_ports[:-1], node_ports[-1])
    threads = [Thread(target=node.run) for node in [leader, *nodes]]

    for thread in threads:
        thread.start()

    client = Client(node_ports)
    client.write("Hello", "World")
    client.read("Hello")


if __name__ == '__main__':
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    main(3)
