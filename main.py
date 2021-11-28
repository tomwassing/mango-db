from client import Client
from follower import Follower
import signal
import logging
from threading import Thread
def main(num_nodes, start_port=25000):
    logging.basicConfig(format='%(asctime)s - %(levelname)s: %(message)s',
                        level=logging.DEBUG,datefmt='%d-%m-%y %H:%M:%S')

    node_ports = list(range(start_port, start_port + num_nodes))
    nodes = [Follower(port, [p for p in node_ports if p != port]) for port in node_ports]
    threads = [Thread(target=node.run) for node in nodes]

    for thread in threads:
        thread.start()

    client = Client(node_ports)
    client.write("Hello", "World")


if __name__ == '__main__':
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    main(4)
