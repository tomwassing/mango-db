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
    node_hosts = [("127.0.0.1", port) for port in node_ports]
    nodes = [Follower(("127.0.0.1", port), [h for h in node_hosts if h[1] != port], node_hosts[-1]) for port in node_ports[:-1]]
    leader = Leader(node_hosts[-1], node_hosts[:-1], node_hosts[-1])
    threads = [Thread(target=node.run) for node in [leader, *nodes]]

    for thread in threads:
        thread.start()

    client = Client(node_hosts)
    client.write("World!", 'Hello1?')
    client.write("World!", 'Hello2?')
    client.write("World!", 'Hello3?')
    client.write("World!", 'Hello4?')
    client.write("World!", 'Hello5?')
    client.read("World!")
    client.exit()


if __name__ == '__main__':
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    main(3)
