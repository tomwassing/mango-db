
import sys
import zmq
import threading
import logging
import time

class Node:

    def __init__(self, port, node_ports):
        self.port = port
        self.sockets = []

        context = zmq.Context()

        socket = context.socket(zmq.PAIR)
        logging.info(f"Node:{port} binding on port {self.port}...")
        socket.bind(f"tcp://*:{port}")
        socket.get
        self.sockets.append(socket)

        for node_port in node_ports:
            socket = context.socket(zmq.PAIR) 
            logging.info(f"Node:{port} connecting to port {node_port}...")
            socket.connect(f"tcp://localhost:{node_port}")
            self.sockets.append(socket)

    def run(self):
        poller = zmq.Poller()

        for socket in self.sockets:
            poller.register(socket, zmq.POLLIN)

        self.send_to_all(f"Hello from node:{self.port}")

        logging.info(f"Node:{self.port} started on port {self.port}")
        while True:
            socks = dict(poller.poll(100))
            for sock in self.sockets:
                if socks.get(sock) == zmq.POLLIN:
                    result = sock.recv_string()
                    logging.debug(f"Node:{self.port}, received message: {result}")

    def send_to_all(self, message):
        for socket in self.sockets:
            logging.debug(f"Node:{self.port} send message")
            socket.send_string(message)


def main(num_nodes, start_port=5000):
    logging.basicConfig(format='%(asctime)s - %(levelname)s: %(message)s',
        level=logging.INFO,datefmt='%d-%m-%y %H:%M:%S')

    node_ports = list(range(start_port, start_port + num_nodes))
    nodes = [Node(port, [p for p in node_ports if p != port]) for port in node_ports]
    threads = [threading.Thread(target=node.run, daemon=True) for node in nodes]

    time.sleep(2)
    for thread in threads:
        thread.start()

    time.sleep(100)


    # try:
    #     leader.run_leader()
    # except KeyboardInterrupt:
    #     print("\nExiting...")
    #     for thread in threads:
    #         thread.join()
    #     sys.exit(0)


if __name__ == '__main__':
    main(6)
