import sys
import zmq
import threading
import logging
import time
import json
import random


'''
TODO: 
    - Shutdown correctly using Ctrl+C
    - READ operation
    - Ordering of messages
'''

class Node:
    def __init__(self, port, node_ports):
        self.port = port
        self.sockets = []

        context = zmq.Context()

        # Start listening for messages from other nodes.
        socket = context.socket(zmq.PAIR)
        logging.info(f"Node:{port} binding on port {self.port}...")
        socket.bind(f"tcp://*:{port}")
        self.sockets.append((port, socket))

        # Connecting to all other nodes.
        for node_port in node_ports:
            socket = context.socket(zmq.PAIR) 
            logging.info(f"Node:{port} connecting to port {node_port}...")
            socket.connect(f"tcp://localhost:{node_port}")
            self.sockets.append((node_port, socket))

    def run(self):
        # Setup polling on all sockets.
        poller = zmq.Poller()
        for _, socket in self.sockets:
            poller.register(socket, zmq.POLLIN)

        logging.info(f"Node:{self.port} started on port {self.port}")
        while True:
            socks = dict(poller.poll(100))
            for sock_port, sock in self.sockets:
                if socks.get(sock) == zmq.POLLIN:
                    result = sock.recv_string()
                    self.on_message(sock_port, sock, json.loads(result))
                    logging.debug(f"Node:{self.port}, received message: {result} from node:{sock_port}")

    def send_to_all(self, data):
        message = json.dumps(data)
        logging.info(f"Node:{self.port} sending message: {message}")
        for node_port, socket in self.sockets:
            if node_port != self.port:
                logging.debug(f"Node:{self.port} send message to node:{node_port}")
                socket.send_string(message)

    def send(self, socket, message):
        socket.send_string(json.dumps(message))

    def on_message(self, port, socket, data):
        pass


class PendingElement:
    def __init__(self, key, value, msg_id, client_socket):
        self.key = key
        self.value = value
        self.msg_id = msg_id
        self.acknowledged = set()
        self.client_socket = client_socket
    
    def acknowledge(self, node_port):
        self.acknowledged.add(node_port)

    def is_complete(self, sockets):
        return len(self.acknowledged) == len(sockets) - 1


class Client:
    def __init__(self, node_ports):
        self.sockets = []

        context = zmq.Context()

        # Connecting to all other nodes.
        for node_port in node_ports:
            socket = context.socket(zmq.PAIR) 
            logging.info(f"Client connecting to port {node_port}...")
            socket.connect(f"tcp://localhost:{node_port}")
            self.sockets.append((node_port, socket))

    def write(self, key, value):
        data = {
            "type": "client_write",
            "key": key,
            "value": value
        }

        _, socket = random.choice(self.sockets)
        socket.send_string(json.dumps(data))
        logging.info(f"Client: send message: {data}")
        result = socket.recv_string()
        logging.info(f"Client: received message: {result}")


class Follower(Node):
    def __init__(self, port, node_ports):
        super().__init__(port, node_ports)
        self.ack_buffer = {}
        self.write_buffer = {}
        self.write_id = 0

    def handle_acknowledgement(self, socket, msg_id):
        data = {
            "type": "acknowledge",
            "id": msg_id
        }

        self.send(socket, data)

    def write(self, key, value, socket):
        '''Add key-value pair to acknowledge buffer and send write message to
        all the other nodes.'''
        msg_id = f"{self.port}:{self.write_id}"
        self.ack_buffer[msg_id] = PendingElement(key, value, msg_id, socket)
        self.write_id += 1

        data = {
            "type": "write",
            "id": msg_id,
            "key": key,
            "value": value
        }
        
        self.send_to_all(data)

    def on_message(self, port, socket, data):
        if data["type"] == "client_write":
            logging.debug(f"Follower:{self.port}: received client_write message: {data} from client")
            self.write(data["key"], data["value"], socket)

        elif data["type"] == "write":
            # Handling incoming write message from other nodes. Ack the message and
            # add to own write buffer.
            logging.debug(f"Follower:{self.port}: received write message: {data} from node:{port}")
            self.write_buffer[data["id"]] = (data["key"], data["value"])
            self.handle_acknowledgement(socket, data["id"])
        
        elif data["type"] == "acknowledge":
            # Receiving ack message from other nodes, finalize if all ack messages
            # have been received
            msg_id = data["id"]
            logging.debug(f"Follower:{self.port}: received acknowledge message: {data} from node:{port}")
            self.ack_buffer[msg_id].acknowledge(socket)

            if self.ack_buffer[msg_id].is_complete(self.sockets):
                logging.debug(f"Follower:{self.port}: received all acknowledgements for message: {msg_id}")
                self.write_buffer[msg_id] = self.ack_buffer[msg_id]
                del self.ack_buffer[msg_id]
                logging.debug(f"Follower:{self.port}: sent write_result message: {self.write_buffer[msg_id]} to client")

                self.write_buffer[msg_id].client_socket.send_string(json.dumps({
                    "type": "write_result",
                    "key": self.write_buffer[msg_id].key,
                    "value": self.write_buffer[msg_id].value
                }))


def main(num_nodes, start_port=5000):
    logging.basicConfig(format='%(asctime)s - %(levelname)s: %(message)s',
                        level=logging.DEBUG,datefmt='%d-%m-%y %H:%M:%S')

    node_ports = list(range(start_port, start_port + num_nodes))
    nodes = [Follower(port, [p for p in node_ports if p != port]) for port in node_ports]
    threads = [threading.Thread(target=node.run, daemon=True) for node in nodes]

    for thread in threads:
        thread.start()

    client = Client(node_ports)
    client.write("Hello", "World")


if __name__ == '__main__':
    main(3)
