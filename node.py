import threading
import logging
import json
import random
import signal
import socket

'''
TODO: 
    - Shutdown correctly using Ctrl+C
    - READ operation
    - Ordering of messages
'''

BUFFER_SIZE = 1024

class Node:
    def __init__(self, port, ports):
        self.port = port
        self.ports = ports
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.bind(("", port))

        logging.info(f"Node:{port} listining on port {self.port}")

    def run(self):
        while True:
            data, addr = self.socket.recvfrom(BUFFER_SIZE)
            message = json.loads(data.decode())
            logging.debug(f"Node:{self.port}, received message: {message} from {addr}")
            self.on_message(addr, message)

    def send_to_all(self, data):
        for port in self.ports:
            self.send(("127.0.0.1", port), data)

    def send(self, addr, message):
        self.socket.sendto(json.dumps(message).encode(), addr)
        logging.debug(f"Node:{self.port}, sent message: {message} to {addr}")

    def on_message(self, addr, message):
        pass


class PendingElement:
    def __init__(self, key, value, msg_id, client_addr):
        self.key = key
        self.value = value
        self.msg_id = msg_id
        self.acknowledged = set()
        self.client_addr = client_addr
    
    def acknowledge(self, node_port):
        self.acknowledged.add(node_port)

    def is_complete(self, number_of_nodes):
        return len(self.acknowledged) == number_of_nodes


class Client:
    def __init__(self, node_ports):
        self.node_ports = node_ports

        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.bind(("", 0))

    def write(self, key, value):
        data = {
            "type": "client_write",
            "key": key,
            "value": value
        }

        port = random.choice(self.node_ports)
        self.socket.sendto(json.dumps(data).encode(), ("127.0.0.1", port))
        logging.info(f"Client: send message to node:{port} : {data}")
        data, addr = self.socket.recvfrom(BUFFER_SIZE)
        logging.info(f"Client: received message: {data} from {addr}")


class Follower(Node):
    def __init__(self, port, node_ports):
        super().__init__(port, node_ports)
        self.ack_buffer = {}
        self.write_buffer = {}
        self.write_id = 0

    def handle_acknowledgement(self, addr, msg_id):
        data = {
            "type": "acknowledge",
            "id": msg_id,
            "from": self.port,
        }

        self.send(addr, data)

    def write(self, key, value, addr):
        '''Add key-value pair to acknowledge buffer and send write message to
        all the other nodes.'''
        msg_id = f"{self.port}:{self.write_id}"
        self.ack_buffer[msg_id] = PendingElement(key, value, msg_id, addr)
        self.write_id += 1

        data = {
            "type": "write",
            "id": msg_id,
            "key": key,
            "value": value,
            "from": self.port,
        }
        
        self.send_to_all(data)

    def on_message(self, addr, data):
        if data["type"] == "client_write":
            logging.debug(f"Follower:{self.port}: received client_write message: {data} from client")
            self.write(data["key"], data["value"], addr)

        elif data["type"] == "write":
            # Handling incoming write message from other nodes. Ack the message and
            # add to own write buffer.
            logging.debug(f"Follower:{self.port}: received write message: {data} from node:{addr}")
            self.write_buffer[data["id"]] = (data["key"], data["value"])
            self.handle_acknowledgement(addr, data["id"])
        
        elif data["type"] == "acknowledge":
            # Receiving ack message from other nodes, finalize if all ack messages
            # have been received
            msg_id = data["id"]
            logging.debug(f"Follower:{self.port}: received acknowledge message: {data} from node:{addr}")
            self.ack_buffer[msg_id].acknowledge(addr)

            if self.ack_buffer[msg_id].is_complete(len(self.ports)):
                logging.debug(f"Follower:{self.port}: received all acknowledgements for message: {msg_id}")
                self.write_buffer[msg_id] = self.ack_buffer[msg_id]
                del self.ack_buffer[msg_id]

                receiver = self.write_buffer[msg_id].client_addr
                data = {
                    "type": "write_result",
                    "key": self.write_buffer[msg_id].key,
                    "value": self.write_buffer[msg_id].value
                }

                self.send(receiver, data)


def main(num_nodes, start_port=25000):
    logging.basicConfig(format='%(asctime)s - %(levelname)s: %(message)s',
                        level=logging.DEBUG,datefmt='%d-%m-%y %H:%M:%S')

    node_ports = list(range(start_port, start_port + num_nodes))
    nodes = [Follower(port, [p for p in node_ports if p != port]) for port in node_ports]
    threads = [threading.Thread(target=node.run) for node in nodes]

    for thread in threads:
        thread.start()

    client = Client(node_ports)
    client.write("Hello", "World")


if __name__ == '__main__':
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    main(4)
