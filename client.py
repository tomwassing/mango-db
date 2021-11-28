import logging
import random
import socket
import json

class Client:
    def __init__(self, node_ports):
        self.node_ports = node_ports

        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.bind(("", 0))

    def send_recv(self, data):
        port = random.choice(self.node_ports)
        self.socket.sendto(json.dumps(data).encode(), ("127.0.0.1", port))
        logging.info(f"Client: send message to node:{port} : {data}")
        data, addr = self.socket.recvfrom(1024)
        # logging.info(f"Client: received message: {data} from {addr}")

    def write(self, key, value):
        data = {
            "type": "client_write",
            "key": key,
            "value": value
        }

        self.send_recv(data)


    def read(self, key):
        data = {
            "type": "client_read",
            "key": key,
        }

        self.send_recv(data)
