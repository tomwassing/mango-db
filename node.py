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

