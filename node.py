import threading
import logging
import json
import random
import signal
import socket


class Node:
    def __init__(self, port, ports):
        self.port = port
        self.ports = ports
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.bind(("", port))

        logging.info(f"Node:{port} listining on port {self.port}")

    def run(self):
        while True:
            data, addr = self.socket.recvfrom(1024)
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
