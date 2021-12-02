import threading
import logging
import json
import random
import signal
import socket


class Node:
    def __init__(self, port, ports, leader_port):
        self.port = port
        self.ports = ports
        self.leader = leader_port
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.bind(("", port))

        logging.info(f"{self} listining on port {self.port}")

    def run(self):
        while True:
            data, addr = self.socket.recvfrom(1024)
            message = json.loads(data.decode())
            logging.debug(f"{self}, received message: {message} from {addr}")
            self.on_message(addr, message)

    def send_to_all(self, data):
        for port in self.ports:
            self.send(("127.0.0.1", port), data)

    def send(self, addr, message):
        logging.debug(f"{self}, sent message: {message} to {addr}")
        self.socket.sendto(json.dumps(message).encode(), addr)

    def on_message(self, addr, message):
        pass

    def __str__(self) -> str:
        return f"Node:{self.port}"

    def __repr__(self) -> str:
        return self.__str__()