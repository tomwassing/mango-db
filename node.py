import threading
import logging
import json
import random
import signal
import socket
import time


class Node:
    def __init__(self, port, node_hosts, leader_port):
        self.port = port
        self.node_hosts = node_hosts
        self.leader = leader_port
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind(("", port))
        self.is_connected = True

        logging.info(f"{self} listining on port {self.port}")

    def run(self):
        while self.is_connected:
            data, addr = self.socket.recvfrom(1024)
            message = json.loads(data.decode())
            logging.debug(f"{self}, received message: {message} from {addr}")
            self.on_message(addr, message)

    def run_delayed(self):
        while self.is_connected:
            time.sleep(.05)
            data, addr = self.socket.recvfrom(1024)
            message = json.loads(data.decode())
            logging.debug(f"{self}, received message: {message} from {addr}")
            self.on_message(addr, message)

    def send_to_all(self, data):
        for host in self.node_hosts:
            self.send(host, data)

    def send(self, addr, message):
        logging.debug(f"{self}, sent message: {message} to {addr}")
        self.socket.sendto(json.dumps(message).encode(), addr)

    def on_message(self, addr, message):
        pass

    def __str__(self) -> str:
        return f"Node:{self.port}"

    def __repr__(self) -> str:
        return self.__str__()