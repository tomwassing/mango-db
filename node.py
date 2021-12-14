"""
node.py

Description:
    This file contains the definition of the node class.
    It provides the basic functionality for each node in the system.
    This functionality includes running receiving messages, handeling messages and
    sending messages.
"""

import threading
import logging
import json
import random
import signal
import socket
import time


class Node:
    def __init__(self, host, node_hosts, leader_port):
        self.host = host
        self.port = host[1]
        self.node_hosts = node_hosts
        self.leader = leader_port
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind(("", self.port))
        self.is_connected = True

        logging.info("{} listining on port {}".format(self, self.port))

    # Waits for incomming messages and calls on_message function to handle it
    def run(self):
        while self.is_connected:
            data, addr = self.socket.recvfrom(1024)
            message = json.loads(data.decode())
            logging.debug("{}, received message: {} from {}".format(self, message, addr))
            self.on_message(addr, message)

    # The same as run, only with a artificual delay for testing
    def run_delayed(self):
        while self.is_connected:
            time.sleep(.05)
            data, addr = self.socket.recvfrom(1024)
            message = json.loads(data.decode())
            logging.debug("{}, received message: {} from {}".format(self, message, addr))
            self.on_message(addr, message)

    # Sends 'data' to all other known hosts
    def send_to_all(self, data):
        for host in self.node_hosts:
            self.send(host, data)

    # Sends a message to a specific host
    def send(self, addr, message):
        logging.debug("{}, sent message: {} to {}".format(self, message, addr))
        self.socket.sendto(json.dumps(message).encode(), addr)

    # This function handles incomming messages and will be overloaded by child classes
    def on_message(self, addr, message):
        pass

    # Allows you to print a node as a string
    def __str__(self) -> str:
        return "Node:{}:{}".format(self.host[0], self.host[1])

    # Allows you to print a node as a string
    def __repr__(self) -> str:
        return self.__str__()