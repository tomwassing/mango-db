"""
leader.py

Description:
    Contains definitions for the leader node, which inherits from the follower
    class in follower.py.
    It overloads a few of the follower functions because it is in charge of the ordering.
"""

import logging
from follower import Follower


class Leader(Follower):
    def __init__(self, port, node_ports, leader_port, order_on_write=False):
        super().__init__(port, node_ports, leader_port, order_on_write=order_on_write)

    # Send write acck to client and stores data
    def send_client_write_ack(self, msg_id):
        keys, values, client_addr = self.write_buffer[msg_id]
        # Stores data since it takes care of the ordering
        self.store_data(msg_id, keys, values, client_addr)

    # If the write is acknowledged by all nodes ordering can be taken care of
    def handle_client_write_ack(self, addr, data):
        key, value, client_addr = self.write_buffer[data["id"]]
        self.store_data(data["id"], key, value, client_addr)

    # Exdends the on_message function with the client_write_ack message type
    def on_message(self, addr, data):
        super().on_message(addr, data)
        if data["type"] == "client_write_ack":
            self.handle_client_write_ack(addr, data)

    # Stores the key-value pair(s) and takes care of the ordering
    def store_data(self, msg_id, keys, values, client_addr):
        for i in range(len(keys)):
            self.data[keys[i]] = (values[i], self.order_index)
        del self.write_buffer[msg_id]

        logging.info("{}: saved '{} = {}'".format(self, keys, values))

        # Send order_index along with msg_id to all nodes
        data = {
            "type": "write_order",
            "id": msg_id,
            "index": self.order_index
        }

        self.send_to_all(data)
        self.order_index += 1

        if self.order_on_write and client_addr:
            self.send_write_result(client_addr, keys, values)

    # Allows you to print info about leader node as a string
    def __str__(self) -> str:
        return "Leader:{}:{}".format(self.host[0], self.host[1])
