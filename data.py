"""
data.py

Description:
    This file contains the defnition of the PendingElement class.
    The PendingElement contains a key-value pair along with
    the message id, list of nodes who acknowledged it and the client address.
    It is used to store writes in the acknowledgement buffer until all
    nodes in the system have acknoledged it.
"""

class PendingElement:
    def __init__(self, keys, values, msg_id, client_addr):
        self.keys = keys
        self.values = values
        self.msg_id = msg_id
        self.acknowledged = set()
        self.client_addr = client_addr

    # Adds node_port to set of nodes who acknowledged the write
    def acknowledge(self, node_port):
        self.acknowledged.add(node_port)

    # Checks whether all nodes have acknowledged the write
    def is_complete(self, number_of_nodes):
        return len(self.acknowledged) == number_of_nodes

