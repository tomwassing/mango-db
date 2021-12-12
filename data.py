
class PendingElement:
    def __init__(self, keys, values, msg_id, client_addr):
        self.keys = keys
        self.values = values
        self.msg_id = msg_id
        self.acknowledged = set()
        self.client_addr = client_addr

    def acknowledge(self, node_port):
        self.acknowledged.add(node_port)

    def is_complete(self, number_of_nodes):
        return len(self.acknowledged) == number_of_nodes

