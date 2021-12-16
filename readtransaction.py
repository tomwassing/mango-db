"""
readtransaction.py

Description:
    This class keeps track of keys for which a write
    operation is still pending when a client wants
    to perform a read operation.
    It also contains helper functions to easily store
    (pending) writes and return the values corresponding
    to those keys once no more keys are pending.
"""

class ReadTransaction():
    def __init__(self, addr):
        self.n_keys = 0
        self.n_pending = 0
        self.keys = []
        self.values = {}
        self.write_orders = {}
        self.addr = addr
        self.pending = []

    # Adds a key to the list of pending keys
    def add_pending(self, key):
        self.pending.append(key)
        self.n_pending += 1
        self.keys.append(key)

    # Adds a key-value pair, unless it's not pending
    def add_pair(self, key, value, write_order, pending=False):
        self.n_keys += 1
        self.values[key] = value
        self.write_orders[key] = write_order
        if pending:
            self.n_pending -= 1
        else:
            self.keys.append(key)

        return not self.n_pending

    # Returns the key-value pairs and their corresponding write order
    def return_data(self):
        return_values = []
        return_write_orders = []
        if self.n_keys == 1:
            return_values = self.values[self.keys[0]]
            return_write_orders = self.write_orders[self.keys[0]]
        else:
            for k in self.keys:
                return_values.append(self.values[k])
                return_write_orders.append(self.write_orders[k])

        # Data used for read_result message
        data = {
            "type": "read_result",
            "key": self.keys,
            "value": return_values,
            "order_index": return_write_orders
        }

        return data
