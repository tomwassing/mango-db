"""
readtransaction.py

Description:
    TODO
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

    def add_pending(self, key):
        self.pending.append(key)
        self.n_pending += 1
        self.keys.append(key)


    def add_pair(self, key, value, write_order, pending=False):
        self.n_keys += 1
        self.values[key] = value
        self.write_orders[key] = write_order
        if pending:
            self.n_pending -= 1
        else:
            self.keys.append(key)

        return not self.n_pending

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

        data = {
                "type": "read_result",
                "key": self.keys,
                "value": return_values,
                "order_index": return_write_orders
                }

        return data
