from ethereum import EthereumHandler
from algorand import AlgorandHandler
from utils import parse_val_string, save_obj, load_obj
import multiprocessing as mp
from time import sleep
import math

class NaiveBlockchainEMM:
    def __init__(self, blockchain="ethereum", filepath=None):
        self.roots = {}
        self.blockchain = blockchain
        if (blockchain == "algorand"):
            self.handler = AlgorandHandler()
        else:
            self.handler = EthereumHandler()

        if (filepath != None):
            self.load_from_file(filepath)

    def load_from_file(self, filepath):
        self.roots = load_obj(filepath)

    def save_to_file(self, filepath):
        save_obj(self.roots, filepath)

    def init_blockchain_MM(self, map, parallel=True):
        if (parallel):
            stagger_const = 5 / (1 + math.exp(-0.05 * (len(map) - 20)))
            print("Stagger constant: ", stagger_const)

            manager = mp.Manager()
            self.roots = manager.dict(self.roots)
            with mp.Pool(mp.cpu_count()) as pool:
                costs = pool.starmap(self.update, [(key, value, "+", stagger_const*i) for i, (key, value) in enumerate(map.items())])
            self.roots = dict(self.roots)
            return sum(costs)
        else:
            cost = 0
            for key, val in map.items():
                cost += self.update(key, val, "+")
            return cost

    def update(self, key, val, op="+", delay=0):
        sleep(delay)
        if key not in self.roots:
            self.roots[key] = ""
        root = self.roots[key]

        if (type(val) != list):
            val = [val]

        for val_str in val:
            msg = "o=" + root + "?k=" + key + "?v=" + op + val_str
            tx_hash, cost = self.handler.send_msg_on_blockchain(msg)
            root = tx_hash

        self.roots[key] = tx_hash
        return cost

    def query(self, key):
        vals = ''
        root = self.roots[key]
        while root != None:
            decoded_data = self.handler.get_decoded_msg(root)

            decoded_data_dict = {}
            for param in decoded_data.split("?"):
                data = param.split('=')
                decoded_data_dict[data[0]] = data[1]
            assert decoded_data_dict['k'] == key
            vals += decoded_data_dict['v'] + ','
            if (len(decoded_data_dict['o']) == 0):
                root = None
            else:
                root = decoded_data_dict['o']
        return parse_val_string(vals)

class ImprovedNaiveBlockchainEMM:
    def __init__(self, blockchain="ethereum", filepath=None):
        self.roots = {}
        self.blockchain = blockchain
        if (blockchain == "algorand"):
            self.handler = AlgorandHandler()
        else:
            self.handler = EthereumHandler()

        if (filepath != None):
            self.load_from_file(filepath)

    def load_from_file(self, filepath):
        self.roots = load_obj(filepath)

    def save_to_file(self, filepath):
        save_obj(self.roots, filepath)

    def init_blockchain_MM(self, map, parallel=True):
        if (parallel):
            stagger_const = 5 / (1 + math.exp(-0.05 * (len(map) - 20)))
            print("Stagger constant: ", stagger_const)

            manager = mp.Manager()
            self.roots = manager.dict(self.roots)
            with mp.Pool(mp.cpu_count()) as pool:
                costs = pool.starmap(self.update, [(key, value, "+", stagger_const*i) for i, (key, value) in enumerate(map.items())])
            self.roots = dict(self.roots)

            return sum(costs)
        else:
            cost = 0
            for key, val in map.items():
                cost += self.update(key, val, "+")
            return cost

    def update(self, key, val, op="+", delay=0):
        sleep(delay)
        if key not in self.roots:
            self.roots[key] = ""
        root = self.roots[key]

        if (type(val) == list):
            # TODO: parse commas
            # TODO: handle going over byte limit
            val_str = ",".join(op + str(x) for x in val)
        else:
            val_str = val
        msg = "o=" + root + "?k=" + key + "?v=" + val_str

        tx_hash, cost = self.handler.send_msg_on_blockchain(msg)

        self.roots[key] = tx_hash
        return cost

    def query(self, key):
        vals = ''
        root = self.roots[key]
        while root != None:
            decoded_data = self.handler.get_decoded_msg(root)

            decoded_data_dict = {}
            for param in decoded_data.split("?"):
                data = param.split('=')
                decoded_data_dict[data[0]] = data[1]
            assert decoded_data_dict['k'] == key
            vals += decoded_data_dict['v'] + ','
            if (len(decoded_data_dict['o']) == 0):
                root = None
            else:
                root = decoded_data_dict['o']
        return parse_val_string(vals)
