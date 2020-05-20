from binarytree import bst, Node, build
from ethereum import EthereumHandler
from algorand import AlgorandHandler
from utils import parse_val_string, save_obj, load_obj
import multiprocessing as mp
from time import sleep
import math

import json


class Scheme1EMM:
    def __init__(self, blockchain="ethereum", filepath=None):
        self.roots = {}
        self.patch_root_tx_hashes = {}

        self.patches_dict = {} ## the successor -> predecessor dict
        self.patch_tree_root_node = None

        self.node_to_txhash = {}

        self.blockchain = blockchain

        if (blockchain == "algorand"):
            self.handler = AlgorandHandler()
        else:
            self.handler = EthereumHandler()

        if (filepath != None):
            self.load_from_file(filepath)

    def load_from_file(self, roots_file, patches_file):
        self.roots = load_obj(roots_file)
        self.patch_root_tx_hashes = load_obj(patches_file)

    def save_to_file(self, roots_file, patches_file):
        save_obj(self.roots, roots_file)
        save_obj(self.patches, patches_file)


    def load_patch_tree_from_root(self, key):
        if (key in self.patch_root_tx_hashes):
            patch_root_tx_hash = self.patch_root_tx_hashes[key]
            self.patch_tree_root_node = self.load_patch_tree_from_node(patch_root_tx_hash)
        else:
            self.patch_tree_root_node = Node("?s=?p=?l=?r=")
            patch_root_tx_hash, cost = self.handler.send_msg_on_blockchain(str(self.patch_tree_root_node.value.strip()))
            self.patch_root_tx_hashes[key] = patch_root_tx_hash
            self.node_to_txhash[self.node_string(self.patch_tree_root_node)] = patch_root_tx_hash

        return self.patch_tree_root_node


    def load_patch_tree_from_node(self, node_tx_hash):
        if (len(node_tx_hash) == 0):
            return Node("")
        try:
            decoded_data = self.handler.get_decoded_msg(node_tx_hash) ## looks like l=left child address?r=right child address?s=successor?p=predecessor
        except Exception as e:
            print("Error loading ", node_tx_hash)
            raise(e)
        decoded_data_dict = {}
        for param in decoded_data.split("?"):
            data = param.split('=')
            if (len(data) <= 1):
                data.append("")
            decoded_data_dict[data[0]] = data[1]

        pred_tx_hash = decoded_data_dict['p'].strip()
        succ_tx_hash = decoded_data_dict['s'].strip()
        self.patches_dict[succ_tx_hash] = pred_tx_hash

        left_address = decoded_data_dict['l'].strip()
        right_address = decoded_data_dict['r'].strip()

        if (len(left_address) == 0):
            left_child = None
        else:
            left_child = self.load_patch_tree_from_node(left_address)

        if (len(right_address) == 0):
            right_child = None
        else:
            right_child = self.load_patch_tree_from_node(right_address)

        node = Node(self.construct_node_data(succ_tx_hash, pred_tx_hash, left_address, right_address), left=left_child, right=right_child)
        self.node_to_txhash[self.node_string(node)] = node_tx_hash
        return node

    def node_string(self, node):
        return str(node.value).strip()

    def construct_node_data(self, succ_addr, pred_addr, left_addr="", right_addr=""):
        ## TODO: make sure this happens so sorting of strings occurs on succ_addr
        if (succ_addr == None):
            succ_addr = ''
        if (pred_addr == None):
            pred_addr = ''
        if (left_addr == None):
            left_addr = ''
        if (right_addr == None):
            right_addr = ''
        return '?s=' + succ_addr + "?p=" + pred_addr + "?l=" + left_addr + "?r=" + right_addr


    def init_blockchain_MM(self, map, parallel=True):
        if (parallel):
            stagger_const = 5 / (1 + math.exp(-0.05 * (len(map) - 20)))

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
        if (op == "+"):
            sleep(delay)
            if key not in self.roots:
                self.roots[key] = ""
            root = self.roots[key]

            if (type(val) != list):
                val = [val]
            total_cost = 0

            for val_str in val:
                msg = "o=" + root + "?k=" + key + "?v=" + op + val_str
                tx_hash, cost = self.handler.send_msg_on_blockchain(msg)
                total_cost += cost
                root = tx_hash

            self.roots[key] = tx_hash
            return total_cost
        else:
            ## Beginning of step 1
            if (key not in self.roots):
                return 0 ## RAISE EXCEPTION
            root = self.roots[key]
            ## End of step 1

            ## Beginning of step 2
            patch_tree_root_node = self.load_patch_tree_from_root(key)
            ## End of step 2

            ## Beginning of step 3
            if (type(val) != list):
                val = [val]
            P = {}
            C = {}
            addr_list = []
            ## search over chain SKIPPING PATCHED ITEMS. You are not currently doing this. Implement self.query like this first.
            while (root != None and root != ''):
                decoded_data = self.handler.get_decoded_msg(root)
                decoded_data_dict = {}
                for param in decoded_data.split("?"):
                    data = param.split('=')
                    decoded_data_dict[data[0]] = data[1]
                assert decoded_data_dict['k'] == key
                decoded_val = decoded_data_dict['v']
                addr_list.append((decoded_val, root))

                if (len(decoded_data_dict['o']) == 0):
                    root = None
                elif (decoded_data_dict['o'] in self.patches_dict):
                    root = self.patches_dict[decoded_data_dict['o']]
                else:
                    root = decoded_data_dict['o']
            addr_list = [('_', 'HEAD')] + addr_list + [('_', 'TAIL')]
            new_tree = build(list(patch_tree_root_node))

            for v in val:
                for i in range(len(addr_list)):
                    item = addr_list[i]
                    if item[0] == "+"+v:
                        C[item[0]] = item[1]
                        P[item[0]] = (addr_list[i-1][1], addr_list[i+1][1])
            ## End of step 3
            ## C = every tx hash that value v is stored at.
            ## P = the successor and predecessor of v every time it is stored

            ## shallow copy the tree and begin modifying

            ## Beginning of step 4 (value_tx_hash = c)
            ## OUTBOUND PATCHES
            for _, value_tx_hash in C.items():
                if value_tx_hash in self.patches_dict:
                    ## TODO: create patch from c's predecessor to d... how?
                    ## delete and propagate the node containing value_tx_hash -> self.patches_dict[value_tx_hash]
                    pass
            ## End of step 4

            ## Beginning of step 5 (succ_addr, pred_addr = s, p)
            for _, (succ_addr, pred_addr) in P.items():
                if succ_addr in self.patches_dict:
                    ## TODO: replace patch to be succ_addr -> pred_addr
                    ## basically, just copy the old node that was here, replace its data with the new data, and propagate this change up the tree
                    ## propagation up the tree involves: waiting for it to be put on blockchain, and then doing parent

                    ## create new node. replace in tree in proper location.
                    pass
                else:
                    ## TODO: create patch succ_addr -> pred_addr

                    ## create new node, put in tree in proper location. (one index beyond the length of tree, or can traverse to make BST)
                    new_node = Node(self.construct_node_data(succ_addr.strip(), pred_addr.strip (), left_addr="", right_addr="").strip())
                    new_tree[len(list(new_tree))] = new_node
            ## End of step 5

            new_tree_list = list(new_tree)
            for i in range(len(new_tree_list)-1, -1, -1):
                if (self.node_string(new_tree_list[i]) not in self.node_to_txhash):

                    node = new_tree_list[i]
                    tx_hash, cost = self.handler.send_msg_on_blockchain(str(node.value).strip())
                    self.node_to_txhash[self.node_string(node).strip()] = tx_hash
                    new_tree_list[i] = node

                    ## propagate to parent
                    if ((i-1)/2 >= 0):
                        parent = new_tree_list[math.floor((i-1)/2)]
                        if (i % 2 == 0):
                            start_index = str(parent.value).strip().index('?r=')
                            end_index = len(str(parent.value).strip())
                            modified_value = str(parent.value).strip()[0: start_index] + "?r=" + tx_hash + str(parent.value).strip()[end_index:]
                            parent.value = modified_value
                            parent.right = Node(str(node.value).strip())
                        else:
                            start_index = str(parent.value).strip().index('?l=')
                            end_index = str(parent.value).strip().index('?r=')
                            modified_value = str(parent.value).strip()[0:start_index] +   "?l=" + tx_hash + str(parent.value).strip()[end_index:]
                            parent.value = modified_value
                            parent.left =  Node(str(node.value).strip())
                        new_tree_list[math.floor((i-1)/2)] = parent

            if (not node == None):
                self.patch_root_tx_hashes[key] = self.node_to_txhash[self.node_string(new_tree_list[0])]

            ## for each item in new tree (search):
            ## if nodestr(new item) is not in self.node_to_txhash:
            ## put on blockchain, remembering to store the left and right child ADDRESSES
            ## update parent
            ## this can be done recursively

    def query(self, key):
        vals = ''
        root = self.roots[key]

        self.load_patch_tree_from_root(key)
        if ('HEAD' in self.patches_dict):
            root = self.patches_dict['HEAD']
        while root != None and root != '':
            if (root == 'TAIL'):
                break

            decoded_data = self.handler.get_decoded_msg(root)

            decoded_data_dict = {}
            for param in decoded_data.split("?"):
                data = param.split('=')
                decoded_data_dict[data[0]] = data[1]
            assert decoded_data_dict['k'] == key

            vals += decoded_data_dict['v'] + ','
            if (len(decoded_data_dict['o']) == 0):
                root = None
            elif (root in self.patches_dict):
                root = self.patches_dict[root]
            else:
                root = decoded_data_dict['o']
        return parse_val_string(vals)
