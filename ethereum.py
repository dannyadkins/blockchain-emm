from web3 import Web3
import codecs
infura_url = "https://ropsten.infura.io/v3/be563e5435e04e1a9995b49c4b939ddc"
w3 = Web3(Web3.HTTPProvider(infura_url))
import time
import pickle

class EthereumHandler:
    def __init__(self, load=True):
        print("Ethereum network connected: ", w3.isConnected())
        print("Current block #:", w3.eth.blockNumber)
        self.address, self.private_key = self.load_account()


    def load_account(self):
        try:
            account_file = open(".eth-account", "r+")
            print("Loading Ethereum account from file.")
            lines = account_file.readlines()
            private_key = lines[0].strip()
            address = lines[1].strip()
        except IOError:
            print("No account found. Creating new account.")
            account_file = open(".eth-account", "w+")
            my_account = w3.eth.account.create('bruno')
            private_key = my_account._private_key
            address = my_account._address
            account_file.write(private_key.hex())
            account_file.write('\n')
            account_file.write(address)
            account_file.close()

        return address, private_key

    def send_msg_on_blockchain(self, msg):
        encoded_msg = msg.encode()
        signed_txn = w3.eth.account.signTransaction(dict(
            nonce=w3.eth.getTransactionCount(self.address),
            gasPrice=w3.eth.gasPrice,
            gas=21000 + 8704,
            to=self.address,
            value=12345,
            data=encoded_msg,
          ),
          self.private_key,
        )
        sent_tx = w3.eth.sendRawTransaction(signed_txn.rawTransaction)
        tx_data = self.awaitTransactionReceipt(sent_tx)
        hash = tx_data['transactionHash']
        print("Message successfully sent. Hash: ", hash.hex())
        return hash.hex()

    def awaitTransactionReceipt(self, tx, i=0):
        try:
            receipt = w3.eth.getTransactionReceipt(tx)
            return receipt
        except Exception as e:
            if (i > 120):
                raise Exception("Transaction was not confirmed after time limit.")
            time.sleep(1)
            if (i % 20 == 0):
                print("Waited " + str(i) + " seconds for tx to confirm...")
                print(e)
            return self.awaitTransactionReceipt(tx, i+1)

def parse_val_string(string):
    add_set = set()
    delete_set = set()

    for item in string.rstrip(',').split(','):
        if item[0] == "+":
            add_set.add(item[1:])
        if item[0] == "-":
            delete_set.add(item[1:])

    return add_set - delete_set

def save_obj(obj, name ):
    with open('saves/'+ name + '.pkl', 'wb') as f:
        pickle.dump(obj, f, pickle.HIGHEST_PROTOCOL)

def load_obj(name ):
    with open('saves/' + name + '.pkl', 'rb') as f:
        return pickle.load(f)

class NaiveBlockchainEMM:
    def __init__(self, filepath=None):
        self.roots = {}
        self.handler = EthereumHandler()

        if (filepath != None):
            self.load_from_file(filepath)

    def load_from_file(self, filepath):
        self.roots = load_obj(filepath)

    def save_to_file(self, filepath):
        save_obj(self.roots, filepath)

    def init_blockchain_MM(self, map):
        for key, val in map.items():
            self.update(key, val, "+")

    def update(self, key, val, op="+"):
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

        tx_hash = self.handler.send_msg_on_blockchain(msg)

        self.roots[key] = tx_hash

    def query(self, key):
        vals = ''
        root = self.roots[key]
        while root != None:
            tx_data = w3.eth.getTransaction(root)

            input = tx_data['input']
            if (input[:2] != '0x'):
                raise Exception("Invalid hex string found in transaction")

            decoded_data = codecs.decode(input[2:], "hex").decode('utf-8')

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

def test_naive_EMM_init_and_query():
    naive = NaiveBlockchainEMM()
    val = ['foo1', 'foo2']
    naive.init_blockchain_MM({"bar": val})
    query_result_1 = naive.query('bar')
    assert query_result_1 == val
    naive.add("bar", "foo3")
    print(naive.query('k'))
    print("Passed check: test_naive_EMM_init_and_query")

def test_naive_EMM_save_and_load():
    naive = NaiveBlockchainEMM()
    val = ['foo1', 'foo2']
    naive.init_blockchain_MM({"bar1": val, "bar2": "blah1"})
    roots1 = naive.roots
    naive.save_to_file('naive-roots')
    naive.load_from_file('naive-roots')
    roots2 = naive.roots
    assert roots1 == roots2
    print("Passed check: test_naive_EMM_save_and_load")

def test_naive_EMM_add_and_delete():
    naive = NaiveBlockchainEMM()
    val = ['foo1', 'foo2']
    naive.init_blockchain_MM({"bar": val})
    naive.update("bar", "foo3", "+")
    naive.update("bar", "foo1", "-")
    read_result = naive.query('bar')
    assert read_result == set(val)
    print("Passed check: test_naive_EMM_add_and_delete")

test_naive_EMM_add_and_delete()
