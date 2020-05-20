from web3 import Web3
import codecs
from time import sleep

from Crypto.Cipher import AES

infura_url = "https://ropsten.infura.io/v3/be563e5435e04e1a9995b49c4b939ddc"
w3 = Web3(Web3.HTTPProvider(infura_url))

class EthereumHandler:
    def __init__(self, load=True):
        print("Ethereum network connected: ", w3.isConnected())
        print("Current block #:", w3.eth.blockNumber)
        self.address, self.private_key = self.load_account()
        self.MAX_NOTE_LENGTH = 128


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
        tx_sent = False
        while (tx_sent == False):
            try:
                sent_tx = w3.eth.sendRawTransaction(signed_txn.rawTransaction)
                tx_sent = True
            except(ValueError):
                print("Too fast. Sleeping...")
                sleep(20)
        tx_data = self.await_transaction_receipt(sent_tx)
        hash = tx_data['transactionHash']
        print("Message successfully sent. Hash: ", hash.hex())
        return hash.hex(), tx_data['gasUsed']

    def await_transaction_receipt(self, tx, i=0):
        try:
            receipt = w3.eth.getTransactionReceipt(tx)
            return receipt
        except Exception as e:
            if (i > 120):
                raise Exception("Transaction was not confirmed after time limit.")
            sleep(1)
            if (i % 20 == 0):
                print("Waited " + str(i) + " seconds for tx to confirm...")
                print(e)
            return self.await_transaction_receipt(tx, i+1)

    def get_decoded_msg(self, hash):
        tx_data = w3.eth.getTransaction(hash)

        input = tx_data['input']
        if (input[:2] != '0x'):
            raise Exception("Invalid hex string found in transaction")

        decoded_data = codecs.decode(input[2:], "hex").decode('utf-8')
        return decoded_data
