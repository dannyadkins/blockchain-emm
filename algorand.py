from algosdk import account, encoding, algod
from algosdk.future import transaction

import base64
from time import sleep

token = ''

api_address = 'https://testnet-algorand.api.purestake.io/ps1'

headers = {
   "X-API-Key": "4WWa6pMEVt9mXEYVrNlQR3OMlNYqr6p36F9USInI",
}

class PaymentTxnContainer:
    def __init__(self, fee=1, first=1, last=1, gen=None, gh='', flat_fee=1):
        self.fee = fee
        self.first = first
        self.last = last
        self.gen = gen
        self.gh = gh
        self.flat_fee = flat_fee

class AlgorandHandler:
    def __init__(self, load=True, verbose=False):
        self.verbose = verbose
        self.address, self.private_key = self.load_account()
        self.client = algod.AlgodClient(token, api_address, headers)
        self.MAX_NOTE_LENGTH = 1024

    def load_account(self):
        # generate an account
        try:
            account_file = open(".algo-account", "r+")
            print("Loading account from file.")
            lines = account_file.readlines()
            private_key = lines[0].strip()
            address = lines[1].strip()
        except IOError:
            print("No account found. Creating new account.")
            account_file = open(".algo-account", "w+")
            private_key, address = account.generate_account()
            account_file.write(private_key)
            account_file.write('\n')
            account_file.write(address)
            account_file.close()

        print("Address:", address)
        return address, private_key

    def send_msg_on_blockchain(self, msg):
        params = self.client.suggested_params()
        gen = params["genesisID"]
        gh = params["genesishashb64"]
        first_valid_round = params["lastRound"]
        last_valid_round = first_valid_round + 1000
        fee = params["fee"]

        send_amount = 1
        note = msg.encode()
        sender = self.address
        receiver = self.address

        payment_container = PaymentTxnContainer(fee=fee, first=first_valid_round, last=last_valid_round, flat_fee=True, gen=gen, gh=gh)

        tx = transaction.PaymentTxn(sender, payment_container, receiver, send_amount, note=note)
        stx = tx.sign(self.private_key)

        txid = self.client.send_transaction(stx,  headers={'content-type': 'application/x-binary'})

        confirmation = self.wait_for_confirmation(txid)
        confirmed_tx = self.client.transaction_info(self.address, txid)
        if(self.verbose):
            print("Message successfully sent. TxID: ", txid)
        return txid, confirmed_tx["fee"]

    def get_decoded_msg(self, txid):
        tx = self.client.transaction_info(self.address, txid)
        return base64.b64decode(tx.get('noteb64')).decode()

    def wait_for_confirmation(self, txid ):
        while True:
            txinfo = self.client.pending_transaction_info(txid)
            if txinfo.get('round') and txinfo.get('round') > 0:
                if (self.verbose):
                    print("Transaction {} confirmed in round {}.".format(txid, txinfo.get('round')))
                status = True
                break
            else:
                if (self.verbose):
                    print("Waiting for confirmation for " + str(txid) + "...")
                status = self.client.status_after_block(self.client.status().get('lastRound') +1)
        return status
