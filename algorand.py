from algosdk import account, encoding
from gen_multimap import *

def load_account():
    # generate an account
    try:
        account_file = open(".account", "r+")
        print("Loading account from file.")
        lines = account_file.readlines()
        private_key = lines[0].strip()
        address = lines[1].strip()
    except IOError:
        print("No account found. Creating new account.")
        account_file = open(".account", "w+")
        private_key, address = account.generate_account()
        account_file.write(private_key)
        account_file.write('\n')
        account_file.write(address)
        account_file.close()

    print("Address:", address)
    return address, private_key

address, private_key = load_account()
mm = gen_multimap()
print(mm)

parse_tree = ''

txn = transaction.PaymentTxn(existing_account, sp, address_1, amount)


for key, val in mm.items():
    print(key)
