from NaiveSchemes import NaiveBlockchainEMM, ImprovedNaiveBlockchainEMM
from Scheme1EMM import Scheme1EMM
import time
import csv
import os


def write_results_to_csv(type, blockchain, stage, num_keys, num_vals_per_key, time, cost, file_size):
    with open('results.csv', 'a') as csvfile:
        writer = csv.writer(csvfile, delimiter=',')
        writer.writerow([type, blockchain, stage, num_keys, num_vals_per_key, int(time*100)/100, cost, file_size])

def test_stabilization(type="naive", blockchain="algorand", num_keys = 5, num_vals_per_key = 1):
    if (type == "naive"):
        emm = NaiveBlockchainEMM(blockchain)
    elif (type == "improved"):
        emm = ImprovedNaiveBlockchainEMM(blockchain)
    elif (type == "scheme1"):
        emm = Scheme1EMM(blockchain)
    else:
        print("Options for EMM type are: naive/improved/scheme1")
    mm = {}

    for i in range(0, num_keys):
        vals = []
        for j in range(0, num_vals_per_key):
            vals.append("val" + str(j))
        mm['key' + str(i)] = vals

    start_time = time.time()
    total_cost = emm.init_blockchain_MM(mm)
    stabilization_time = time.time() - start_time
    print("Stabilization time: ", stabilization_time)

    for key, vals in mm.items():
        query_result = emm.query(key)
        try:
            assert query_result == set(vals)
        except:
            print("Test failed. Query result: ", query_result)
            raise

    # file_name = "./saves/" + type + "_" + blockchain[0:3] + "_" + str(num_keys) + "keys_" + str(num_vals_per_key) + "vals.pkl"
    # emm.save_to_file(file_name)
    # file_size = os.stat(file_name).st_size

    write_results_to_csv(type=type, blockchain=blockchain, stage="stabilization", num_keys=num_keys, num_vals_per_key=num_vals_per_key, time=stabilization_time, cost=total_cost, file_size=-1)
    return emm

def test_delete(emm, num_keys = 5, num_vals_per_key = 1):
    mm = {}

    for i in range(0, num_keys):
        vals = []
        for j in range(0, num_vals_per_key):
            vals.append("val" + str(j+1))
        mm['key' + str(i)] = vals



    for key, vals in mm.items():
        init_vals = emm.query(key)
        start_time = time.time()

        emm.update(key, vals, op="-")
        delete_time = time.time() - start_time
        print("Delete time: ", delete_time)
        query_result = emm.query(key)
        try:
            assert query_result == init_vals - set(vals)
        except:
            print("Test failed. Query result: ", query_result)
            raise

    # file_name = "./saves/" + type + "_" + blockchain[0:3] + "_" + str(num_keys) + "keys_" + str(num_vals_per_key) + "vals.pkl"
    # emm.save_to_file(file_name)
    # file_size = os.stat(file_name).st_size

    write_results_to_csv(type=type, blockchain=blockchain, stage="stabilization", num_keys=num_keys, num_vals_per_key=num_vals_per_key, time=delete_time, cost=-1, file_size=-1)
    return emm


if __name__ == '__main__':
    for i in range(0, 32, 4):
        if (i == 0):
            i = 1
        for j in range(4, 8, 4):
            if (j == 0):
                j = 1
            emm = test_stabilization(type="scheme1", blockchain="algorand", num_keys=i, num_vals_per_key=j)
            test_delete(emm, num_keys=i, num_vals_per_key=1)
