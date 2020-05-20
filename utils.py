import os
import pickle

def parse_val_string(string):
    add_set = set()
    delete_set = set()

    for item in string.rstrip(',').split(','):
        if (len(item) > 0):
            if item[0] == "+":
                add_set.add(item[1:])
            if item[0] == "-":
                delete_set.add(item[1:])

    return add_set - delete_set

def save_obj(obj, name ):
    dirname = os.path.dirname(name)
    if not os.path.exists(dirname):
        os.makedirs(dirname)
    with open(name, 'wb') as f:
        pickle.dump(obj, f, pickle.HIGHEST_PROTOCOL)

def load_obj(name ):
    with open('saves/' + name + '.pkl', 'rb') as f:
        return pickle.load(f)
