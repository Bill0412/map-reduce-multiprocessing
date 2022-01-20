import pickle

d = {
    'Slave1': ['mauris', 'dapibus'],
    'Slave2': ['mauris', 'odio'],
    'Slave3': ['odio', 'dapibus'],
    'Slave4': ['mauris', 'dapibus', 'odio'],
    'Slave5': ['mauris'],
    'Slave6': ['dapibus'],
    'Slave7': ['odio'],
    'Slave8': ['mauris', 'tellus'],
    'Slave9': ['mauris', 'dapibus']
}

if __name__ == '__main__':
    with open('input/req_words.pkl', 'wb') as outfile:
        pickle.dump(d, outfile)
