import multiprocessing
import pickle
import random
from enum import Enum
import time
import os

import generate_pickle

OUT_PATH = 'output/'

if not os.path.exists(OUT_PATH):
    os.mkdir(OUT_PATH)

# abandon a slave if inactive for 15 seconds
SLAVE_ABANDON_THRESHOLD = 15

class Master:
    def __init__(self, file_list, words_dict, slave_id_list, slave_queues, master_queue) -> None:
        self.file_list = file_list
        self.words_dict = words_dict
        self.slave_id_list = slave_id_list
        self.slave_queues = slave_queues
        self.master_queue = master_queue # {"slave_id": "id", "phase": "map" or "reduce"}

        self.slave_pool = list()

        self.tasks_in_progress = dict()   # slave_id: [file_name, start_time]

        self.file_pool = self.file_list.copy()
        # move from file_pool to files in progress, move back if does not work for 15 secs
        self.files_in_progress = set()
        self.finished_files = list()

        # reduce phase
        self.reduce_words_list = list()
        self.reduce_finished_files = list()
        self.reduce_words_in_progress = set()

        self.reduce_tasks_in_progress = dict()

        self.reduce_words_pool = list()

        self.reduced_file_list = list()

    def run(self):
        print("master started")
        # map
        while len(self.finished_files) != len(self.file_list):
            # add the finished ones
            while not self.master_queue.empty():
                d = self.master_queue.get()
                if d["phase"] == "reduce":
                    continue
                elif d["phase"] == "start":
                    self.slave_pool.append(d["slave_id"])
                    continue
                slave_id = d["slave_id"]
                file_name = self.tasks_in_progress[slave_id][0]
                self.finished_files.append(file_name)
                print(type(self.files_in_progress), print(file_name))
                self.files_in_progress.remove(file_name)
                del self.tasks_in_progress[slave_id]
                self.slave_pool.append(slave_id)

                # prepare for reduce phase
                self.reduce_words_list.append(d['file_name'])

                print("{} finished map for {}".format(slave_id, file_name))

            slaves_to_remove = list()
            # abandon inactive slaves
            for slave_id, info in self.tasks_in_progress.items():
                file_name, start_time = info

                if time.time() - start_time > SLAVE_ABANDON_THRESHOLD:
                    print("map removed {}".format(slave_id))
                    self.files_in_progress.remove(file_name)
                    self.file_pool.append(file_name)
                    slaves_to_remove.append(slave_id)

                    # abandon the slave
                    # self.slave_pool.append(slave_id)
            for slave_id in slaves_to_remove:
                del self.tasks_in_progress[slave_id]

            # process left files
            if len(self.slave_pool) != 0 and len(self.file_pool) != 0:
                slave_id = self.slave_pool[-1]
                self.slave_pool.pop(-1)

                file_name = self.file_pool[-1]
                self.file_pool.pop(-1)
                self.files_in_progress.add(file_name)

                q = self.slave_queues[slave_id]

                q.put({
                    'file_name': file_name,
                    'phase': 'map'
                })

                self.tasks_in_progress[slave_id] = [file_name, time.time()]

        self.reduce_words_pool = list()

        for word, lst in self.words_dict.items():
            self.reduce_words_pool.append([word] + lst)

        # reduce
        while len(self.words_dict) != len(self.reduce_finished_files):
            # add finished ones
            while not self.master_queue.empty():
                d = self.master_queue.get()
                print("[master][reduce] got from master queue: {}".format(d))
                if d["phase"] in ["map", "start"]:
                    self.slave_pool.append(d["slave_id"])
                    continue
                slave_id = d["slave_id"]

                if slave_id not in self.reduce_tasks_in_progress:
                    print("{} not found in reduce_tasks_in_progress".format(slave_id))
                    continue
                file_name = self.reduce_tasks_in_progress[slave_id]

                print("[master][reduce]{} finished reduce task for {}".format(slave_id, file_name))
                self.reduce_finished_files.append(file_name[0])
                self.reduce_words_in_progress.remove(d["original_slave_id"])
                del self.reduce_tasks_in_progress[slave_id]
                self.slave_pool.append(slave_id)

                # prepare for report phase
                self.reduced_file_list.append(d['file_name'])

            # abandon inactive slaves
            slaves_to_remove = list()
            for slave_id, info in self.reduce_tasks_in_progress.items():
                original_slave_id, start_time = info

                if time.time() - start_time > SLAVE_ABANDON_THRESHOLD:
                    print("reduce removed {}".format(slave_id))
                    self.reduce_words_in_progress.remove(original_slave_id)
                    self.reduce_words_pool.append([original_slave_id] + words_dict[original_slave_id])
                    slaves_to_remove.append(slave_id)
                    # abandon the slave
                    # self.slave_pool.append(slave_id)
            for slave_id in slaves_to_remove:
                del self.reduce_tasks_in_progress[slave_id]

            # process left files
            if len(self.slave_pool) != 0 and len(self.reduce_words_pool) != 0:
                slave_id = self.slave_pool[-1]
                self.slave_pool.pop(-1)

                info = self.reduce_words_pool[-1]
                self.reduce_words_pool.pop(-1)
                self.reduce_words_in_progress.add(info[0])

                q = self.slave_queues[slave_id]

                q.put({
                    'slave_id': info[0],
                    'req_words': info[1:],
                    'files': self.reduce_words_list,
                    'phase': 'reduce'
                })

                self.reduce_tasks_in_progress[slave_id] = [info[0], time.time()]

        # display info and stop all slaves

        for slave_id in self.slave_id_list:
            self.slave_queues[slave_id].put('stop')

        for file in self.reduced_file_list:
            print(file, ": ")
            with open(file) as infile:
                for line in infile:
                    print(line)

class ErrorType(Enum):
    LateStart = 0
    DontStart = 1
    StartbutCrash = 2
    # ....

class Slave:
    def __init__(self, my_id, control_arg: ErrorType, self_queue, master_queue) -> None:
        self.my_id = my_id
        self.control_arg = control_arg
        self.self_queue = self_queue
        self.master_queue = master_queue

    def map_util(self, file_name, new_file_name):
        d = dict()
        with open(file_name) as infile:
            for line in infile:
                for word in line.split():
                    word = word.lower()
                    word = ''.join(ch for ch in word if ch.isalpha())

                    if word not in d:
                        d[word] = 0
                    d[word] += 1

        with open(new_file_name, 'wb') as outfile:
            pickle.dump(d, outfile)

    def reduce_util(self, files, req_words, file_name):
        freqs = dict()

        for word in req_words:
            freqs[word] = 0
        for file in files:
            with open(file, 'rb') as infile:
                d = pickle.load(infile)

                for word in req_words:
                    if word in d:
                        freqs[word] += d[word]

        with open(file_name, 'w') as outfile:
            for word, freq in freqs.items():
                outfile.write("{} {}\n".format(word, freq))

    def run(self):
        print("{} started".format(self.my_id))
        if self.control_arg == ErrorType.LateStart:
            time.sleep(5)
        elif self.control_arg == ErrorType.DontStart:
            return

        self.master_queue.put({
            "phase": "start",
            "slave_id": self.my_id
        })

        while True:
            info = self.self_queue.get()

            if info == 'stop':
                print("{} exited".format(self.my_id))
                break

            phase = info['phase']

            if phase == 'reduce':

                files = info['files']
                req_words = info['req_words']
                slave_id = info['slave_id']
                file_name = os.path.join(OUT_PATH, "ans-{}.txt".format(slave_id[5:]))

                print("[reduce]{} started reduce for {}".format(self.my_id, req_words))

                self.reduce_util(files, req_words, file_name)

                print("[reduce]{} finished reduce for {}, words: {}".format(self.my_id, slave_id, req_words))
                self.master_queue.put({
                    'file_name': file_name,
                    'slave_id': self.my_id,
                    'phase': 'reduce',
                    'original_slave_id': slave_id
                })
            elif phase == 'map':
                file_name = info['file_name']
                new_file_name = OUT_PATH + file_name[:-4].split('/')[-1] + '_mapped.pkl'

                print("{} started map for {}".format(self.my_id, file_name))

                self.map_util(file_name, new_file_name)

                self.master_queue.put({
                    'slave_id': self.my_id,
                    'file_name': new_file_name,
                    'phase': 'map'
                })


if __name__ == '__main__':
    generate_pickle.generate_data()

    n_slaves = 10
    pending_file_list = [f'input/file_{i}.txt' for i in range(n_slaves)]
    slave_id_list = [f'Slave{i}' for i in range(3)]
    with open('input/req_words.pkl', 'rb') as f:
        words_dict = pickle.load(f)  # {'Slave1': ['a', 'and'], 'Slave2': ['Alice', 'Bob']}

    slave_queues = dict()
    print("starting manager")
    # Create multiprocessing queues
    with multiprocessing.Manager() as manager:
        for i in range(len(slave_id_list)):
            slave_queues[slave_id_list[i]] = manager.Queue()
        master_queue = manager.Queue()

        # Create a Master
        m = Master(pending_file_list, words_dict, slave_id_list, slave_queues, master_queue)
        # Create some slaves
        s_list = [Slave(slave_id_list[i],
                        ErrorType(random.randint(0, 2)),
                        slave_queues[slave_id_list[i]],
                        master_queue)
                  for i in range(len(slave_id_list))]

        print("starting processes")
        # run in multiprocess
        m_p = multiprocessing.Process(target=m.run)
        s_p_list = [multiprocessing.Process(target=s.run) for s in s_list]
        m_p.start()
        for p in s_p_list:
            p.start()
        m_p.join()
        for p in s_p_list:
            p.join()
