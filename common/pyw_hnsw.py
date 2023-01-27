# https://github.com/nmslib/hnswlib/blob/master/examples/pyw_hnswlib.py
from pathlib import Path

import hnswlib
import numpy as np
import threading
import pickle
import tqdm


class Index:
    def __init__(self, space: str, dim: int):
        self.index = hnswlib.Index(space, dim)
        self.lock = threading.Lock()
        self.dict_labels = {}
        self.cur_ind = 0

    def init_index(self, max_elements: int, ef_construction: int = 200, M: int = 16, random_seed: int = 100):
        self.index.init_index(max_elements=max_elements, ef_construction=ef_construction, M=M, random_seed=random_seed)

    def add_items(self, data, ids: list[str] | None = None):
        if ids is not None:
            assert len(data) == len(ids)
        num_added = len(data)
        with self.lock:
            start = self.cur_ind
            self.cur_ind += num_added
        int_labels = []

        if ids is not None:
            for dl in ids:
                int_labels.append(start)
                self.dict_labels[start] = dl
                start += 1
        else:
            for _ in range(len(data)):
                int_labels.append(start)
                self.dict_labels[start] = start
                start += 1
        self.index.add_items(data=data, ids=np.asarray(int_labels))

    def set_ef(self, ef: int):
        self.index.set_ef(ef)

    def load_index(self, path: Path):
        self.index.load_index(str(path) + '_index.bin')
        with open(str(path) + '_keys.pkl', 'rb') as f:
            self.cur_ind, self.dict_labels = pickle.load(f)

    def save_index(self, path: Path):
        Path(str(path) + '_index.bin').parent.mkdir(parents=True, exist_ok=True)
        self.index.save_index(str(path) + '_index.bin')
        with open(str(path) + '_keys.pkl', 'wb') as f:
            pickle.dump((self.cur_ind, self.dict_labels), f)

    def set_num_threads(self, num_threads: int):
        self.index.set_num_threads(num_threads)

    def knn_query(self, data, k: int = 1):
        labels_int, distances = self.index.knn_query(data=data, k=k)
        labels = []
        for li in labels_int:
            labels.append(
                [self.dict_labels[l] for l in li]
            )
        return labels, distances

    def get_all_items(self):
        labels = list(self.dict_labels.values())
        vectors = np.array([self.index.get_items([i])[0] for i in self.dict_labels.keys()])
        return labels, vectors


class DuplicateFreeIndex:
    def __init__(self, space: str, dim: int):
        self.index = hnswlib.Index(space, dim)
        self.dict_labels = {}

    def init_index(self, max_elements: int, ef_construction: int = 200, M: int = 16, random_seed: int = 100):
        self.index.init_index(max_elements=max_elements, ef_construction=ef_construction, M=M, random_seed=random_seed)

    def add_items(self, data, ids: list[str]):
        for iid, datum in tqdm.tqdm(zip(ids, data)):
            if self.index.get_current_count() > 0:
                # try for duplicates first
                nearest, distances = self.index.knn_query(np.array([datum]), k=1)

                # looks like this vector is already in the index
                if distances[0] == 0:
                    self.dict_labels[nearest[0]].append(iid)
                    continue

            # not in index yet or empty index
            index_id = self.index.get_current_count()
            self.index.add_items(np.array([datum]), np.array([index_id]))
            self.dict_labels[index_id] = [iid]

    def set_ef(self, ef: int):
        self.index.set_ef(ef)

    def load_index(self, path: Path):
        self.index.load_index(str(path) + '_index.bin')
        with open(str(path) + '_keys.pkl', 'rb') as f:
            self.dict_labels = pickle.load(f)

    def save_index(self, path: Path):
        Path(str(path) + '_index.bin').parent.mkdir(parents=True, exist_ok=True)
        self.index.save_index(str(path) + '_index.bin')
        with open(str(path) + '_keys.pkl', 'wb') as f:
            pickle.dump(self.dict_labels, f)

    def knn_query(self, data, k: int = 1, include_duplicates: bool = False):
        labels_int, distances = self.index.knn_query(data=data, k=k)
        if include_duplicates:
            raise NotImplementedError
        else:
            labels = [[self.dict_labels[l][0] for l in li] for li in labels_int]
        return labels, distances

    def get_all_items(self):
        labels = list(self.dict_labels.values())
        vectors = np.array([self.index.get_items([i])[0] for i in self.dict_labels.keys()])
        return labels, vectors
