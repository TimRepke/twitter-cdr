from pathlib import Path

import numpy as np
import pickle


class VectorIndex:
    def __init__(self):
        self.dict_labels = {}
        self.vectors = np.array([])

    @property
    def id2idx(self) -> dict[str, int]:
        return {v: k for k, v in self.dict_labels.items()}

    @property
    def idx2id(self) -> dict[int, str]:
        return self.dict_labels

    def add_items(self, data: np.ndarray, ids: list[str] | None = None):
        if len(self.vectors) == 0:
            self.vectors = data
        else:
            self.vectors = np.vstack([self.vectors, data])

        num_added = len(data)
        start = len(self.dict_labels)
        int_labels = np.arange(num_added) + start
        if ids is not None:
            assert num_added == len(ids)
            self.dict_labels.update({i: key for i, key in zip(int_labels, ids)})
        else:
            self.dict_labels.update({i: i for i in int_labels})

    def load(self, path: Path):
        self.vectors = np.load(str(path) + '_vecs.npy')
        with open(str(path) + '_keys.pkl', 'rb') as f:
            self.dict_labels = pickle.load(f)

    def save(self, path: Path):
        Path(str(path) + '_vecs.bin').parent.mkdir(parents=True, exist_ok=True)
        np.save(str(path) + '_vecs.npy', self.vectors)
        with open(str(path) + '_keys.pkl', 'wb') as f:
            pickle.dump(self.dict_labels, f)

    def get_all_items(self):
        return list(self.dict_labels.values()), self.vectors
