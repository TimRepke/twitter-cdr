from pathlib import Path
import os
from transformers import (AutoModel,
                          AutoModelForSequenceClassification,
                          AutoTokenizer, TextClassificationPipeline, AutoConfig)

from scipy.special import softmax
from typing import Literal, Optional, Union
from dataclasses import dataclass
import numpy as np
from abc import ABC, abstractmethod
from sentence_transformers import SentenceTransformer


def prepare_tweet(text: str):
    return ' '.join([
        '@user' if token.startswith('@') and len(token) > 1 else \
            'http' if token.startswith('http') else \
                'http' if token.startswith('www') else \
                    token
        for token in text.split(' ')
    ])


class Classifier:
    def __init__(self, hf_name: str, cache_dir: Path):
        self.hf_name = hf_name
        self._classifier: TextClassificationPipeline | None = None
        self._config: AutoConfig | None = None
        self._cache = cache_dir

    # def store(self, target_dir: Path):
    #     target_dir.mkdir(parents=True, exist_ok=True)
    #     target = str(target_dir)
    #
    #     tokenizer = AutoTokenizer.from_pretrained(self.hf_name)
    #     config = AutoConfig.from_pretrained(self.hf_name)
    #     model = AutoModelForSequenceClassification.from_pretrained(self.hf_name)
    #
    #     tokenizer.save_pretrained(target)
    #     config.
    #     model.save_pretrained(target)
    #
    #
    #     pretrained_model = AutoModelForSequenceClassification.from_pretrained(self.hf_name)
    #     pretrained_model.save_pretrained(target)
    #
    #     tokenizer = AutoTokenizer.from_pretrained(self.hf_name)
    #     tokenizer.save_pretrained(target)

    def load(self):
        if self._classifier is None:
            self._cache.mkdir(parents=True, exist_ok=True)
            target = str(self._cache)

            tokenizer = AutoTokenizer.from_pretrained(self.hf_name, cache_dir=target)
            model = AutoModelForSequenceClassification.from_pretrained(self.hf_name, cache_dir=target)
            self._config = AutoConfig.from_pretrained(self.hf_name, cache_dir=target)
            self._classifier = TextClassificationPipeline(model=model, tokenizer=tokenizer)

    @staticmethod
    def preprocess(texts: list[str]):
        return [prepare_tweet(text) for text in texts]

    def classify(self, texts: list[str], return_all_scores: bool = False):
        scores = self._classifier(texts, top_k=None if return_all_scores else 1)
        if return_all_scores:
            return [{score['label']: score['score'] for score in scores_i} for scores_i in scores]
        return [{score['label']: score['score']} for score in scores]


class Embedder:
    def __init__(self, hf_name: str, cache_dir: Path):
        self.hf_name = hf_name
        self._model: SentenceTransformer | None = None
        self._cache = cache_dir

    def load(self):
        if self._model is None:
            self._cache.mkdir(parents=True, exist_ok=True)
            target = str(self._cache)

            self._model = SentenceTransformer(self.hf_name, cache_folder=target)

    @staticmethod
    def preprocess(texts: list[str]):
        return [prepare_tweet(text) for text in texts]

    def embed(self, texts: list[str]) -> np.ndarray:
        return self._model.encode(texts, convert_to_numpy=True)
