import pandas as pd
import numpy as np
import scipy.sparse
from typing import List
import time
from nltk.stem import PorterStemmer
from sklearn.feature_extraction.text import TfidfVectorizer


class StemmedTfidfVectorizer(TfidfVectorizer):
    """
    Sk-learn's "TfidfVectorizer" extension to provide the `stemming` feature
    """
    stemmer = PorterStemmer()
    def build_analyzer(self):
        analyzer = super(StemmedTfidfVectorizer, self).build_analyzer()
        return lambda doc: (StemmedTfidfVectorizer.stemmer.stem(w) for w in analyzer(doc))
    

def compute_sparse_repr(corpus: pd.DataFrame):
    """
    TF-IDF. Return vectors normalized, vocabulary of terms and idf weights
    """
    #Extract only the word and the numbers, made a lowercase transformation and usage of custom vocabulary to make representations independent
    doc_tfidf=StemmedTfidfVectorizer(lowercase=True, stop_words=None, token_pattern=r'\w+', norm="l2")

    #Computation of the sparse embedding
    sparse_doc=doc_tfidf.fit_transform(corpus)
    vocab=doc_tfidf.vocabulary_
    idf=doc_tfidf.idf_
    
    return sparse_doc, vocab, idf


def compute_cosine_similarity(sparse_repr: scipy.sparse.spmatrix | np.ndarray, ids: pd.DataFrame, threshold: float, debug:bool=False):
    """
    Compute the pairwise cosine similarity of elems in a vector.
    Return pairs in increasing order that are >=`threshold` and the num of pairs
    """
    #Compute the pairwise similarity (they are normalized so it's only the dot product)
    cosine_scores_matr=np.round(sparse_repr.dot(sparse_repr.transpose()).toarray(), 4)
    ids=ids.reset_index(drop=True)
    
    if debug:
        print(cosine_scores_matr)
    res_list=[]
    #Get all the unique pairs where (a,b)==(b,a) in increasing order. Also don't consider similarity with itself
    for i in range(cosine_scores_matr.shape[0]):
        for j in range(i+1, cosine_scores_matr.shape[0]):
            score=cosine_scores_matr[i, j]
            if score>=threshold:
                if ids[i]<ids[j]:
                    res_list.append((ids[i], ids[j], score))
                else:
                    res_list.append((ids[j], ids[i], score))
    
    return (res_list, len(res_list))


def eval_sol(sparse_repr: scipy.sparse.spmatrix | np.ndarray, ids: pd.DataFrame, thresholds: List[float], n_rep: int=3):
    """
    Compute the pairwise cosine similarity of elems in a vector for `n_rep` times (benchmark purposes)
    """
    time_list=[[] for i in range(len(thresholds))]
    pairs_list=[[] for i in range(len(thresholds))]

    #Get the time spent to compute cosine similarity for n_rep times for every threshold done
    for l_num, t in enumerate(thresholds):
        print(f"Threshold: {t}")
        num_of_pairs=0
        pairs=None
        for i in range(n_rep):
            start_time = time.time()
            pairs, num_of_pairs=compute_cosine_similarity(sparse_repr, ids, t)
            end_time = time.time()
            elapsed_time = end_time - start_time
            time_list[l_num].append(elapsed_time)
            print(f"N. of pairs: {num_of_pairs}")

        pairs_list[l_num]+=pairs
        print(f"Mean time elapsed: {np.mean(time_list[l_num])}\n")

    return time_list, pairs_list