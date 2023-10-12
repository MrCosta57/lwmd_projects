#!/usr/bin/env python
# coding: utf-8

# # Learning with Massive Data
# <p>
# Assignment 3 - Similarity search for document pairs<br>
# Giovanni Costa - 880892
# </p>
# 
# <p>
# <b>SPARK VERSION</b>
# </p>


import os
import numpy as np
import pandas as pd
import time
from typing import List
from pyspark.sql import SparkSession
from pyspark.ml.linalg import SparseVector
from scipy.sparse import save_npz, load_npz
import scipy.sparse


results="results/"
sample_size=1500
thresholds=[0.3, 0.5, 0.8]

# ## Spark version

sparse_repr1=load_npz(results+"sparse_repr_nfcorpus_sampled.npz")
df_id1=pd.read_parquet(results+"ids_nfcorpus_sampled.parquet")["_id"]
df_t1=pd.read_parquet(results+"doc_freq_nfcorpus_sampled.parquet")["df_t"]


# In[ ]:


sparse_repr2=load_npz(results+"sparse_repr_scifact_sampled.npz")
df_id2=pd.read_parquet(results+"ids_scifact_sampled.parquet")["_id"]
df_t2=pd.read_parquet(results+"doc_freq_scifact_sampled.parquet")["df_t"]


# In[ ]:


#Configuration at: https://spark.apache.org/docs/latest/configuration.html
spark = SparkSession.builder.appName("MyApp").getOrCreate()
sc=spark.sparkContext
spark


# In[ ]:


def csr_to_sparse_vector(row: scipy.sparse.spmatrix, doc_feq_sort_idx: np.ndarray | List):
    """
    Convert an array in CSR format into PySpark `SparseVector` type.
    NOTE: returned sparse representation is sorted in order of Document Frequency score, related to the TF-IDF document embedding
    """
    """ dtype_tuple=np.dtype([('integer', int), ('float', float)])
    tmp=np.empty(row.indices.shape[0], dtype=dtype_tuple)
    j=0
    for i in doc_feq_sort_idx:
        if row[0, i] !=0.0:
            tmp[j]=(i, row[0, i])
            j+=1 """
    row_tmp=row.toarray().reshape(-1)
    tmp=[(i, row_tmp[i]) for i in doc_feq_sort_idx if row_tmp[i]!=0.0]
    return SparseVector(row.shape[1], tmp)

def preprocessingForSpark(sparse_repr: scipy.sparse, df_id: pd.DataFrame, df_t: pd.DataFrame, threshold:float):
    """
    Given the sparse representation `sparse_repr`, the related doc-id `df_id` and the document frequencies `df_t`
    process the key-value pairs for doing all the PySpark computation, following MapReduce paradigm
    """
    #Get the sorted index of the terms in document frequency order
    doc_feq_sort_idx=np.argsort(df_t.values)[::-1]
    #Compute the PySpark sparse vector
    docs_sparse_forSpark = [csr_to_sparse_vector(sparse_repr.getrow(i), doc_feq_sort_idx) for i in range(sparse_repr.shape[0])]

    doc_ids = df_id.reset_index(drop=True)
    #Compute d* vector (useful in `b_d` function)
    
    #Make them in key-value pairs
    rdd_forMap=[(doc_ids[i], (docs_sparse_forSpark[i], threshold)) for i in range(sparse_repr.shape[0])]

    return rdd_forMap


# In[2]:


def b_d(sparse_repr, threshold):
    """
    Implementation of Prefix Filtering technique used in `my_map()` function
    """
    cum_sum=0
    res=0
    for i in range(sparse_repr.indices.shape[0]):
        j=int(i)
        res=j
        t=int(sparse_repr.indices[j])
        mult_val=sparse_repr[t]*d_star_sc.value[t]
        cum_sum+=mult_val
        if cum_sum>=threshold:
            res=j-1
            break
    return res

def my_map(elem):
    """
    Map the pairs according to "MapReduce" paradigm.
    Takes in input a key-value pair `<doc_id, doc_representation>`.
    Return a key-value pair `<term_id, <doc_id, doc_representation>>` using Prefix Filtering technique
    """
    doc_id=elem[0]
    sparse_repr=elem[1][0]
    threshold=elem[1][1]
    bound=b_d(sparse_repr, threshold)
    result=[(t, (doc_id, sparse_repr, threshold)) for t in sparse_repr.indices[bound+1:]]
    return result

def max_of_intersection(list1, list2):
    """
    Return the max value of the intersection of two sorted list.
    """
    max=0
    i = 0
    j = 0
    while i < len(list1) and j < len(list2):
        elem1=list1[i]
        elem2=list2[j]
        if elem1 == elem2:
            if elem1>max:
                max=elem1
            i += 1
            j += 1
        elif elem1 < elem2:
            i += 1
        else:
            j += 1
    return max

def my_reduce(elem):
    """
    Reduce the pairs according to "MapReduce" paradigm.
    Takes in input a key-value pair `<term_id, list(<doc_id, doc_representation>)>`.
    Return a key-value pair `<doc_id1, doc_id2, similarity(d1, d2)>`
    """
    result=[]
    pairs_dict={} #To directly prune the symmetric pairs
    key=elem[0]
    values=elem[1]
    for id1, d1, threshold in values:
        for id2, d2, _ in values:
            if id1!=id2 and (not pairs_dict.get((id2, id1), False)) and key==max_of_intersection(d1.indices, d2.indices):
                sim=round(d1.dot(d2), 4) #because vector are already normalized, so sim(d1,d2) it's simply the dot product
                if sim>=threshold:
                    pairs_dict[(id1, id2)]=True
                    result.append((id1, id2, sim))

    return result


# In[ ]:


d_star=sparse_repr1.max(axis=0).toarray().reshape(-1)
d_star_sc=sc.broadcast(d_star)
print("Computation of pairwise similarity for dataset1...")
for threshold in thresholds:
    print(f"Threshold: {threshold}")
    rdd_forMap=preprocessingForSpark(sparse_repr1, df_id1, df_t1, threshold)
    rdd_forMap=sc.parallelize(rdd_forMap)

    rdd_forReduce=rdd_forMap.flatMap(my_map)
    result_pairs=rdd_forReduce.groupByKey().flatMap(my_reduce)

    start_time = time.time()
    num_pairs=result_pairs.count()
    end_time = time.time()
    elapsed_time = end_time - start_time

    print(f"N. of pairs: {num_pairs}")
    print(f"Time spent: {elapsed_time}")
    #result_pairs.collect()


# In[ ]:


d_star=sparse_repr2.max(axis=0).toarray().reshape(-1)
d_star_sc=sc.broadcast(d_star)
print("Computation of pairwise similarity for dataset2...")
for threshold in thresholds:
    print(f"Threshold: {threshold}")
    rdd_forMap=preprocessingForSpark(sparse_repr2, df_id2, df_t2, threshold)
    rdd_forMap=sc.parallelize(rdd_forMap)

    rdd_forReduce=rdd_forMap.flatMap(my_map)
    result_pairs=rdd_forReduce.groupByKey().flatMap(my_reduce)

    start_time = time.time()
    num_pairs=result_pairs.count()
    end_time = time.time()
    elapsed_time = end_time - start_time

    print(f"N. of pairs: {num_pairs}")
    print(f"Time spent: {elapsed_time}")
    #result_pairs.collect()


# In[ ]:


spark.stop()