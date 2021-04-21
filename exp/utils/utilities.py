import os 
import random
import numpy as np
from  utils.consts import FILE_SIZE_MB_10
from sklearn.datasets import make_blobs
from sklearn.datasets import make_classification

def get_size_on_disk(x, y, metric = 'MB'):
    # this function is for validation 
    tmp_file_name = "/tmp/tmp_test.csv"
    length = len(y)
    y.resize(length, 1)
    csv_array = np.concatenate((x,y), axis = 1)
    np.savetxt(tmp_file_name, csv_array, delimiter=",")
    size_byte = os.path.getsize(tmp_file_name)
    size_kb = size_byte / 1024.0
    size_mb = size_kb / 1024.0
    size_gb = size_mb / 1024.0
    if metric.lower() == "kb":
        return size_kb
    elif metric.lower() == "mb":
        return size_mb
    elif metric.lower() == "gb":
        return size_gb
    elif metric.lower() == "byte":
        return size_byte
    else:
        raise ValueError("Unknown Metric.")

def generate_vertical_split_dataset(x,y, output_dir = "output_data/volume", feature_count = 500):
    length = len(x)
    id_list= np.array(list(range(length)))
    id_list.resize(length, 1)
    y.resize(length,1)
    y_guest = y
    x_guest = x[:, :int(feature_count / 2) ]
    x_host  = x[:,  int(feature_count / 2):]
    csv_guest = np.concatenate( (id_list, y_guest, x_guest), axis=1)
    csv_host  = np.concatenate( (id_list, x_host), axis=1)
    np.random.shuffle(csv_host)
    feature_names = [ "X_%d" % i for i in range(500) ]
    header_guest = "id,y" 
    header_host = 'id'
    for i in range(int(feature_count / 2) ):
        header_guest = header_guest + ',' + feature_names[i]
        header_host  = header_host  + ',' + feature_names[i + int(feature_count / 2) ]
    np.savetxt( os.path.join(output_dir, 'guest.csv'), csv_guest, delimiter=",", header= header_guest, fmt ='%1.6g')
    np.savetxt( os.path.join(output_dir, 'host.csv' ), csv_host , delimiter=",", header= header_host, fmt ='%1.6g' )

def get_target_size_dataset(target_size = 10, metric = "MB"):
    if metric.lower() == "gb":
        target_size = target_size * 1024.0
    target_sample_size =  round(target_size / FILE_SIZE_MB_10 * 10.0) - 1 # 少一行留给表头
    x, y = make_classification(n_samples= target_sample_size, n_features= 500, n_repeated = 30, n_redundant= 30)
    return x, y 

def get_target_count_dataset(target_count = 2000):
    x_continues, y = make_classification(n_samples= target_count, n_features= 70, n_repeated = 20, n_redundant= 20)
    x_categorical = generate_categorical_feature(y) # 30个离散特征
    # guest 和 host 各 15 个离散特征
    # return x_continues, x_categorical
    x_guest = np.concatenate((x_categorical[:, :15], x_continues[:, :35]), axis= 1)
    x_host  = np.concatenate((x_categorical[:, 15:], x_continues[:, 35:]), axis= 1)
    x = np.concatenate((x_guest, x_host), axis= 1)
    return x, y
    

def generate_categorical_feature(y, feature_count = 30): # binary 
    length = len(y)
    n_class = len(set(y)) 
    data = list(range(7))
    if n_class > 2:
        raise NotImplementedError('multi-classification not implemented!')
    res = []
    for _ in range(feature_count):
        x_positive = data[:4]
        random.shuffle(data)
        x_negative = data[:4]
        each_feature = []
        for i in range(length):
            if y[i] == 1:
                each_feature.append(random.choice(x_positive))
            else:
                each_feature.append(random.choice(x_negative))
        res.append(each_feature)
    return np.array(res).T

